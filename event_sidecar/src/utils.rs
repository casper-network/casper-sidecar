#[cfg(feature = "additional-metrics")]
use metrics::db::EVENTS_PROCESSED_PER_SECOND;
#[cfg(feature = "additional-metrics")]
use std::sync::Arc;
#[cfg(feature = "additional-metrics")]
use std::time::Duration;
#[cfg(feature = "additional-metrics")]
use std::time::Instant;
use std::{
    fmt::{self, Debug, Display, Formatter},
    io,
    net::{SocketAddr, ToSocketAddrs},
};
use thiserror::Error;
#[cfg(feature = "additional-metrics")]
use tokio::sync::{
    mpsc::{channel, Sender},
    Mutex,
};
use warp::{reject, Filter};

#[derive(Debug)]
pub struct Unexpected(pub(super) anyhow::Error);
impl reject::Reject for Unexpected {}

#[derive(Debug)]
pub struct InvalidPath;
impl reject::Reject for InvalidPath {}

/// DNS resolution error.
#[derive(Debug, Error)]
#[error("could not resolve `{address}`: {kind}")]
pub struct ResolveAddressError {
    /// Address that failed to resolve.
    address: String,
    /// Reason for resolution failure.
    kind: ResolveAddressErrorKind,
}

/// DNS resolution error kind.
#[derive(Debug)]
enum ResolveAddressErrorKind {
    /// Resolve returned an error.
    ErrorResolving(io::Error),
    /// Resolution did not yield any address.
    NoAddressFound,
}

impl Display for ResolveAddressErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ResolveAddressErrorKind::ErrorResolving(err) => {
                write!(f, "could not run dns resolution: {err}")
            }
            ResolveAddressErrorKind::NoAddressFound => {
                write!(f, "no addresses found")
            }
        }
    }
}

/// Parses a network address from a string, with DNS resolution.
pub(crate) fn resolve_address(address: &str) -> Result<SocketAddr, ResolveAddressError> {
    address
        .to_socket_addrs()
        .map_err(|err| ResolveAddressError {
            address: address.to_string(),
            kind: ResolveAddressErrorKind::ErrorResolving(err),
        })?
        .next()
        .ok_or_else(|| ResolveAddressError {
            address: address.to_string(),
            kind: ResolveAddressErrorKind::NoAddressFound,
        })
}

/// An error starting one of the HTTP servers.
#[derive(Debug, Error)]
pub(crate) enum ListeningError {
    /// Failed to resolve address.
    #[error("failed to resolve network address: {0}")]
    ResolveAddress(ResolveAddressError),

    /// Failed to listen.
    #[error("failed to listen on {address}: {error}")]
    Listen {
        /// The address attempted to listen on.
        address: SocketAddr,
        /// The failure reason.
        error: Box<dyn std::error::Error + Send + Sync>,
    },
}

/// Handle the case where no filter URL was specified after the root address (HOST:PORT).
/// Return: a message that an invalid path was provided.
/// Example: curl http://127.0.0.1:18888
/// {"code":400,"message":"Invalid request path provided"}
pub fn root_filter() -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone
{
    warp::path::end()
        .and_then(|| async { Err::<String, warp::Rejection>(warp::reject::custom(InvalidPath)) })
}

#[cfg(feature = "additional-metrics")]
struct MetricsData {
    last_measurement: Instant,
    observed_events: u64,
}

#[cfg(feature = "additional-metrics")]
pub fn start_metrics_thread(module_name: String) -> Sender<()> {
    let (metrics_queue_tx, mut metrics_queue_rx) = channel(10000);
    let metrics_data = Arc::new(Mutex::new(MetricsData {
        last_measurement: Instant::now(),
        observed_events: 0,
    }));
    let metrics_data_for_thread = metrics_data.clone();
    tokio::spawn(async move {
        let metrics_data = metrics_data_for_thread;
        let sleep_for = Duration::from_secs(30);
        loop {
            tokio::time::sleep(sleep_for).await;
            let mut guard = metrics_data.lock().await;
            let duration = guard.last_measurement.elapsed();
            let number_of_observed_events = guard.observed_events;
            guard.observed_events = 0;
            guard.last_measurement = Instant::now();
            drop(guard);
            let seconds = duration.as_secs_f64();
            let events_per_second = (number_of_observed_events as f64) / seconds;
            EVENTS_PROCESSED_PER_SECOND
                .with_label_values(&[module_name.as_str()])
                .set(events_per_second);
        }
    });
    let metrics_data_for_thread = metrics_data.clone();
    tokio::spawn(async move {
        let metrics_data = metrics_data_for_thread;
        while metrics_queue_rx.recv().await.is_some() {
            let mut guard = metrics_data.lock().await;
            guard.observed_events += 1;
            drop(guard);
        }
    });
    metrics_queue_tx
}

#[cfg(test)]
pub mod tests {
    use crate::{
        database::{postgresql_database::PostgreSqlDatabase, sqlite_database::SqliteDatabase},
        run, run_rest_server,
        testing::{
            mock_node::tests::MockNode,
            testing_config::{get_port, prepare_config, TestingConfig},
        },
        Database,
    };
    use anyhow::Error as AnyhowError;
    use pg_embed::{
        pg_enums::PgAuthMethod,
        pg_fetch::{PgFetchSettings, PG_V13},
        postgres::{PgEmbed, PgSettings},
    };
    use std::{path::PathBuf, process::ExitCode, time::Duration};
    use tempfile::{tempdir, TempDir};
    use tokio::{sync::mpsc::Receiver, time::timeout};

    pub(crate) fn display_duration(duration: Duration) -> String {
        // less than a second
        if duration.as_millis() < 1000 {
            format!("{}ms", duration.as_millis())
            // more than a minute
        } else if duration.as_secs() > 60 {
            let (minutes, seconds) = (duration.as_secs() / 60, duration.as_secs() % 60);
            format!("{}min. {}s", minutes, seconds)
            // over a second / under a minute
        } else {
            format!("{}s", duration.as_secs())
        }
    }
    /// Forces a stop on the given nodes and waits until all starts finish. Will timeout if the nodes can't start in 3 minutes.
    pub(crate) async fn start_nodes_and_wait(nodes: Vec<&mut MockNode>) -> Vec<()> {
        let mut futures = vec![];
        for node in nodes {
            futures.push(node.start());
        }
        timeout(Duration::from_secs(180), futures::future::join_all(futures))
            .await
            .unwrap()
    }
    /// wait_for_n_messages waits at most the `timeout_after` for observing `n` messages received on the `receiver`
    /// If the receiver returns a None the function will finish early
    pub(crate) async fn wait_for_n_messages<T: Send + Sync + 'static>(
        n: usize,
        mut receiver: Receiver<T>,
        timeout_after: Duration,
    ) -> Receiver<T> {
        let join_handle = tokio::spawn(async move {
            for _ in 0..n {
                if receiver.recv().await.is_none() {
                    break;
                }
            }
            receiver
        });
        match timeout(timeout_after, join_handle).await {
            Ok(res) => {
                res.map_err(|err| AnyhowError::msg(format!("Failed to wait for receiver, {err}")))
            }
            Err(_) => Err(AnyhowError::msg("Waiting for messages timed out")),
        }
        .unwrap()
    }
    /// Forces a stop on the given nodes and waits until the stop happens. Will timeout if the nodes can't stop in 3 minutes.
    pub(crate) async fn stop_nodes_and_wait(nodes: Vec<&mut MockNode>) -> Vec<()> {
        let mut futures = vec![];
        for node in nodes {
            futures.push(node.stop());
        }
        timeout(Duration::from_secs(180), futures::future::join_all(futures))
            .await
            .unwrap()
    }
    pub(crate) fn any_string_contains(data: &[String], infix: String) -> bool {
        let infix_str = infix.as_str();
        data.iter().any(|x| x.contains(infix_str))
    }

    #[allow(dead_code)]
    pub struct MockNodeTestProperties {
        pub testing_config: TestingConfig,
        pub temp_storage_dir: TempDir,
        pub node_port_for_sse_connection: u16,
        pub node_port_for_rest_connection: u16,
        pub event_stream_server_port: u16,
    }
    pub async fn prepare_one_node_and_start(node_mock: &mut MockNode) -> MockNodeTestProperties {
        let (
            testing_config,
            temp_storage_dir,
            node_port_for_sse_connection,
            node_port_for_rest_connection,
            event_stream_server_port,
        ) = build_test_config();
        node_mock.set_sse_port(node_port_for_sse_connection);
        node_mock.set_rest_port(node_port_for_rest_connection);
        start_nodes_and_wait(vec![node_mock]).await;
        start_sidecar(testing_config.clone()).await;
        MockNodeTestProperties {
            testing_config,
            temp_storage_dir,
            node_port_for_sse_connection,
            node_port_for_rest_connection,
            event_stream_server_port,
        }
    }

    pub async fn start_sidecar_with_rest_api(
        config: TestingConfig,
    ) -> tokio::task::JoinHandle<Result<ExitCode, AnyhowError>> {
        tokio::spawn(async move { unpack_test_config_and_run(config, true).await })
        // starting the sidecar
    }

    pub async fn start_sidecar(
        config: TestingConfig,
    ) -> tokio::task::JoinHandle<Result<ExitCode, AnyhowError>> {
        tokio::spawn(async move { unpack_test_config_and_run(config, false).await })
        // starting the sidecar
    }

    pub fn build_test_config() -> (TestingConfig, TempDir, u16, u16, u16) {
        build_test_config_with_retries(10, 1, true, None)
    }

    pub fn build_test_config_without_db_storage() -> (TestingConfig, TempDir, u16, u16, u16) {
        build_test_config_with_retries(10, 1, false, None)
    }

    pub fn build_test_config_with_network_name(
        network_name: &str,
    ) -> (TestingConfig, TempDir, u16, u16, u16) {
        build_test_config_with_retries(10, 1, true, Some(network_name.into()))
    }

    pub fn build_test_config_without_connections(
        enable_db_storage: bool,
        maybe_network_name: Option<String>,
    ) -> (TestingConfig, TempDir, u16) {
        let temp_storage_dir =
            tempdir().expect("Should have created a temporary storage directory");
        let testing_config =
            prepare_config(&temp_storage_dir, enable_db_storage, maybe_network_name);
        let event_stream_server_port = testing_config.event_stream_server_port();
        (testing_config, temp_storage_dir, event_stream_server_port)
    }
    pub fn build_test_config_with_retries(
        max_attempts: usize,
        delay_between_retries: usize,
        enable_db_storage: bool,
        maybe_network_name: Option<String>,
    ) -> (TestingConfig, TempDir, u16, u16, u16) {
        let (mut testing_config, temp_storage_dir, event_stream_server_port) =
            build_test_config_without_connections(enable_db_storage, maybe_network_name);
        testing_config.add_connection(None, None, None);
        let node_port_for_sse_connection = testing_config
            .event_server_config
            .connections
            .first()
            .unwrap()
            .sse_port;
        let node_port_for_rest_connection = testing_config
            .event_server_config
            .connections
            .first()
            .unwrap()
            .rest_port;
        testing_config.set_retries_for_node(
            node_port_for_sse_connection,
            max_attempts,
            delay_between_retries,
        );
        testing_config.set_allow_partial_connection_for_node(node_port_for_sse_connection, true);
        (
            testing_config,
            temp_storage_dir,
            node_port_for_sse_connection,
            node_port_for_rest_connection,
            event_stream_server_port,
        )
    }
    pub struct PostgresTestContext {
        pub pg: PgEmbed,
        pub _temp_dir: TempDir,
        pub db: PostgreSqlDatabase,
        pub port: u16,
    }
    impl Drop for PostgresTestContext {
        fn drop(&mut self) {
            let _ = self.pg.stop_db_sync();
        }
    }
    async fn spin_up_postgres(
        pg_settings: PgSettings,
        temp_dir: TempDir,
    ) -> Result<(PgEmbed, TempDir), AnyhowError> {
        let fetch_settings = PgFetchSettings {
            version: PG_V13,
            ..Default::default()
        };
        let pg_res = PgEmbed::new(pg_settings, fetch_settings).await;
        if let Err(e) = pg_res {
            drop(temp_dir);
            return Err(anyhow::Error::from(e));
        }
        let mut pg = pg_res.unwrap();
        let res = pg.setup().await;
        if let Err(e) = res {
            let _ = pg.stop_db().await;
            drop(temp_dir);
            return Err(anyhow::Error::from(e));
        }
        let res = pg.start_db().await;
        if let Err(e) = res {
            let _ = pg.stop_db().await;
            drop(temp_dir);
            return Err(anyhow::Error::from(e));
        }
        Ok((pg, temp_dir))
    }
    async fn start_embedded_postgres() -> (PgEmbed, TempDir, u16) {
        let port = get_port();
        let temp_storage_path =
            tempdir().expect("Should have created a temporary storage directory");
        let path = temp_storage_path.path().to_string_lossy().to_string();
        let pg_settings = PgSettings {
            database_dir: PathBuf::from(path),
            port,
            user: "postgres".to_string(),
            password: "p@$$w0rd".to_string(),
            auth_method: PgAuthMethod::Plain,
            persistent: false,
            timeout: Some(Duration::from_secs(120)),
            migration_dir: None,
        };
        let (db, folder) = spin_up_postgres(pg_settings, temp_storage_path)
            .await
            .unwrap();
        (db, folder, port)
    }
    pub async fn build_postgres_database() -> Result<PostgresTestContext, AnyhowError> {
        let (pg, temp_dir, port) = start_embedded_postgres().await;
        let database_name = "event_sidecar";
        pg.create_database(database_name).await?;
        let pg_db_uri: String = pg.full_db_uri(database_name);
        let db = PostgreSqlDatabase::new_from_postgres_uri(pg_db_uri)
            .await
            .unwrap();
        Ok(PostgresTestContext {
            pg,
            _temp_dir: temp_dir,
            db,
            port,
        })
    }
    pub async fn build_postgres_based_test_config(
        max_attempts: usize,
        delay_between_retries: usize,
    ) -> (TestingConfig, TempDir, u16, u16, u16, PostgresTestContext) {
        use crate::types::config::StorageConfig;
        let context = build_postgres_database().await.unwrap();
        let temp_storage_dir =
            tempdir().expect("Should have created a temporary storage directory");
        let mut testing_config = prepare_config(&temp_storage_dir, true, None);
        let event_stream_server_port = testing_config.event_stream_server_port();
        testing_config.set_storage(StorageConfig::postgres_with_port(context.port));
        testing_config.add_connection(None, None, None);
        let node_port_for_sse_connection = testing_config
            .event_server_config
            .connections
            .first()
            .unwrap()
            .sse_port;
        let node_port_for_rest_connection = testing_config
            .event_server_config
            .connections
            .first()
            .unwrap()
            .rest_port;
        testing_config.set_retries_for_node(
            node_port_for_sse_connection,
            max_attempts,
            delay_between_retries,
        );
        testing_config.set_allow_partial_connection_for_node(node_port_for_sse_connection, true);
        (
            testing_config,
            temp_storage_dir,
            node_port_for_sse_connection,
            node_port_for_rest_connection,
            event_stream_server_port,
            context,
        )
    }

    pub async fn unpack_test_config_and_run(
        testing_config: TestingConfig,
        spin_up_rest_api: bool,
    ) -> Result<ExitCode, AnyhowError> {
        let has_db_configured = testing_config.has_db_configured();
        let sse_config = testing_config.inner();
        let storage_config = testing_config.storage_config;
        let storage_folder = storage_config.storage_folder.clone();
        let maybe_database = if has_db_configured {
            let sqlite_database = SqliteDatabase::new_from_config(&storage_config)
                .await
                .unwrap();
            Some(Database::SqliteDatabaseWrapper(sqlite_database))
        } else {
            None
        };

        if spin_up_rest_api {
            if !has_db_configured {
                return Err(AnyhowError::msg(
                    "Can't unpack TestingConfig with REST API if no database is configured",
                ));
            }
            let rest_api_server_config = testing_config.rest_api_server_config;
            let database_for_rest_api = maybe_database.clone().unwrap();
            tokio::spawn(async move {
                run_rest_server(rest_api_server_config, database_for_rest_api).await
            });
        }
        run(
            sse_config,
            storage_folder,
            maybe_database,
            testing_config.network_name,
        )
        .await
    }
}
