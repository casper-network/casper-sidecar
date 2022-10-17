extern crate core;

mod event_stream_server;
mod rest_server;
mod sql;
mod sqlite_database;
#[cfg(test)]
mod testing;
mod types;
mod utils;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use casper_event_listener::EventListener;
use casper_event_types::SseData;

use anyhow::{Context, Error};
use clap::Parser;
use futures::future::join_all;
use hex_fmt::HexFmt;
use tempfile::TempDir;
use tracing::{debug, info, trace, warn};

use crate::{
    event_stream_server::{Config as SseConfig, EventStreamServer},
    rest_server::run_server as start_rest_server,
    sqlite_database::SqliteDatabase,
    types::{
        config::Config,
        database::{DatabaseWriteError, DatabaseWriter},
        sse_events::*,
    },
};

pub fn read_config(config_path: &str) -> Result<Config, Error> {
    let toml_content =
        std::fs::read_to_string(config_path).context("Error reading config file contents")?;
    toml::from_str(&toml_content).context("Error parsing config into TOML format")
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct CmdLineArgs {
    /// Path to the TOML-formatted config file
    #[arg(short, long, value_name = "FILE")]
    path_to_config: String,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    // Install global collector for tracing
    tracing_subscriber::fmt::init();

    let args = CmdLineArgs::parse();

    let path_to_config = args.path_to_config;

    let config: Config = read_config(&path_to_config).context("Error constructing config")?;
    info!("Configuration loaded");

    run(config).await
}

// todo mediator

async fn run(config: Config) -> Result<(), Error> {
    let path_to_database_dir = Path::new(&config.storage.storage_path);

    // Creates and initialises Sqlite database
    let sqlite_database =
        SqliteDatabase::new(path_to_database_dir, config.storage.sqlite_config.clone())
            .await
            .context("Error instantiating database")?;

    // Prepare the REST server task - this will be executed later
    let rest_server_handle = tokio::spawn(start_rest_server(
        config.rest_server.port,
        sqlite_database.clone(),
    ));

    let mut node_event_listeners = Vec::with_capacity(config.connection.node_connections.len());

    for connection in &config.connection.node_connections {
        let bind_address = format!("{}:{}", connection.ip_address, connection.sse_port);

        let event_listener = EventListener::new(
            bind_address,
            connection.max_retries,
            connection.delay_between_retries_secs,
        )
        .await?;

        node_event_listeners.push(event_listener);
    }

    let api_versions_all_match = node_event_listeners
        .windows(2)
        .all(|w| w[0].api_version == w[1].api_version);

    if !api_versions_all_match {
        return Err(Error::msg(
            "Error connecting to nodes - all connections must have the same API version",
        ));
    }

    let api_version = node_event_listeners[0].api_version;

    let event_stream_server = EventStreamServer::new(
        SseConfig::new(
            config.event_stream_server.port,
            Some(config.event_stream_server.event_stream_buffer_length),
            Some(config.event_stream_server.max_concurrent_subscribers),
        ),
        PathBuf::from(&config.storage.storage_path),
        api_version,
    )
    .context("Failed initialise event stream server")?;

    // The ESS instance is wrapped in an arc
    let ess_with_arc = Arc::new(event_stream_server);

    // Task to manage incoming events from all three filters
    let listening_task_handle = async move {
        let mut join_handles = Vec::with_capacity(node_event_listeners.len());

        let connection_configs = config.connection.node_connections.clone();

        for (event_listener, connection_config) in
            node_event_listeners.into_iter().zip(connection_configs)
        {
            let cloned_ess = ess_with_arc.clone();

            let join_handle = tokio::spawn(sse_processor(
                event_listener,
                cloned_ess,
                sqlite_database.clone(),
                connection_config.enable_event_logging,
            ));

            join_handles.push(join_handle);
        }

        let _ = join_all(join_handles).await;

        Err::<(), Error>(Error::msg("Connected node(s) are unavailable"))
    };

    tokio::select! {
        result = rest_server_handle => { warn!(?result, "REST server shutting down") }
        result = listening_task_handle => { warn!(?result, "SSE processing ended") }
    }

    Ok(())
}

async fn sse_processor(
    sse_event_listener: EventListener,
    event_stream_server: Arc<EventStreamServer>,
    sqlite_database: SqliteDatabase,
    enable_event_logging: bool,
) {
    let mut sse_data_stream = sse_event_listener.consume_combine_streams().await;

    while let Some(sse_event) = sse_data_stream.recv().await {
        match sse_event.data {
            SseData::ApiVersion(version) => {
                if enable_event_logging {
                    info!(%version, "API Version");
                }
            }
            SseData::BlockAdded { block, block_hash } => {
                let full_length_hash = format!("{}", HexFmt(block_hash.inner()));
                if enable_event_logging {
                    info!("Block Added: {:18}", HexFmt(block_hash.inner()));
                    debug!("Block Added: {}", full_length_hash);
                }

                let res = sqlite_database
                    .save_block_added(
                        BlockAdded::new(block_hash, block.clone()),
                        sse_event.id.unwrap(),
                        sse_event.source,
                    )
                    .await;

                match res {
                    Ok(_) => {
                        event_stream_server.broadcast(SseData::BlockAdded { block, block_hash })
                    }
                    Err(DatabaseWriteError::UniqueConstraint(uc_err)) => {
                        debug!(
                            "Already received BlockAdded ({}), logged in event_log",
                            full_length_hash
                        );
                        trace!(?uc_err);
                    }
                    Err(other_err) => warn!(?other_err, "Unexpected error saving BlockAdded"),
                }
            }
            SseData::DeployAccepted { deploy } => {
                let full_length_hash = format!("{}", HexFmt(deploy.id().inner()));
                if enable_event_logging {
                    info!("Deploy Accepted: {:18}", HexFmt(deploy.id().inner()));
                    debug!("Deploy Accepted: {}", full_length_hash);
                }
                let deploy_accepted = DeployAccepted::new(deploy.clone());
                let res = sqlite_database
                    .save_deploy_accepted(deploy_accepted, sse_event.id.unwrap(), sse_event.source)
                    .await;

                match res {
                    Ok(_) => event_stream_server.broadcast(SseData::DeployAccepted { deploy }),
                    Err(DatabaseWriteError::UniqueConstraint(uc_err)) => {
                        debug!(
                            "Already received DeployAccepted ({}), logged in event_log",
                            full_length_hash
                        );
                        trace!(?uc_err);
                    }
                    Err(other_err) => warn!(?other_err, "Unexpected error saving DeployAccepted"),
                }
            }
            SseData::DeployExpired { deploy_hash } => {
                let full_length_hash = format!("{}", HexFmt(deploy_hash.inner()));
                if enable_event_logging {
                    info!("Deploy Expired: {:18}", HexFmt(deploy_hash.inner()));
                    debug!("Deploy Expired: {}", full_length_hash);
                }
                let res = sqlite_database
                    .save_deploy_expired(
                        DeployExpired::new(deploy_hash),
                        sse_event.id.unwrap(),
                        sse_event.source,
                    )
                    .await;

                match res {
                    Ok(_) => event_stream_server.broadcast(SseData::DeployExpired { deploy_hash }),
                    Err(DatabaseWriteError::UniqueConstraint(uc_err)) => {
                        debug!(
                            "Already received DeployExpired ({}), logged in event_log",
                            full_length_hash
                        );
                        trace!(?uc_err);
                    }
                    Err(other_err) => warn!(?other_err, "Unexpected error saving DeployExpired"),
                }
            }
            SseData::DeployProcessed {
                deploy_hash,
                account,
                timestamp,
                ttl,
                dependencies,
                block_hash,
                execution_result,
            } => {
                let full_length_hash = format!("{}", HexFmt(deploy_hash.inner()));
                if enable_event_logging {
                    info!("Deploy Processed: {:18}", HexFmt(deploy_hash.inner()));
                    debug!("Deploy Processed: {}", full_length_hash);
                }
                let deploy_processed = DeployProcessed::new(
                    deploy_hash.clone(),
                    account.clone(),
                    timestamp,
                    ttl,
                    dependencies.clone(),
                    block_hash.clone(),
                    execution_result.clone(),
                );
                let res = sqlite_database
                    .save_deploy_processed(
                        deploy_processed.clone(),
                        sse_event.id.unwrap(),
                        sse_event.source,
                    )
                    .await;

                match res {
                    Ok(_) => event_stream_server.broadcast(SseData::DeployProcessed {
                        deploy_hash,
                        account,
                        timestamp,
                        ttl,
                        dependencies,
                        block_hash,
                        execution_result,
                    }),
                    Err(DatabaseWriteError::UniqueConstraint(uc_err)) => {
                        debug!(
                            "Already received DeployProcessed ({}), logged in event_log",
                            full_length_hash
                        );
                        trace!(?uc_err);
                    }
                    Err(other_err) => warn!(?other_err, "Unexpected error saving DeployProcessed"),
                }
            }
            SseData::Fault {
                era_id,
                timestamp,
                public_key,
            } => {
                let fault = Fault::new(era_id, public_key.clone(), timestamp);
                warn!(%fault, "Fault reported");
                let res = sqlite_database
                    .save_fault(fault.clone(), sse_event.id.unwrap(), sse_event.source)
                    .await;

                match res {
                    Ok(_) => event_stream_server.broadcast(SseData::Fault {
                        era_id,
                        timestamp,
                        public_key,
                    }),
                    Err(DatabaseWriteError::UniqueConstraint(uc_err)) => {
                        debug!("Already received Fault ({:#?}), logged in event_log", fault);
                        trace!(?uc_err);
                    }
                    Err(other_err) => warn!(?other_err, "Unexpected error saving Fault"),
                }
            }
            SseData::FinalitySignature(fs) => {
                if enable_event_logging {
                    debug!("Finality Signature: {} for {}", fs.signature, fs.block_hash);
                }
                let finality_signature = FinalitySignature::new(fs.clone());
                let res = sqlite_database
                    .save_finality_signature(
                        finality_signature.clone(),
                        sse_event.id.unwrap(),
                        sse_event.source,
                    )
                    .await;

                match res {
                    Ok(_) => event_stream_server.broadcast(SseData::FinalitySignature(fs)),
                    Err(DatabaseWriteError::UniqueConstraint(uc_err)) => {
                        debug!(
                            "Already received FinalitySignature ({}), logged in event_log",
                            fs.signature
                        );
                        trace!(?uc_err);
                    }
                    Err(other_err) => {
                        warn!(?other_err, "Unexpected error saving FinalitySignature")
                    }
                }
            }
            SseData::Step {
                era_id,
                execution_effect,
            } => {
                let step = Step::new(era_id, execution_effect.clone());
                if enable_event_logging {
                    info!("Step at era: {}", era_id.value());
                }
                let res = sqlite_database
                    .save_step(step, sse_event.id.unwrap(), sse_event.source)
                    .await;

                match res {
                    Ok(_) => event_stream_server.broadcast(SseData::Step {
                        era_id,
                        execution_effect,
                    }),
                    Err(DatabaseWriteError::UniqueConstraint(uc_err)) => {
                        debug!(
                            "Already received Step ({}), logged in event_log",
                            era_id.value()
                        );
                        trace!(?uc_err);
                    }
                    Err(other_err) => warn!(?other_err, "Unexpected error saving Step"),
                }
            }
            SseData::Shutdown => {
                warn!("Node ({}) is unavailable", sse_event.source);
                break;
            }
        }
    }
}

#[cfg(test)]
struct TestingConfig {
    config: Config,
}

#[cfg(test)]
impl TestingConfig {
    fn default() -> Self {
        let config = Config::new();

        Self { config }
    }

    fn set_storage_path(mut self, path: String) -> Self {
        self.config.storage.storage_path = path;
        self
    }

    fn set_node_connection_port(mut self, port: u16) -> Self {
        self.config.connection.node_connections[0].sse_port = port;
        self
    }

    fn set_max_sse_subscribers(mut self, num_subscribers: u32) -> Self {
        self.config.event_stream_server.max_concurrent_subscribers = num_subscribers;
        self
    }

    fn configure_retry_settings(mut self, max_retries: u8, delay_between_retries: u8) -> Self {
        for connection in &mut self.config.connection.node_connections {
            connection.max_retries = max_retries;
            connection.delay_between_retries_secs = delay_between_retries;
        }
        self
    }

    fn use_free_ports_for_servers(mut self) -> Self {
        let rest_server_port = portpicker::pick_unused_port().expect("Error getting free port");
        let sse_server_port = portpicker::pick_unused_port().expect("Error getting free port");
        self.config.rest_server.port = rest_server_port;
        self.config.event_stream_server.port = sse_server_port;

        self
    }

    fn inner(&self) -> Config {
        self.config.clone()
    }

    fn connection_port(&self) -> u16 {
        self.config.connection.node_connections[0].sse_port
    }

    fn rest_server_port(&self) -> u16 {
        self.config.rest_server.port
    }

    fn event_stream_server_port(&self) -> u16 {
        self.config.event_stream_server.port
    }
}

#[cfg(test)]
fn prepare_config(temp_storage: &TempDir) -> TestingConfig {
    let path_to_temp_storage = temp_storage
        .path()
        .to_str()
        .expect("Error getting path of temporary directory")
        .to_string();

    // Get an unused port to bind the mock event_stream_server to
    let port_for_mock_event_stream =
        portpicker::pick_unused_port().expect("Unable to get free port");

    // test_config points to the mock_node
    TestingConfig::default()
        .set_storage_path(path_to_temp_storage)
        .set_node_connection_port(port_for_mock_event_stream)
        .use_free_ports_for_servers()
}

#[cfg(test)]
mod unit_tests {
    use crate::read_config;

    #[test]
    fn should_parse_example_config_toml() {
        read_config("../EXAMPLE_CONFIG.toml").expect("Error parsing example config");
    }
}

#[cfg(test)]
mod integration_tests {
    use super::*;
    use crate::testing::fake_event_stream_server::{
        spin_up_fake_event_stream, EventStreamScenario,
    };
    use eventsource_stream::Eventsource;
    use futures_util::StreamExt;
    use reqwest::Client;
    use std::time::Duration;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore]
    async fn should_bind_to_mock_event_stream_and_shutdown_cleanly() {
        let temp_dir = TempDir::new().expect("Error creating temporary directory");

        let test_config = prepare_config(&temp_dir);

        // spin up a mock event stream server for the sidecar to listen to
        tokio::task::spawn(spin_up_fake_event_stream(
            test_config.connection_port(),
            EventStreamScenario::Realistic,
        ));

        // run the sidecar against the mock_node. Should receive 30 events then send the Shutdown.
        run(test_config.inner())
            .await
            .expect("Error running sidecar");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore]
    async fn should_allow_client_connection_to_sse() {
        let temp_dir = TempDir::new().expect("Error creating temporary directory");

        let test_config = prepare_config(&temp_dir);

        // spin up a mock event stream server for the sidecar to listen to
        tokio::task::spawn(spin_up_fake_event_stream(
            test_config.connection_port(),
            EventStreamScenario::Realistic,
        ));

        tokio::spawn(run(test_config.inner()));

        // Allow sidecar to spin up
        tokio::time::sleep(Duration::from_secs(3)).await;

        let sidecar_sse_url = format!(
            "http://127.0.0.1:{}/events/main",
            test_config.event_stream_server_port()
        );

        let mut sidecar_event_source = Client::new()
            .get(&sidecar_sse_url)
            .send()
            .await
            .expect("Error connecting to event stream of sidecar")
            .bytes_stream()
            .eventsource();

        while let Some(event) = sidecar_event_source.next().await {
            serde_json::from_str::<SseData>(&event.expect("Event was an error").data)
                .expect("Error parsing event");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore]
    async fn should_respond_to_rest_query() {
        let temp_dir = TempDir::new().expect("Error creating temporary directory");

        let test_config = prepare_config(&temp_dir);

        // spin up a mock event stream server for the sidecar to listen to
        tokio::task::spawn(spin_up_fake_event_stream(
            test_config.connection_port(),
            EventStreamScenario::Realistic,
        ));

        tokio::spawn(run(test_config.inner()));

        // Allow sidecar to spin up
        tokio::time::sleep(Duration::from_secs(3)).await;

        let latest_block_url = format!("http://127.0.0.1:{}/block", test_config.rest_server_port());

        let response = Client::new()
            .get(&latest_block_url)
            .send()
            .await
            .expect("Error requesting the /block endpoint");

        assert!(response.status().is_success());
    }
}

/// These tests NCTL to be running and deploys to be sent.
#[cfg(test)]
mod performance_tests {
    use super::*;
    use bytes::Bytes;
    use eventsource_stream::{EventStream, Eventsource};
    use futures::{Stream, StreamExt};
    use hex::encode;
    use std::println;
    use std::time::Duration;
    use tokio::time::Instant;

    #[derive(Clone)]
    struct EventWithHash {
        hash: String,
        received_at: Instant,
    }

    impl PartialEq for EventWithHash {
        fn eq(&self, other: &Self) -> bool {
            self.hash == other.hash
        }
    }

    const EVENT_COUNT: u8 = 30;
    const ACCEPTABLE_LAG_IN_MILLIS: u128 = 1000;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore]
    async fn check_delay_in_receiving_blocks() {
        let temp_dir = TempDir::new().expect("Error creating temporary directory");

        let perf_test_config = prepare_config(&temp_dir).set_node_connection_port(18101);

        tokio::spawn(run(perf_test_config.inner()));

        // Allow sidecar to spin up
        tokio::time::sleep(Duration::from_secs(3)).await;

        let source_sse_url = format!(
            "http://127.0.0.1:{}/events/main",
            perf_test_config.connection_port()
        );

        let source_event_stream = reqwest::Client::new()
            .get(&source_sse_url)
            .send()
            .await
            .expect("Error connecting to node")
            .bytes_stream()
            .eventsource();

        let sidecar_sse_url = format!(
            "http://127.0.0.1:{}/events/main",
            perf_test_config.event_stream_server_port()
        );

        let sidecar_event_stream = reqwest::Client::new()
            .get(&sidecar_sse_url)
            .send()
            .await
            .expect("Error connecting to sidecar")
            .bytes_stream()
            .eventsource();

        let source_task_handle =
            tokio::spawn(push_timestamped_block_events_to_vecs(source_event_stream));

        let sidecar_task_handle =
            tokio::spawn(push_timestamped_block_events_to_vecs(sidecar_event_stream));

        let (source_task_result, sidecar_task_result) =
            tokio::join!(source_task_handle, sidecar_task_handle);

        let (block_events_from_source, node_overall_duration) =
            source_task_result.expect("Error recording events from node");
        let (block_events_from_sidecar, sidecar_overall_duration) =
            sidecar_task_result.expect("Error recording events from sidecar");

        let block_time_diffs =
            extract_time_diffs(block_events_from_source, block_events_from_sidecar);

        let block_time_diff_millis = block_time_diffs
            .iter()
            .map(|time_diff| {
                println!(
                    "Block Time Diff: {} micros / {} ms",
                    time_diff.as_micros(),
                    time_diff.as_millis()
                );
                time_diff.as_millis()
            })
            .collect::<Vec<u128>>();

        let average_delay: u128 = block_time_diff_millis
            .iter()
            .sum::<u128>()
            .checked_div(block_time_diff_millis.len() as u128)
            .expect("Error calculating the average delay for blocks");

        println!(
            "\n\tBLOCKS RESULT:\n\
        \tAverage delay taken over {} matching block diffs = {} ms\n\
        \tOverall difference in time to receive {} events = {}ms\t (sidecar: {}s, node: {}s)\n",
            block_time_diff_millis.len(),
            average_delay,
            EVENT_COUNT,
            sidecar_overall_duration
                .as_millis()
                .checked_sub(node_overall_duration.as_millis())
                .expect("Error taking the difference in the overall durations"),
            sidecar_overall_duration.as_secs(),
            node_overall_duration.as_secs()
        );

        assert!(average_delay < ACCEPTABLE_LAG_IN_MILLIS);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore]
    async fn check_delay_in_receiving_deploys() {
        let temp_dir = TempDir::new().expect("Error creating temporary directory");

        let perf_test_config = prepare_config(&temp_dir).set_node_connection_port(18101);

        tokio::spawn(run(perf_test_config.inner()));

        // Allow sidecar to spin up
        tokio::time::sleep(Duration::from_secs(3)).await;

        let source_sse_url = format!(
            "http://127.0.0.1:{}/events/deploys",
            perf_test_config.connection_port()
        );

        let source_event_stream = reqwest::Client::new()
            .get(&source_sse_url)
            .send()
            .await
            .expect("Error connecting to source")
            .bytes_stream()
            .eventsource();

        let sidecar_sse_url = format!(
            "http://127.0.0.1:{}/events/deploys",
            perf_test_config.event_stream_server_port()
        );

        let sidecar_event_stream = reqwest::Client::new()
            .get(&sidecar_sse_url)
            .send()
            .await
            .expect("Error connecting to sidecar")
            .bytes_stream()
            .eventsource();

        let source_task_handle =
            tokio::spawn(push_timestamped_deploy_events_to_vecs(source_event_stream));

        let sidecar_task_handle =
            tokio::spawn(push_timestamped_deploy_events_to_vecs(sidecar_event_stream));

        let (source_task_result, sidecar_task_result) =
            tokio::join!(source_task_handle, sidecar_task_handle);

        let (deploy_events_from_source, source_overall_duration) =
            source_task_result.expect("Error recording events from source");
        let (deploy_events_from_sidecar, sidecar_overall_duration) =
            sidecar_task_result.expect("Error recording events from sidecar");

        assert_eq!(
            deploy_events_from_source.len(),
            deploy_events_from_source.len()
        );

        let deploy_time_diffs =
            extract_time_diffs(deploy_events_from_source, deploy_events_from_sidecar);

        let deploy_time_diff_millis = deploy_time_diffs
            .iter()
            .map(|time_diff| {
                println!(
                    "Deploy Time Diff: {} micros / {} ms",
                    time_diff.as_micros(),
                    time_diff.as_millis()
                );
                time_diff.as_millis()
            })
            .collect::<Vec<u128>>();

        let average_delay: u128 = deploy_time_diff_millis
            .iter()
            .sum::<u128>()
            .checked_div(deploy_time_diff_millis.len() as u128)
            .expect("Error calculating the average delay for deploys");

        println!(
            "\n\tDEPLOYS RESULT:\n\
        \tAverage delay taken over {} matching deploy diffs = {} ms\n\
        \tOverall difference in time to receive {} events = {}ms\t (sidecar: {}s, source: {}s)\n",
            deploy_time_diff_millis.len(),
            average_delay,
            EVENT_COUNT,
            sidecar_overall_duration
                .as_millis()
                .checked_sub(source_overall_duration.as_millis())
                .expect("Error taking the difference in the overall duration"),
            sidecar_overall_duration.as_secs(),
            source_overall_duration.as_secs()
        );

        assert!(average_delay < ACCEPTABLE_LAG_IN_MILLIS);
    }

    async fn push_timestamped_block_events_to_vecs(
        mut event_stream: EventStream<impl Stream<Item = Result<Bytes, reqwest::Error>> + Unpin>,
    ) -> (Vec<EventWithHash>, Duration) {
        let mut events_vec = Vec::new();

        let mut events_read = 0u8;

        let before = Instant::now();

        while let Some(event) = event_stream.next().await {
            let received_timestamp = Instant::now();
            let data = serde_json::from_str::<SseData>(
                &event.expect("Received error from event stream").data,
            )
            .expect("Error deserialising the event into SseData");
            if let SseData::BlockAdded { block_hash, .. } = data {
                events_read += 1;
                let hash = encode(block_hash.inner());
                events_vec.push(EventWithHash {
                    hash,
                    received_at: received_timestamp,
                });
            }
            if events_read >= EVENT_COUNT {
                break;
            }
        }

        let after = Instant::now();

        (events_vec, after.duration_since(before))
    }

    async fn push_timestamped_deploy_events_to_vecs(
        mut event_stream: EventStream<impl Stream<Item = Result<Bytes, reqwest::Error>> + Unpin>,
    ) -> (Vec<EventWithHash>, Duration) {
        let mut events_vec = Vec::new();

        let mut events_read = 0u8;

        let before = Instant::now();

        while let Some(event) = event_stream.next().await {
            let received_timestamp = Instant::now();
            let data = serde_json::from_str::<SseData>(
                &event.expect("Received error from event stream").data,
            )
            .expect("Error deserialising the event into SseData");
            if let SseData::DeployAccepted { deploy } = data {
                events_read += 1;
                let hash = encode(*deploy.id());
                events_vec.push(EventWithHash {
                    hash,
                    received_at: received_timestamp,
                })
            }
            if events_read >= EVENT_COUNT {
                break;
            }
        }

        let after = Instant::now();

        (events_vec, after.duration_since(before))
    }

    fn extract_time_diffs(
        events_from_node: Vec<EventWithHash>,
        events_from_sidecar: Vec<EventWithHash>,
    ) -> Vec<Duration> {
        let mut time_diffs = Vec::new();
        for node_event in events_from_node {
            for sidecar_event in &events_from_sidecar {
                if sidecar_event.eq(&node_event) {
                    time_diffs.push(sidecar_event.received_at - node_event.received_at);
                }
            }
        }

        time_diffs
    }
}
