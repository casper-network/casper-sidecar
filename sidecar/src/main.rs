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

use casper_event_listener::EventListener;
use casper_event_types::SseData;

use anyhow::{Context, Error};
use clap::Parser;
use futures::future::join_all;
use hex_fmt::HexFmt;
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

async fn run(config: Config) -> Result<(), Error> {
    let path_to_database_dir = Path::new(&config.storage.storage_path);

    // Creates and initialises Sqlite database
    let sqlite_database =
        SqliteDatabase::new(path_to_database_dir, config.storage.sqlite_config.clone())
            .await
            .context("Error instantiating database")?;

    // Prepare the REST server task - this will be executed later
    let rest_server_handle = tokio::spawn(start_rest_server(
        config.rest_server.ip_address.clone(),
        config.rest_server.port,
        sqlite_database.file_path.clone(),
        config.storage.sqlite_config.max_read_connections,
    ));

    // Task to manage incoming events from all three filters
    let processor_task_handle = async move {
        let mut join_handles = Vec::with_capacity(config.node_connections.len());

        for (index, connection) in config.node_connections.into_iter().enumerate() {
            let bind_address = format!("{}:{}", connection.ip_address, connection.sse_port);

            let event_listener = EventListener::new(
                bind_address,
                connection.max_retries,
                connection.delay_between_retries_secs,
            )
            .await?;

            // Only initialise the passthrough for one (the first) node connection.
            let event_stream_server = if index == 0 {
                let event_stream_server_address = format!(
                    "{}:{}",
                    config.event_stream_server.ip_address, config.event_stream_server.port
                );

                Some(
                    EventStreamServer::new(
                        SseConfig::new(
                            Some(event_stream_server_address),
                            Some(config.event_stream_server.event_stream_buffer_length),
                            Some(config.event_stream_server.max_concurrent_subscribers),
                        ),
                        PathBuf::from(&config.storage.storage_path),
                        event_listener.api_version,
                    )
                    .context("Failed initialise event stream server!")?,
                )
            } else {
                None
            };

            let join_handle = tokio::spawn(sse_processor(
                event_listener,
                event_stream_server,
                sqlite_database.clone(),
                connection.enable_event_logging,
            ));

            join_handles.push(join_handle);
        }

        let _ = join_all(join_handles).await;

        Ok::<(), Error>(())
    };

    tokio::select! {
        result = rest_server_handle => { warn!(?result, "REST server shutting down") }
        result = processor_task_handle => { warn!(?result, "SSE processing ended")}
    }

    Ok(())
}

async fn sse_processor(
    sse_event_listener: EventListener,
    event_stream_server: Option<EventStreamServer>,
    sqlite_database: SqliteDatabase,
    enable_event_logging: bool,
) {
    let mut sse_data_stream = sse_event_listener.stream_events().await;

    while let Some(sse_event) = sse_data_stream.recv().await {
        if let Some(server) = event_stream_server.as_ref() {
            server.broadcast(sse_event.data.clone());
        }

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
                        BlockAdded::new(block_hash, block),
                        sse_event.id.unwrap(),
                        sse_event.source,
                    )
                    .await;

                if let Err(db_write_err) = res {
                    match db_write_err {
                        DatabaseWriteError::UniqueConstraint(uc_err) => trace!(
                            ?uc_err,
                            "Already received BlockAdded ({}), logged in event_log",
                            full_length_hash
                        ),
                        _ => warn!(?db_write_err, "Error saving BlockAdded"),
                    }
                }
            }
            SseData::DeployAccepted { deploy } => {
                let full_length_hash = format!("{}", HexFmt(deploy.id().inner()));
                if enable_event_logging {
                    info!("Deploy Accepted: {:18}", HexFmt(deploy.id().inner()));
                    debug!("Deploy Accepted: {}", full_length_hash);
                }
                let deploy_accepted = DeployAccepted::new(deploy);
                let res = sqlite_database
                    .save_deploy_accepted(deploy_accepted, sse_event.id.unwrap(), sse_event.source)
                    .await;

                if let Err(db_write_err) = res {
                    match db_write_err {
                        DatabaseWriteError::UniqueConstraint(uc_err) => trace!(
                            ?uc_err,
                            "Already received DeployAccepted ({}), logged in event_log",
                            full_length_hash
                        ),
                        _ => warn!(?db_write_err, "Error saving DeployAccepted"),
                    }
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

                if let Err(db_write_err) = res {
                    match db_write_err {
                        DatabaseWriteError::UniqueConstraint(uc_err) => trace!(
                            ?uc_err,
                            "Already received DeployExpired ({}), logged in event_log",
                            full_length_hash
                        ),
                        _ => warn!(?db_write_err, "Error saving DeployExpired"),
                    }
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
                    account,
                    timestamp,
                    ttl,
                    dependencies,
                    block_hash,
                    execution_result,
                );
                let res = sqlite_database
                    .save_deploy_processed(
                        deploy_processed,
                        sse_event.id.unwrap(),
                        sse_event.source,
                    )
                    .await;

                if let Err(db_write_err) = res {
                    match db_write_err {
                        DatabaseWriteError::UniqueConstraint(uc_err) => trace!(
                            ?uc_err,
                            "Already received DeployProcessed ({}), logged in event_log",
                            full_length_hash
                        ),
                        _ => warn!(?db_write_err, "Error saving DeployProcessed"),
                    }
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

                if let Err(db_write_err) = res {
                    match db_write_err {
                        DatabaseWriteError::UniqueConstraint(uc_err) => trace!(
                            ?uc_err,
                            "Already received Fault ({:#?}), logged in event_log",
                            fault
                        ),
                        _ => warn!(?db_write_err, "Error saving Fault"),
                    }
                }
            }
            SseData::FinalitySignature(fs) => {
                if enable_event_logging {
                    debug!("Finality Signature: {}", fs.signature);
                }
                let finality_signature = FinalitySignature::new(fs);
                let res = sqlite_database
                    .save_finality_signature(
                        finality_signature.clone(),
                        sse_event.id.unwrap(),
                        sse_event.source,
                    )
                    .await;

                if let Err(db_write_err) = res {
                    match db_write_err {
                        DatabaseWriteError::UniqueConstraint(uc_err) => trace!(
                            ?uc_err,
                            "Already received FinalitySignature ({:#?}), logged in event_log",
                            finality_signature
                        ),
                        _ => warn!(?db_write_err, "Error saving FinalitySignature"),
                    }
                }
            }
            SseData::Step {
                era_id,
                execution_effect,
            } => {
                let step = Step::new(era_id, execution_effect);
                if enable_event_logging {
                    info!("Step at era: {}", era_id.value());
                }
                let res = sqlite_database
                    .save_step(step, sse_event.id.unwrap(), sse_event.source)
                    .await;

                if let Err(db_write_err) = res {
                    match db_write_err {
                        DatabaseWriteError::UniqueConstraint(uc_err) => trace!(
                            ?uc_err,
                            "Already received Step ({}), logged in event_log",
                            era_id.value()
                        ),
                        _ => warn!(?db_write_err, "Error saving Step"),
                    }
                }
            }
            SseData::Shutdown => {
                warn!("Node ({}) is shutting down", sse_event.source);
                break;
            }
        }
    }
}

/// A convenience wrapper around [Config] with a [Drop] impl that removes the `test_storage` dir created in `target` during testing.
/// This means there is no need to explicitly remove the directory at the end of the tests which is liable to be skipped if the test fails earlier.
#[cfg(test)]
struct ConfigWithCleanup {
    config: Config,
}

#[cfg(test)]
impl ConfigWithCleanup {
    fn new(path: &str) -> Self {
        let config = read_config(path).expect("Error parsing config file");
        Self { config }
    }

    fn set_node_connection_port(&mut self, port: u16) {
        self.config.node_connections[0].sse_port = port;
    }

    fn use_free_ports_for_servers(&mut self) -> (u16, u16) {
        let rest_server_port = portpicker::pick_unused_port().expect("Error getting free port");
        let sse_server_port = portpicker::pick_unused_port().expect("Error getting free port");
        self.config.rest_server.port = rest_server_port;
        self.config.event_stream_server.port = sse_server_port;

        (rest_server_port, sse_server_port)
    }
}

#[cfg(test)]
impl Drop for ConfigWithCleanup {
    fn drop(&mut self) {
        let path_to_test_storage = Path::new(&self.config.storage.storage_path);
        if path_to_test_storage.exists() {
            let res = std::fs::remove_dir_all(path_to_test_storage);
            if let Err(error) = res {
                println!("Error removing test_storage dir: {}", error);
            }
        }
    }
}

#[cfg(test)]
mod unit_tests {
    use crate::read_config;

    #[test]
    fn should_parse_test_config_toml_files() {
        read_config("config_test.toml").expect("Error parsing config_test.toml");
        read_config("config_perf_test.toml").expect("Error parsing config_perf_test.toml");
    }
}

#[cfg(test)]
mod integration_tests {
    use super::*;
    use crate::testing::mock_node::start_mock_node_with_shutdown;
    use eventsource_stream::Eventsource;
    use futures_util::StreamExt;
    use reqwest::Client;
    use std::time::Duration;

    const TEST_CONFIG_PATH: &str = "config_test.toml";

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    // #[ignore]
    async fn should_connect_and_shutdown_cleanly() {
        // Spin up mock_node
        let (port, node_shutdown_tx) = start_mock_node_with_shutdown(None).await;

        // test_config points to the mock_node
        let mut test_config = ConfigWithCleanup::new(TEST_CONFIG_PATH);

        // replace the hard-coded port in the config with the port returned by the mock node
        test_config.set_node_connection_port(port);

        // replace the default server ports with dynamically generated ones to prevent conflicts with other tests.
        test_config.use_free_ports_for_servers();

        // run the sidecar against the mock_node. Should receive 30 events then send the Shutdown.
        run(test_config.config.clone())
            .await
            .expect("Error running sidecar");

        // shutdown the mock_node
        node_shutdown_tx.send(()).unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    // #[ignore]
    async fn should_allow_client_connection_to_sse() {
        // Spin up mock_node
        let (port, node_shutdown_tx) = start_mock_node_with_shutdown(Some(30)).await;

        // test_config points to the mock_node
        let mut test_config = ConfigWithCleanup::new(TEST_CONFIG_PATH);

        // replace the hard-coded port in the config with the port returned by the mock node
        test_config.set_node_connection_port(port);

        // replace the default server ports with dynamically generated ones to prevent conflicts with other tests.
        let (_, sse_server_port) = test_config.use_free_ports_for_servers();

        tokio::spawn(run(test_config.config.clone()));

        // Allow sidecar to spin up
        tokio::time::sleep(Duration::from_secs(3)).await;

        let sidecar_sse_url = format!("http://127.0.0.1:{}/events/main", sse_server_port);

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

        node_shutdown_tx.send(()).unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    // #[ignore]
    async fn should_respond_to_rest_query() {
        // Spin up mock_node
        let (port, node_shutdown_tx) = start_mock_node_with_shutdown(Some(30)).await;

        // test_config points to the mock_node
        let mut test_config = ConfigWithCleanup::new(TEST_CONFIG_PATH);

        // replace the hard-coded port in the config with the port returned by the mock node
        test_config.set_node_connection_port(port);

        // replace the default server ports with dynamically generated ones to prevent conflicts with other tests.
        let (rest_server_port, _) = test_config.use_free_ports_for_servers();

        tokio::spawn(run(test_config.config.clone()));

        // Allow sidecar to spin up
        tokio::time::sleep(Duration::from_secs(3)).await;

        let latest_block_url = format!("http://127.0.0.1:{}/block", rest_server_port);

        let response = Client::new()
            .get(&latest_block_url)
            .send()
            .await
            .expect("Error requesting the /block endpoint");

        assert!(response.status().is_success());

        node_shutdown_tx.send(()).unwrap();
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

    const PERF_TEST_CONFIG_PATH: &str = "config_perf_test.toml";
    const EVENT_COUNT: u8 = 30;
    const ACCEPTABLE_LAG_IN_MILLIS: u128 = 1000;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore]
    async fn check_delay_in_receiving_blocks() {
        let mut perf_test_config = ConfigWithCleanup::new(PERF_TEST_CONFIG_PATH);

        let (_, sse_server_port) = perf_test_config.use_free_ports_for_servers();

        tokio::spawn(run(perf_test_config.config.clone()));

        // Allow sidecar to spin up
        tokio::time::sleep(Duration::from_secs(3)).await;

        let node_event_stream = reqwest::Client::new()
            .get("http://127.0.0.1:18101/events/main")
            .send()
            .await
            .expect("Error connecting to node")
            .bytes_stream()
            .eventsource();

        let sidecar_sse_url = format!("http://127.0.0.1:{}/events/main", sse_server_port);

        let sidecar_event_stream = reqwest::Client::new()
            .get(&sidecar_sse_url)
            .send()
            .await
            .expect("Error connecting to sidecar")
            .bytes_stream()
            .eventsource();

        let node_task_handle =
            tokio::spawn(push_timestamped_block_events_to_vecs(node_event_stream));

        let sidecar_task_handle =
            tokio::spawn(push_timestamped_block_events_to_vecs(sidecar_event_stream));

        let (node_task_result, sidecar_task_result) =
            tokio::join!(node_task_handle, sidecar_task_handle);

        let (block_events_from_node, node_overall_duration) =
            node_task_result.expect("Error recording events from node");
        let (block_events_from_sidecar, sidecar_overall_duration) =
            sidecar_task_result.expect("Error recording events from sidecar");

        let block_time_diffs =
            extract_time_diffs(block_events_from_node, block_events_from_sidecar);

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
        let mut perf_test_config = ConfigWithCleanup::new(PERF_TEST_CONFIG_PATH);

        let (_, sse_server_port) = perf_test_config.use_free_ports_for_servers();

        tokio::spawn(run(perf_test_config.config.clone()));

        // Allow sidecar to spin up
        tokio::time::sleep(Duration::from_secs(3)).await;

        let node_event_stream = reqwest::Client::new()
            .get("http://127.0.0.1:18101/events/deploys")
            .send()
            .await
            .expect("Error connecting to node")
            .bytes_stream()
            .eventsource();

        let sidecar_sse_url = format!("http://127.0.0.1:{}/events/deploys", sse_server_port);

        let sidecar_event_stream = reqwest::Client::new()
            .get(&sidecar_sse_url)
            .send()
            .await
            .expect("Error connecting to sidecar")
            .bytes_stream()
            .eventsource();

        let node_task_handle =
            tokio::spawn(push_timestamped_deploy_events_to_vecs(node_event_stream));

        let sidecar_task_handle =
            tokio::spawn(push_timestamped_deploy_events_to_vecs(sidecar_event_stream));

        let (node_task_result, sidecar_task_result) =
            tokio::join!(node_task_handle, sidecar_task_handle);

        let (deploy_events_from_node, node_overall_duration) =
            node_task_result.expect("Error recording events from node");
        let (deploy_events_from_sidecar, sidecar_overall_duration) =
            sidecar_task_result.expect("Error recording events from sidecar");

        assert_eq!(deploy_events_from_node.len(), deploy_events_from_node.len());

        let deploy_time_diffs =
            extract_time_diffs(deploy_events_from_node, deploy_events_from_sidecar);

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
            .expect("Error calculating the average delay for blocks");

        println!(
            "\n\tDEPLOYS RESULT:\n\
        \tAverage delay taken over {} matching deploy diffs = {} ms\n\
        \tOverall difference in time to receive {} events = {}ms\t (sidecar: {}s, node: {}s)\n",
            deploy_time_diff_millis.len(),
            average_delay,
            EVENT_COUNT,
            sidecar_overall_duration
                .as_millis()
                .checked_sub(node_overall_duration.as_millis())
                .expect("Error taking the difference in the overall duration"),
            sidecar_overall_duration.as_secs(),
            node_overall_duration.as_secs()
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
        events_from_node
            .iter()
            .map(|event_from_node| {
                let cloned_events_from_sidecar = events_from_sidecar.clone();
                cloned_events_from_sidecar
                    .iter()
                    .map(|event_from_sidecar| {
                        if event_from_sidecar.eq(event_from_node) {
                            let time_difference =
                                event_from_sidecar.received_at - event_from_node.received_at;
                            return Some(time_difference);
                        }
                        None
                    })
                    .reduce(
                        |previous, current| {
                            if current.is_some() {
                                current
                            } else {
                                previous
                            }
                        },
                    )
                    .map(|reduced| reduced.expect("Reducer failed to get the Duration"))
            })
            .map(|opt_time_difference| {
                opt_time_difference.expect("Duration should have been populated")
            })
            .collect::<Vec<Duration>>()
    }
}
