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

use anyhow::{Context, Error};
use bytes::Bytes;
use eventsource_stream::{EventStream, Eventsource};
use futures::{Stream, StreamExt};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tracing::{debug, info, warn};

use crate::{
    event_stream_server::{Config as SseConfig, EventStreamServer, SseData},
    rest_server::run_server as start_rest_server,
    sqlite_database::SqliteDatabase,
    types::{config::Config, database::DatabaseWriter, sse_events::*},
};

const CONNECTION_REFUSED: &str = "Connection refused (os error 111)";
const CONNECTION_ERR_MSG: &str = "Connection refused: Please check connection to node.";
const CONFIG_PATH: &str = "config.toml";

fn parse_error_for_connection_refused(error: reqwest::Error) -> Error {
    if error.to_string().contains(CONNECTION_REFUSED) {
        Error::msg(CONNECTION_ERR_MSG)
    } else {
        Error::from(error)
    }
}

pub fn read_config(config_path: &str) -> Result<Config, Error> {
    let toml_content =
        std::fs::read_to_string(config_path).context("Error reading config file contents")?;
    toml::from_str(&toml_content).context("Error parsing config into TOML format")
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    // Install global collector for tracing
    tracing_subscriber::fmt::init();

    let config: Config = read_config(CONFIG_PATH).context("Error constructing config")?;
    info!("Configuration loaded");

    run(config).await
}

async fn run(config: Config) -> Result<(), Error> {
    let url_base = format!(
        "http://{ip}:{port}/events",
        ip = config.node_connection.ip_address,
        port = config.node_connection.sse_port
    );

    let mut main_event_stream = reqwest::Client::new()
        .get(format!("{}/main", url_base).as_str())
        .send()
        .await
        .map_err(parse_error_for_connection_refused)?
        .bytes_stream()
        .eventsource();

    let deploys_event_stream = reqwest::Client::new()
        .get(format!("{}/deploys", url_base).as_str())
        .send()
        .await
        .map_err(parse_error_for_connection_refused)?
        .bytes_stream()
        .eventsource();

    let sigs_event_stream = reqwest::Client::new()
        .get(format!("{}/sigs", url_base).as_str())
        .send()
        .await
        .map_err(parse_error_for_connection_refused)?
        .bytes_stream()
        .eventsource();

    // Channel for funnelling all event types into.
    let (aggregate_events_tx, mut aggregate_events_rx) = unbounded_channel();
    // Clone the aggregate sender for each event type. These will all feed into the aggregate receiver.
    let main_events_tx = aggregate_events_tx.clone();
    let deploy_events_tx = aggregate_events_tx.clone();
    let sigs_event_tx = aggregate_events_tx.clone();

    // Parse the first event to see if the connection was successful
    let api_version =
        match main_event_stream.next().await {
            None => return Err(Error::msg("First event was empty")),
            Some(Err(error)) => {
                return Err(Error::msg(format!("failed to get first event: {}", error)))
            }
            Some(Ok(event)) => match serde_json::from_str::<SseData>(&event.data) {
                Ok(sse_data) => match sse_data {
                    SseData::ApiVersion(version) => version,
                    _ => return Err(Error::msg("First event should have been API Version")),
                },
                Err(serde_err) => {
                    return match event.data.as_str() {
                        CONNECTION_REFUSED => Err(Error::msg(
                            "Connection refused: Please check network connection to node.",
                        )),
                        _ => Err(Error::from(serde_err)
                            .context("First event was not of expected format")),
                    }
                }
            },
        };

    info!(
        message = "Connected to node",
        api_version = api_version.to_string().as_str(),
        node_ip_address = config.node_connection.ip_address.as_str()
    );

    // For each filtered Stream pass the events along a Sender which all feed into the
    // aggregate Receiver. The first event is (should be) the API Version which is already
    // extracted from the Main filter in the code above, however it can to be discarded
    // from the Deploys and Sigs filter streams.
    tokio::spawn(stream_events_to_channel(
        main_event_stream,
        main_events_tx,
        false,
    ));
    tokio::spawn(stream_events_to_channel(
        deploys_event_stream,
        deploy_events_tx,
        true,
    ));
    tokio::spawn(stream_events_to_channel(
        sigs_event_stream,
        sigs_event_tx,
        true,
    ));

    let path_to_database_dir = Path::new(&config.storage.storage_path);

    // Creates and initialises Sqlite database
    let sqlite_database =
        SqliteDatabase::new(path_to_database_dir, config.storage.sqlite_config.clone())
            .await
            .context("Error instantiating database")?;

    // Prepare the REST server task - this will be executed later
    let rest_server_handle = tokio::spawn(start_rest_server(
        config.rest_server.ip_address,
        config.rest_server.port,
        sqlite_database.file_path.clone(),
        config.storage.sqlite_config.max_read_connections,
    ));

    // Create new instance for the Sidecar's Event Stream Server
    let mut event_stream_server = EventStreamServer::new(
        SseConfig::new_on_specified(config.sse_server.ip_address, config.sse_server.port),
        PathBuf::from(config.storage.sse_cache_path),
        api_version,
    )
    .context("Error starting EventStreamServer")?;

    // Adds space under setup logs before stream starts for readability
    println!("\n\n");

    let node_ip_address = config.node_connection.ip_address.clone();

    // Task to manage incoming events from all three filters
    let sse_processing_task = async {
        while let Some(evt) = aggregate_events_rx.recv().await {
            let event_id: u64 = evt.id.as_str().parse().map_err(Error::from)?;
            let event_source_address = node_ip_address.clone();

            match serde_json::from_str::<SseData>(&evt.data) {
                Ok(sse_data) => {
                    event_stream_server.broadcast(sse_data.clone());

                    match sse_data {
                        SseData::ApiVersion(version) => {
                            info!(%version, "API Version");
                        }
                        SseData::BlockAdded { block, block_hash } => {
                            info!(%block_hash, "Block Added");
                            let res = sqlite_database
                                .save_block_added(
                                    BlockAdded::new(block_hash, block),
                                    event_id,
                                    event_source_address,
                                )
                                .await;

                            if let Err(error) = res {
                                warn!(%error, "Error saving block");
                            }
                        }
                        SseData::DeployAccepted { deploy } => {
                            let deploy_accepted = DeployAccepted::new(deploy);
                            info!(deploy_hash=%deploy_accepted.deploy_hash(), "Deploy Accepted");
                            let res = sqlite_database
                                .save_deploy_accepted(
                                    deploy_accepted,
                                    event_id,
                                    event_source_address,
                                )
                                .await;

                            if let Err(error) = res {
                                warn!(%error, "Error saving deploy accepted");
                            }
                        }
                        SseData::DeployExpired { deploy_hash } => {
                            info!(%deploy_hash, "Deploy Expired");
                            let res = sqlite_database
                                .save_deploy_expired(
                                    DeployExpired::new(deploy_hash),
                                    event_id,
                                    event_source_address,
                                )
                                .await;

                            if let Err(error) = res {
                                warn!(%error, "Error saving deploy expired");
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
                            let deploy_processed = DeployProcessed::new(
                                deploy_hash.clone(),
                                account,
                                timestamp,
                                ttl,
                                dependencies,
                                block_hash,
                                execution_result,
                            );
                            info!(%deploy_hash, "Deploy Processed");
                            let res = sqlite_database
                                .save_deploy_processed(
                                    deploy_processed,
                                    event_id,
                                    event_source_address,
                                )
                                .await;

                            if let Err(error) = res {
                                warn!(%error, "Error saving deploy processed");
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
                                .save_fault(fault, event_id, event_source_address)
                                .await;

                            if let Err(error) = res {
                                warn!("Error saving fault: {}", error);
                            }
                        }
                        SseData::FinalitySignature(fs) => {
                            debug!(%fs.signature, "Finality signature");
                            let finality_signature = FinalitySignature::new(fs);
                            let res = sqlite_database
                                .save_finality_signature(
                                    finality_signature,
                                    event_id,
                                    event_source_address,
                                )
                                .await;

                            if let Err(error) = res {
                                warn!(%error, "Error saving finality signature")
                            }
                        }
                        SseData::Step {
                            era_id,
                            execution_effect,
                        } => {
                            let step = Step::new(era_id, execution_effect);
                            info!(%era_id, "Step");
                            let res = sqlite_database
                                .save_step(step, event_id, event_source_address)
                                .await;

                            if let Err(error) = res {
                                warn!("Error saving step: {}", error);
                            }
                        }
                        SseData::Shutdown => {
                            warn!("Node is shutting down");
                            break;
                        }
                    }
                }
                Err(err) => {
                    warn!(?evt, "Error from stream");
                    if err.to_string() == CONNECTION_REFUSED {
                        warn!("Connection to node lost...");
                    } else {
                        warn!(
                            "Error parsing SSE: {}, for data:\n{}\n",
                            err.to_string(),
                            &evt.data
                        );
                    }
                    continue;
                }
            }
        }
        Result::<_, Error>::Ok(())
    };

    tokio::select! {
        _ = sse_processing_task => {
            info!("Stopped processing SSEs")
        }

        _ = rest_server_handle => {
            info!("REST server stopped")
        }
    };

    Ok(())
}

async fn stream_events_to_channel(
    mut event_stream: EventStream<impl Stream<Item = Result<Bytes, reqwest::Error>> + Unpin>,
    sender: UnboundedSender<eventsource_stream::Event>,
    discard_first: bool,
) {
    if discard_first {
        let _ = event_stream.next().await;
    }
    while let Some(event) = event_stream.next().await {
        match event {
            Ok(event) => {
                let _ = sender.send(event);
            }
            Err(error) => warn!(%error, "Error receiving events"),
        }
        if discard_first {
            let _ = event_stream.next().await;
        }
        while let Some(event) = event_stream.next().await {
            match event {
                Ok(event) => {
                    let _ = sender.send(event);
                }
                Err(error) => warn!(%error, "Error receiving events"),
            }
        }
    }
}

#[cfg(test)]
mod unit_tests {
    use crate::{read_config, CONFIG_PATH};

    #[test]
    fn should_parse_config_toml_files() {
        read_config(CONFIG_PATH).expect("Error parsing config.toml");
        read_config("config_test.toml").expect("Error parsing config_test.toml");
    }
}

#[cfg(test)]
mod integration_tests {
    use super::*;
    use crate::testing::test_node::start_test_node_with_shutdown;
    use serial_test::serial;
    use std::time::Duration;

    const TEST_CONFIG_PATH: &str = "config_test.toml";

    #[tokio::test]
    #[serial]
    #[ignore]
    async fn should_return_helpful_error_if_node_unreachable() {
        let test_config = read_config(TEST_CONFIG_PATH).expect("Error reading config file");

        let result = run(test_config).await;

        assert!(result.is_err());
        if let Some(error) = result.err() {
            assert_eq!(error.to_string(), CONNECTION_ERR_MSG)
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    #[ignore]
    async fn should_connect_and_shutdown_cleanly() {
        let node_shutdown_tx = start_test_node_with_shutdown(4444, None).await;

        let test_config = read_config(TEST_CONFIG_PATH).expect("Error reading config file");

        run(test_config).await.expect("Error running sidecar");

        node_shutdown_tx.send(()).unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    #[ignore]
    async fn should_allow_client_connection_to_sse() {
        let node_shutdown_tx = start_test_node_with_shutdown(4444, Some(30)).await;

        let test_config = read_config(TEST_CONFIG_PATH).expect("Error reading config file");

        tokio::spawn(run(test_config));

        // Allow sidecar to spin up
        tokio::time::sleep(Duration::from_secs(3)).await;

        let mut main_event_stream = reqwest::Client::new()
            .get("http://127.0.0.1:19999/events/main")
            .send()
            .await
            .map_err(parse_error_for_connection_refused)
            .expect("Error in main event stream")
            .bytes_stream()
            .eventsource();

        while let Some(event) = main_event_stream.next().await {
            event.expect("Error from event stream - event should have been OK");
        }

        node_shutdown_tx.send(()).unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    #[ignore]
    async fn should_respond_to_rest_query() {
        let node_shutdown_tx = start_test_node_with_shutdown(4444, Some(30)).await;

        let test_config = read_config(TEST_CONFIG_PATH).expect("Error reading config file");

        tokio::spawn(run(test_config));

        // Allow sidecar to spin up
        tokio::time::sleep(Duration::from_secs(3)).await;

        let response = reqwest::Client::new()
            .get("http://127.0.0.1:17777/block")
            .send()
            .await
            .expect("Error requesting the /block endpoint");

        assert!(response.status().is_success());

        node_shutdown_tx.send(()).unwrap();
    }
}

#[cfg(test)]
mod performance_tests {
    use super::*;
    use hex::encode;
    use serial_test::serial;
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
    #[serial]
    #[ignore]
    // This test needs NCTL running in the background
    async fn check_delay_in_receiving_blocks() {
        let config = read_config(CONFIG_PATH).expect("Error reading config");

        tokio::spawn(run(config));

        // Allow sidecar to spin up
        tokio::time::sleep(Duration::from_secs(3)).await;

        let node_event_stream = reqwest::Client::new()
            .get("http://127.0.0.1:18101/events/main")
            .send()
            .await
            .expect("Error connecting to node")
            .bytes_stream()
            .eventsource();

        let sidecar_event_stream = reqwest::Client::new()
            .get("http://127.0.0.1:19999/events/main")
            .send()
            .await
            .map_err(parse_error_for_connection_refused)
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
    #[serial]
    #[ignore]
    // This test needs NCTL running in the background with deploys being sent
    async fn check_delay_in_receiving_deploys() {
        let config = read_config(CONFIG_PATH).expect("Error reading config");

        tokio::spawn(run(config));

        // Allow sidecar to spin up
        tokio::time::sleep(Duration::from_secs(3)).await;

        let node_event_stream = reqwest::Client::new()
            .get("http://127.0.0.1:18101/events/deploys")
            .send()
            .await
            .expect("Error connecting to node")
            .bytes_stream()
            .eventsource();

        let sidecar_event_stream = reqwest::Client::new()
            .get("http://127.0.0.1:19999/events/deploys")
            .send()
            .await
            .map_err(parse_error_for_connection_refused)
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
