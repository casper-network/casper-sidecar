extern crate core;

mod event_stream_server;
mod rest_server;
mod sqlite_db;
#[cfg(test)]
mod testing;
pub mod types;

use crate::event_stream_server::SseData;
use crate::sqlite_db::DatabaseWriter;
use crate::types::enums::DeployAtState;
use crate::types::structs::{Fault, Step};
use anyhow::{Context, Error};
use bytes::Bytes;
use casper_node::types::Block;
use casper_types::AsymmetricType;
use event_stream_server::{Config as SseConfig, EventStreamServer};
use eventsource_stream::{EventStream, Eventsource};
use futures::{Stream, StreamExt};
use rest_server::run_server as start_rest_server;
use sqlite_db::SqliteDb;
use std::path::{Path, PathBuf};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tracing::{debug, info, warn};
use tracing_subscriber;
use types::enums::Network;
use types::structs::{Config, DeployProcessed};

const CONNECTION_REFUSED: &str = "Connection refused (os error 111)";
const CONNECTION_ERR_MSG: &str = "Connection refused: Please check connection to node.";

fn parse_error_for_connection_refused(error: reqwest::Error) -> Error {
    if error.to_string().contains(CONNECTION_REFUSED) {
        Error::msg(&CONNECTION_ERR_MSG)
    } else {
        Error::from(error)
    }
}

pub fn read_config(config_path: &str) -> Result<Config, Error> {
    let toml_content = std::fs::read_to_string(config_path).context("Error reading config file contents")?;
    toml::from_str(&toml_content).context("Error parsing config into TOML format")
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    // Install global collector for tracing
    tracing_subscriber::fmt::init();

    let config: Config = read_config("config.toml").context("Error constructing config")?;
    info!("Configuration loaded");

    run(config).await
}

async fn run(config: Config) -> Result<(), Error> {

    let node_config = match config.connection.network {
        Network::Mainnet => config.connection.node.mainnet,
        Network::Testnet => config.connection.node.testnet,
        Network::Local => config.connection.node.local,
    };

    let url_base = format!("http://{ip}:{port}/events", ip = node_config.ip_address, port = node_config.sse_port);

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
                Err(serde_err) => return Err(Error::from(serde_err)
                    .context("First event was not of expected format")),
            },
        };

    info!(
        message = "Connected to node",
        network = config.connection.network.as_str(),
        api_version = api_version.to_string().as_str(),
        node_ip_address = node_config.ip_address.as_str()
    );

    // For each filtered Stream pass the events along a Sender which all feed into the
    // aggregate Receiver. The first event is (should be) the API Version which is already
    // extracted from the Main filter in the code above, however it can to be discarded
    // from the Deploys and Sigs filter streams.
    tokio::spawn(stream_events_to_channel(
        main_event_stream,
        main_events_tx,
        false
    ));
    tokio::spawn(stream_events_to_channel(
        deploys_event_stream,
        deploy_events_tx,
        true
    ));
    tokio::spawn(stream_events_to_channel(
        sigs_event_stream,
        sigs_event_tx,
        true
    ));

    // Instantiates SQLite database
    let storage: SqliteDb = SqliteDb::new(Path::new(&config.storage.db_path)).context("Error instantiating database")?;

    // // Prepare the REST server task - this will be executed later
    let rest_server_handle = tokio::spawn(
        start_rest_server(
            storage.file_path.clone(),
            config.rest_server.port,
        )
    );

    // Create new instance for the Sidecar's Event Stream Server
    let mut event_stream_server = EventStreamServer::new(
        SseConfig::new_on_port(config.sse_server.port),
        PathBuf::from(config.storage.sse_cache),
        api_version
    ).context("Error starting EventStreamServer")?;

    // Adds space under setup logs before stream starts for readability
    println!("\n\n");

    // Task to manage incoming events from all three filters
    let sse_processing_task = async {
        while let Some(evt) = aggregate_events_rx.recv().await {
            match serde_json::from_str::<SseData>(&evt.data) {
                Ok(sse_data) => {
                    event_stream_server.broadcast(sse_data.clone());

                    match sse_data {
                        SseData::ApiVersion(version) => {
                            info!("API Version: {:?}", version.to_string());
                        }
                        SseData::BlockAdded {block, ..} => {
                            let block = Block::from(*block);
                            info!(
                                message = "Block Added:",
                                hash = hex::encode(block.hash().inner()).as_str(),
                                height = block.height()
                            );
                            let res = storage.save_block(block.clone()).await;

                            if res.is_err() {
                                warn!("Error saving block: {}", res.err().unwrap());
                            }
                        }
                        SseData::DeployAccepted {deploy} => {
                            info!(
                                message = "Deploy Accepted:",
                                hash = hex::encode(deploy.id().inner()).as_str()
                            );
                            let res = storage.save_or_update_deploy(DeployAtState::Accepted(deploy))
                                .await;

                            if res.is_err() {
                                warn!("Error saving deploy: {:?}", res.unwrap_err().to_string());
                            }
                        }
                        SseData::DeployExpired {deploy_hash} => {
                            info!(
                                message = "Deploy expired:",
                                hash = hex::encode(deploy_hash.inner()).as_str()
                            );
                            let res = storage.save_or_update_deploy(DeployAtState::Expired(deploy_hash))
                                .await;

                            if res.is_err() {
                                warn!("Error updating expired deploy: {}", res.err().unwrap());
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
                            let deploy_processed = DeployProcessed {
                                account,
                                block_hash,
                                dependencies,
                                deploy_hash: deploy_hash.clone(),
                                execution_result,
                                ttl,
                                timestamp
                            };
                            info!(
                                message = "Deploy Processed:",
                                hash = hex::encode(deploy_hash.inner()).as_str()
                            );
                            let res = storage.save_or_update_deploy(DeployAtState::Processed(deploy_processed))
                                .await;

                            if res.is_err() {
                                warn!("Error updating processed deploy: {}", res.err().unwrap());
                            }
                        }
                        SseData::Fault {era_id, timestamp, public_key} => {
                            let fault = Fault {
                                era_id,
                                public_key: public_key.clone(),
                                timestamp
                            };
                            info!(
                                "\n\tFault reported!\n\tEra: {}\n\tPublic Key: {}\n\tTimestamp: {}",
                                era_id,
                                public_key.to_hex(),
                                timestamp
                            );
                            let res = storage.save_fault(fault).await;

                            if res.is_err() {
                                warn!("Error saving fault: {}", res.err().unwrap());
                            }
                        }
                        SseData::FinalitySignature(fs) => {
                            debug!("Finality signature, {}", fs.signature);
                        }
                        SseData::Step {era_id, execution_effect, } => {
                            let step = Step {
                                era_id,
                                execution_effect
                            };
                            info!("\n\tStep reached for Era: {}", era_id);
                            let res = storage.save_step(step).await;

                            if res.is_err() {
                                warn!("Error saving step: {}", res.err().unwrap());
                            }
                        }
                        SseData::Shutdown => {
                            warn!("Node is shutting down");
                            break
                        }
                    }
                }
                Err(err) => {
                    println!("{:?}", evt);
                    if err.to_string() == CONNECTION_REFUSED {
                        warn!("Connection to node lost...");
                    } else {
                        warn!("Error parsing SSE: {}, for data:\n{}\n", err.to_string(), &evt.data);
                    }
                    continue
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
    discard_first: bool
) {
    if discard_first {
        let _ = event_stream.next().await;
    }
    while let Some(event) = event_stream.next().await {
        match event {
            Ok(event) => {
                let _ = sender.send(event);
            }
            Err(error) => warn!("error receiving events: {}", error),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use hex::encode;
    use lazy_static::lazy_static;
    use serial_test::serial;
    use tokio::time::Instant;
    use super::*;
    use crate::testing::test_node::start_test_node_with_shutdown;

    const TEST_CONFIG_PATH: &str = "config_test.toml";

    lazy_static! {
        static ref TEST_CONFIG: Config = read_config(TEST_CONFIG_PATH).unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn should_return_helpful_error_if_node_unreachable() {
        let test_config = read_config(TEST_CONFIG_PATH).unwrap();

        let result = run(test_config).await;

        assert!(result.is_err());
        if let Some(error) = result.err() {
            assert_eq!(error.to_string(), CONNECTION_ERR_MSG)
        }
    }

    #[tokio::test(flavor="multi_thread", worker_threads=4)]
    #[serial]
    async fn should_connect_and_shutdown_cleanly() {
        let node_shutdown_tx = start_test_node_with_shutdown(4444, None).await;

        let test_config = read_config(TEST_CONFIG_PATH).unwrap();

        run(test_config).await.unwrap();

        node_shutdown_tx.send(()).unwrap();
    }

    #[tokio::test(flavor="multi_thread", worker_threads=4)]
    #[serial]
    async fn should_allow_client_connection_to_sse() {
        let node_shutdown_tx = start_test_node_with_shutdown(4444, Some(30)).await;

        let test_config = read_config(TEST_CONFIG_PATH).unwrap();

        tokio::spawn(run(test_config));

        // Allow sidecar to spin up
        tokio::time::sleep(Duration::from_secs(3)).await;

        let mut main_event_stream = reqwest::Client::new()
            .get("http://127.0.0.1:19999/events/main")
            .send()
            .await
            .map_err(parse_error_for_connection_refused)
            .unwrap()
            .bytes_stream()
            .eventsource();

        while let Some(event) = main_event_stream.next().await {
            event.unwrap();
        }

        node_shutdown_tx.send(()).unwrap();
    }

    #[tokio::test(flavor="multi_thread", worker_threads=4)]
    #[serial]
    async fn should_respond_to_rest_query() {
        let node_shutdown_tx = start_test_node_with_shutdown(4444, Some(30)).await;

        let test_config = read_config(TEST_CONFIG_PATH).unwrap();

        tokio::spawn(run(test_config));

        // Allow sidecar to spin up
        tokio::time::sleep(Duration::from_secs(3)).await;

        let response = reqwest::Client::new()
            .get("http://127.0.0.1:17777/block")
            .send()
            .await
            .unwrap();

        assert!(response.status().is_success());

        node_shutdown_tx.send(()).unwrap();
    }

    #[derive(Clone)]
    struct EventWithHash {
        hash: String,
        received_at: Instant
    }

    impl PartialEq for EventWithHash {
        fn eq(&self, other: &Self) -> bool {
            self.hash == other.hash
        }
    }

    #[tokio::test(flavor="multi_thread", worker_threads=4)]
    #[serial]
    // This test needs NCTL running in the background
    async fn check_delay_in_receiving_blocks() {
        // let node_shutdown_tx = start_test_node_with_shutdown(4444, Some(30)).await;

        let config = read_config("config.toml").unwrap();

        tokio::spawn(run(config));

        // Allow sidecar to spin up
        tokio::time::sleep(Duration::from_secs(3)).await;

        let node_event_stream = reqwest::Client::new()
            .get("http://127.0.0.1:18101/events/main")
            .send()
            .await
            .unwrap()
            .bytes_stream()
            .eventsource();

        let sidecar_event_stream = reqwest::Client::new()
            .get("http://127.0.0.1:19999/events/main")
            .send()
            .await
            .map_err(parse_error_for_connection_refused)
            .unwrap()
            .bytes_stream()
            .eventsource();

        let node_task_handle = tokio::spawn(push_timestamped_block_events_to_vecs(
            node_event_stream
        ));

        let sidecar_task_handle = tokio::spawn(push_timestamped_block_events_to_vecs(
            sidecar_event_stream
        ));

        let (node_task_result, sidecar_task_result) = tokio::join!(
            node_task_handle,
            sidecar_task_handle
        );

        let block_events_from_node = node_task_result.unwrap();
        let block_events_from_sidecar = sidecar_task_result.unwrap();

        let block_time_diffs = extract_time_diffs(block_events_from_node, block_events_from_sidecar);

        let block_time_diff_millis = block_time_diffs.iter().map(|time_diff| {
            time_diff.as_millis()
        }).collect::<Vec<u128>>();

        let average_delay: u128 = block_time_diff_millis.iter().sum::<u128>().checked_div(block_time_diff_millis.len() as u128).unwrap();

        println!("RESULT: ave. delay for blocks: {} ms", average_delay);

        assert!(average_delay < 20);
    }

    #[tokio::test(flavor="multi_thread", worker_threads=4)]
    #[serial]
    // This test needs NCTL running in the background with deploys being sent
    async fn check_delay_in_receiving_deploys() {
        // let node_shutdown_tx = start_test_node_with_shutdown(4444, Some(30)).await;

        let config = read_config("config.toml").unwrap();

        tokio::spawn(run(config));

        // Allow sidecar to spin up
        tokio::time::sleep(Duration::from_secs(3)).await;

        let node_event_stream = reqwest::Client::new()
            .get("http://127.0.0.1:18101/events/deploys")
            .send()
            .await
            .unwrap()
            .bytes_stream()
            .eventsource();

        let sidecar_event_stream = reqwest::Client::new()
            .get("http://127.0.0.1:19999/events/deploys")
            .send()
            .await
            .map_err(parse_error_for_connection_refused)
            .unwrap()
            .bytes_stream()
            .eventsource();

        let node_task_handle = tokio::spawn(push_timestamped_deploy_events_to_vecs(
            node_event_stream
        ));

        let sidecar_task_handle = tokio::spawn(push_timestamped_deploy_events_to_vecs(
            sidecar_event_stream
        ));

        let (node_task_result, sidecar_task_result) = tokio::join!(
            node_task_handle,
            sidecar_task_handle
        );

        let deploy_events_from_node = node_task_result.unwrap();
        let deploy_events_from_sidecar = sidecar_task_result.unwrap();

        let deploy_time_diffs = extract_time_diffs(deploy_events_from_node, deploy_events_from_sidecar);

        let deploy_time_diff_millis = deploy_time_diffs.iter().map(|time_diff| {
            time_diff.as_millis()
        }).collect::<Vec<u128>>();

        let average_delay: u128 = deploy_time_diff_millis.iter().sum::<u128>().checked_div(deploy_time_diff_millis.len() as u128).unwrap();

        println!("RESULT: ave. delay for deploys: {} ms", average_delay);

        assert!(average_delay < 10);
    }

    async fn push_timestamped_block_events_to_vecs(
        mut event_stream: EventStream<impl Stream<Item = Result<Bytes, reqwest::Error>> + Unpin>
    ) -> Vec<EventWithHash> {

        let mut events_vec = Vec::new();

        let mut event_count = 0u8;

        while let Some(event) = event_stream.next().await {
            if event_count > 12 {
                break
            }
            event_count += 1;

            let received_timestamp = Instant::now();
            let data = serde_json::from_str::<SseData>(&event.unwrap().data).unwrap();
            match data {
                SseData::BlockAdded { block_hash, .. } => {
                    let hash = encode(block_hash.inner());
                    events_vec.push(EventWithHash {
                        hash,
                        received_at: received_timestamp
                    });
                }
                _ => {}
            }

        }
        events_vec
    }

    async fn push_timestamped_deploy_events_to_vecs(
        mut event_stream: EventStream<impl Stream<Item = Result<Bytes, reqwest::Error>> + Unpin>
    ) -> Vec<EventWithHash> {

        let mut events_vec = Vec::new();

        let mut event_count = 0u8;

        while let Some(event) = event_stream.next().await {
            if event_count > 12 {
                break
            }
            event_count += 1;

            let received_timestamp = Instant::now();
            let data = serde_json::from_str::<SseData>(&event.unwrap().data).unwrap();
            match data {
                SseData::DeployAccepted { deploy } => {
                    let hash = encode(*deploy.id());
                    events_vec.push(EventWithHash {
                        hash,
                        received_at: received_timestamp
                    })
                }
                _ => {}
            }

        }
        events_vec
    }

    fn extract_time_diffs(events_from_node: Vec<EventWithHash>, events_from_sidecar: Vec<EventWithHash>) -> Vec<Duration> {
        events_from_node.iter().map(|event_from_node| {
            let cloned_events_from_sidecar = events_from_sidecar.clone();
            cloned_events_from_sidecar.iter().map(|event_from_sidecar| {
                if event_from_sidecar.eq(&event_from_node) {
                    let time_difference = event_from_sidecar.received_at - event_from_node.received_at;
                    return Some(time_difference)
                }
                return None
            }).reduce(|previous, current| {
                if current.is_some() { current } else { previous }
            }).map(|reduced| {
                reduced.unwrap()
            })
        }).map(|opt_time_difference| {
            opt_time_difference.unwrap()
        }).collect::<Vec<Duration>>()
    }
}
