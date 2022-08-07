extern crate core;

mod sqlite_db;
mod rest_server;
mod event_stream_server;
pub mod types;
#[cfg(test)]
mod testing;

use std::path::{Path, PathBuf};
use anyhow::{Context, Error};
use casper_node::types::Block;
use casper_types::AsymmetricType;
use sqlite_db::SqliteDb;
use sse_client::EventSource;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tracing::{debug, info, warn};
use tracing_subscriber;
use types::structs::{Config, DeployProcessed};
use types::enums::Network;
use rest_server::run_server as start_rest_server;
use event_stream_server::{EventStreamServer, Config as SseConfig};
use crate::event_stream_server::SseData;
use crate::sqlite_db::DatabaseWriter;
use crate::types::enums::DeployAtState;
use crate::types::structs::{Fault, Step};

const CONNECTION_REFUSED: &str = "Connection refused (os error 111)";
const CONNECTION_ERR_MSG: &str = "Connection refused: Please check connection to node.";

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

    let main_event_source = EventSource::new(
        format!("{}/main", url_base).as_str(),
    ).context("Error constructing EventSource for Main filter")?;

    let deploys_event_source = EventSource::new(
        format!("{}/deploys", url_base).as_str(),
    ).context("Error constructing EventSource for Deploys filter")?;

    let sigs_event_source = EventSource::new(
        format!("{}/sigs", url_base).as_str(),
    ).context("Error constructing EventSource for Signatures filter")?;

    // Channel for funnelling all event types into.
    let (aggregate_events_tx, mut aggregate_events_rx) = unbounded_channel();
    // Clone the aggregate sender for each event type. These will all feed into the aggregate receiver.
    let main_events_tx = aggregate_events_tx.clone();
    let deploy_events_tx = aggregate_events_tx.clone();
    let sigs_event_tx = aggregate_events_tx.clone();

    // This local var for `main_recv` needs to be here as calling .receiver() on the EventSource more than
    // once will fail due to the resources being exhausted/dropped on the first call.
    let main_event_source_receiver = main_event_source.receiver();

    // Parse the first event to see if the connection was successful
    let api_version = match main_event_source_receiver.iter().next() {
        None => return Err(Error::msg("First event was empty")),
        Some(event) => match serde_json::from_str::<SseData>(&event.data) {
            Ok(sse_data) => match sse_data {
                SseData::ApiVersion(version) => version,
                _ => return Err(Error::msg("First event should have been API Version"))
            }
            Err(serde_err) => return match event.data.as_str() {
                CONNECTION_REFUSED => Err(Error::msg(&CONNECTION_ERR_MSG)),
                _ => Err(Error::from(serde_err).context("First event was not of expected format"))
            }
        }
    };

    info!(
        message = "Connected to node",
        network = config.connection.network.as_str(),
        api_version = api_version.to_string().as_str(),
        node_ip_address = node_config.ip_address.as_str()
    );

    // Loop over incoming events and send them along the channel
    // The main receiver loop doesn't need to discard the first event because it has already been parsed out above.
    let main_events_sending_task = async move {
        // arbitrary timeout
        for event in main_event_source_receiver.iter() {
            let _ = main_events_tx.send(event);
        }
    };
    let deploy_events_sending_task = async move {
        send_events_discarding_first(deploys_event_source, deploy_events_tx);
    };
    let sig_events_sending_task = async move {
        send_events_discarding_first(sigs_event_source, sigs_event_tx);
    };

    // Spin up each of the sending tasks
    tokio::spawn(main_events_sending_task);
    tokio::spawn(deploy_events_sending_task);
    tokio::spawn(sig_events_sending_task);

    // Instantiates SQLite database
    let storage: SqliteDb = SqliteDb::new(Path::new(&config.storage.db_path)).context("Error instantiating database")?;

    // // Prepare the REST server task - this will be executed later
    // let rest_server_handle = tokio::spawn(
    //     start_rest_server(
    //         storage.file_path.clone(),
    //         config.rest_server.port,
    //     )
    // );

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

        // _ = rest_server_handle => {
        //     info!("REST server stopped")
        // }
    };

    Ok(())
}

fn send_events_discarding_first(event_source: EventSource, sender: UnboundedSender<sse_client::Event>) {
    // This local var for `receiver` needs to be there as calling .receiver() on the EventSource more than
    // once will fail due to the resources being exhausted/dropped on the first call.
    let receiver = event_source.receiver();
    let _ = receiver.iter().next();
    for event in receiver.iter() {
        let _ = sender.send(event);
    }
}

#[cfg(test)]
mod tests {
    use lazy_static::lazy_static;
    use serial_test::serial;
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

    #[tokio::test(flavor="multi_thread", worker_threads=8)]
    #[serial]
    async fn should_run() {
        let node_shutdown_tx = start_test_node_with_shutdown(4444).await;

        let test_config = read_config(TEST_CONFIG_PATH).unwrap();

        run(test_config).await.unwrap();

        node_shutdown_tx.send(()).unwrap();
        println!("ended");
    }
}
