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
use sqlite_db::Database;
use sse_client::EventSource;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tracing::{debug, info, warn};
use tracing_subscriber;
use types::structs::{Config, DeployProcessed};
use types::enums::Network;
use rest_server::start_server as start_rest_server;
use event_stream_server::{EventStreamServer, Config as SseConfig};
use crate::event_stream_server::SseData;
use crate::types::enums::DeployAtState;
use crate::types::structs::{Fault, Step};


pub fn read_config(config_path: &str) -> Result<Config, Error> {
    let toml_content = std::fs::read_to_string(config_path).context("Error reading config file contents")?;
    Ok(toml::from_str(&toml_content).context("Error parsing config into TOML format")?)
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let config: Config = read_config("config.toml").context("Error constructing config")?;
    info!("Configuration loaded");

    run(config).await
}

async fn run(config: Config) -> Result<(), Error> {
    // Install global collector for tracing
    tracing_subscriber::fmt::init();

    let node = match config.connection.network {
        Network::Mainnet => config.connection.node.mainnet,
        Network::Testnet => config.connection.node.testnet,
        Network::Local => config.connection.node.local,
    };

    // Set URLs for each of the (from the node) Event Stream Server's filters
    let url_base = format!("http://{ip}:{port}/events", ip = node.ip_address, port = node.sse_port);
    let sse_main = EventSource::new(
        format!("{}/main", url_base).as_str(),
    ).context("Error constructing EventSource for Main filter")?;
    let sse_deploys = EventSource::new(
        format!("{}/deploys", url_base).as_str(),
    ).context("Error constructing EventSource for Deploys filter")?;
    let sse_sigs = EventSource::new(
        format!("{}/sigs", url_base).as_str(),
    ).context("Error constructing EventSource for Signatures filter")?;

    // Create channels for each filter to funnel events to a single receiver.
    let (sse_data_sender, mut sse_data_receiver) = unbounded_channel();
    let sse_main_sender = sse_data_sender.clone();
    let sse_deploys_sender = sse_data_sender.clone();
    let sse_sigs_sender = sse_data_sender.clone();

    // This local var for `main_recv` needs to be there as calling .receiver() on the EventSource more than
    // once will fail due to the resources being exhausted/dropped on the first call.
    let main_recv = sse_main.receiver();
    // // first event should always be the API version
    let first_event = main_recv.iter().next();
    let first_event_data = serde_json::from_str::<SseData>(&first_event.unwrap().data).context("Error parsing first SSE into API version")?;

    let api_version = match first_event_data {
      SseData::ApiVersion(version) => version,
        _ => return Err(Error::msg("Unable to parse API version from event stream"))
    };

    // Loop over incoming events and send them along the channel
    // The main receiver loop doesn't need to discard the first event because it has already been parsed out above.
    let sse_main_receiver = async move {
        for event in main_recv.iter() {
            let _ = sse_main_sender.send(event);
        }
    };
    let sse_deploys_receiver = async move {
        send_events_discarding_first(sse_deploys, sse_deploys_sender);
    };
    let sse_sigs_receiver = async move {
        send_events_discarding_first(sse_sigs, sse_sigs_sender);
    };

    info!(
        message = "Connecting to SSE",
        network = config.connection.network.as_str(),
        api_version = api_version.to_string().as_str(),
        node_ip_address = node.ip_address.as_str()
    );

    // Spin up each of the receivers
    tokio::spawn(sse_main_receiver);
    tokio::spawn(sse_deploys_receiver);
    tokio::spawn(sse_sigs_receiver);

    // Instantiates SQLite database
    let storage: Database = Database::new(Path::new(&config.storage.db_path)).context("Error instantiating database")?;

    // Spin up Rest Server
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
    let sse_receiver = async {
        while let Some(evt) = sse_data_receiver.recv().await {
            let cloned_evt = evt.clone();
            let sse_data = serde_json::from_str(&cloned_evt.data).context("Error parsing SSE data")?;
            event_stream_server.broadcast(sse_data);

            let event = serde_json::from_str(&evt.data).context("Error parsing SSE data")?;

            match event {
                SseData::ApiVersion(version) => {
                    info!("API Version: {:?}", version.to_string());
                }
                SseData::BlockAdded {block, block_hash} => {
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

                    // If the block contains deploys then we can update the Deploys table to reflect that they have been included in the block.
                    let deploy_hashes = block.deploy_hashes().to_owned();
                    if !deploy_hashes.is_empty() {
                        let res = storage.save_or_update_deploy(DeployAtState::Added((deploy_hashes, block_hash)))
                            .await;

                        if res.is_err() {
                            warn!("Error updating added deploys: {}", res.err().unwrap());
                        }
                    }
                }
                SseData::DeployAccepted {deploy} => {
                    let deploy_hash = hex::encode(deploy.id().inner());
                    let timestamp = deploy.header().timestamp();
                    info!(
                        message = "Deploy Accepted:",
                        hash = deploy_hash.as_str()
                    );
                    let res = storage.save_or_update_deploy(DeployAtState::Accepted((deploy.id().to_owned(), timestamp)))
                        .await;

                    if res.is_err() {
                        warn!("Error saving deploy: {}", res.err().unwrap());
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
        Result::<_, Error>::Ok(())
    };

    tokio::select! {
        _ = sse_receiver => {
            info!("Consumer process closed - there was likely an error processing the last event")
        }

        _ = rest_server_handle => {
            info!("REST server closed")
        }
    };

    Ok(())
}

fn send_events_discarding_first(event_source: EventSource, sender: UnboundedSender<sse_client::Event>) {
    // This local var for `receiver` needs to be there as calling .receiver() on the EventSource more than
    // once will fail due to the resources being exhausted/dropped on the first call.
    let receiver = event_source.receiver();
    let _ = receiver.iter().next();
    for event in receiver.iter() {
        // warn!("{:?}", event);
        let _ = sender.send(event);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use tokio::sync::oneshot;
    use crate::testing::test_node::start_test_node;

    use super::{read_config, run};

    #[tokio::test(flavor="multi_thread", worker_threads=2)]
    async fn should_consume_node_event_stream() {
        let config = read_config("config_test.toml").expect("Error parsing test config");

        let test_node_sse_port = config.connection.node.local.sse_port;

        let (node_shutdown_tx, node_shutdown_rx) = oneshot::channel();

        tokio::spawn(start_test_node(test_node_sse_port, node_shutdown_rx));

        // Allows server to boot up
        // todo this method is brittle should really have a concrete and dynamic method for determining liveness of the server
        tokio::time::sleep(Duration::from_secs(1)).await;

        let run_result = run(config).await;
        assert!(run_result.is_ok());
        let _ = node_shutdown_tx.send(());
    }
}