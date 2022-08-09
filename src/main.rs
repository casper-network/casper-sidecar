extern crate core;

mod sqlite_db;
mod rest_server;
mod event_stream_server;
pub mod types;

use std::path::{Path, PathBuf};
use anyhow::{Context, Error};
use casper_node::types::Block;
use casper_types::AsymmetricType;
use sqlite_db::Database;
use sse_client::EventSource;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tracing::{debug, info};
use tracing_subscriber;
use types::structs::Config;
use types::enums::Network;
use rest_server::start_server as start_rest_server;
use event_stream_server::{EventStreamServer, Config as SseConfig};
use crate::event_stream_server::SseData;


pub fn read_config(config_path: &str) -> Result<Config, Error> {
    let toml_content = std::fs::read_to_string(config_path).context("Error reading config file contents")?;
    Ok(toml::from_str(&toml_content).context("Error parsing config into TOML format")?)
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    // Install global collector for tracing
    tracing_subscriber::fmt::init();

    let config: Config = read_config("config.toml").context("Error constructing config")?;
    info!("Configuration loaded");

    let node_config = match config.connection.network {
        Network::Mainnet => config.connection.node.mainnet,
        Network::Testnet => config.connection.node.testnet,
        Network::Local => config.connection.node.local,
    };

    // Set URLs for each of the (from the node) Event Stream Server's filters
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
            Err(serde_err) => return Err(Error::from(serde_err).context("First event was not of expected format"))
        }
    };

    info!(
        message = "Connected to SSE",
        network = config.connection.network.as_str(),
        api_version = api_version.to_string().as_str(),
        node_ip_address = node_config.ip_address.as_str()
    );

    // Loop over incoming events and send them along the channel
    // The main receiver loop doesn't need to discard the first event because it has already been parsed out above.
    let main_events_sending_task = async move {
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
    let storage: Database = Database::new(Path::new(&config.storage.db_path)).context("Error instantiating database")?;

    // Prepare the REST server task - this will be executed later
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
            let sse_data = serde_json::from_str::<SseData>(&evt.data).context("Error parsing SSE data")?;
            event_stream_server.broadcast(sse_data.clone());

            match sse_data {
                SseData::ApiVersion(version) => {
                    info!("API Version: {:?}", version.to_string());
                }
                SseData::BlockAdded {block, block_hash} => {
                    let block = Block::from(*block);
                    info!(
                        message = "Block Added:",
                        hash = block_hash.to_string().as_str(),
                        height = block.height()
                    );
                    storage.save_block(&block).await.context("Error saving block data")?;
                }
                SseData::DeployAccepted {deploy} => {
                    info!(
                        "Deploy Accepted: {}",
                        deploy
                    );
                }
                SseData::DeployExpired {deploy_hash} => {
                    info!(
                        message = "Deploy expired:",
                        hash = hex::encode(deploy_hash.inner()).as_str()
                    );
                }
                SseData::DeployProcessed {deploy_hash, ..} => {
                    // let deploy_processed = DeployProcessed {
                    //     account,
                    //     block_hash,
                    //     dependencies,
                    //     deploy_hash,
                    //     execution_result,
                    //     ttl,
                    //     timestamp
                    // };
                    info!(
                        message = "Deploy Processed:",
                        hash = hex::encode(deploy_hash.inner()).as_str()
                    );
                    // storage.save_deploy(&deploy_processed).await?;
                }
                SseData::Fault {era_id, timestamp, public_key} => {
                    info!(
                        "\n\tFault reported!\n\tEra: {}\n\tPublic Key: {}\n\tTimestamp: {}",
                        era_id,
                        public_key.to_hex(),
                        timestamp
                    );
                    // storage.save_fault(&fault).await?;
                }
                SseData::FinalitySignature(fs) => {
                    debug!("Finality signature, {}", fs.signature);
                }
                SseData::Step {era_id, ..} => {
                    info!("\n\tStep reached for Era: {}", era_id);
                    // storage.save_step(&step).await?;
                }
                SseData::Shutdown => {
                    info!("Node is shutting down!");
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

fn send_events_discarding_first(event_source: EventSource, sender: UnboundedSender<sse_client::Event>) {
    // This local var for `receiver` needs to be there as calling .receiver() on the EventSource more than
    // once will fail due to the resources being exhausted/dropped on the first call.
    let receiver = event_source.receiver();
    let _ = receiver.iter().next();
    for event in receiver.iter() {
        let _ = sender.send(event);
    }
}