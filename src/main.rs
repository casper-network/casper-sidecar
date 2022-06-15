extern crate core;

mod sqlite_db;
mod rest_server;
mod event_stream_server;
pub mod types;

use std::path::{Path, PathBuf};
use anyhow::Error;
use casper_node::types::Block;
use casper_types::AsymmetricType;
use sqlite_db::Database;
use sse_client::EventSource;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tracing::info;
use tracing_subscriber;
use types::structs::Config;
use types::enums::Network;
use rest_server::start_server as start_rest_server;
use event_stream_server::{EventStreamServer, Config as SseConfig};
use crate::event_stream_server::SseData;


pub fn read_config(config_path: &str) -> Result<Config, Error> {
    let toml_content = std::fs::read_to_string(config_path)?;
    Ok(toml::from_str(&toml_content)?)
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    // Install global collector for tracing
    tracing_subscriber::fmt::init();

    let config: Config = read_config("config.toml")?;
    info!("Configuration loaded");

    let node = match config.connection.network {
        Network::Mainnet => config.connection.node.mainnet,
        Network::Testnet => config.connection.node.testnet,
        Network::Local => config.connection.node.local,
    };

    // Set URLs for each of the (from the node) Event Stream Server's filters
    let url_base = format!("http://{ip}:{port}/events", ip = node.ip_address, port = node.sse_port);
    let sse_main = EventSource::new(
        format!("{}/main", url_base).as_str(),
    )?;
    let sse_deploys = EventSource::new(
        format!("{}/deploys", url_base).as_str(),
    )?;
    let sse_sigs = EventSource::new(
        format!("{}/sigs", url_base).as_str(),
    )?;

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
    let first_event_data = serde_json::from_str::<SseData>(&first_event.unwrap().data)?;

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
    let storage: Database = Database::new(Path::new(&config.storage.db_path))?;

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
    ).unwrap_or_else(|err| {
        panic!("{:?}", err);
    });

    // Adds space under setup logs before stream starts
    println!("\n\n");

    // Task to manage incoming events from all three filters
    let sse_receiver = async {
        while let Some(evt) = sse_data_receiver.recv().await {
            let cloned_evt = evt.clone();
            let sse_data = serde_json::from_str(&cloned_evt.data).unwrap();
            event_stream_server.broadcast(sse_data);

            let event = serde_json::from_str(&evt.data).unwrap();

            match event {
                SseData::ApiVersion(version) => {
                    info!("API Version: {:?}", version.to_string());
                }
                SseData::BlockAdded {block, block_hash} => {
                    let block = Block::from(*block);
                    info!(
                        message = "Saving block:",
                        hash = block_hash.to_string().as_str(),
                        height = block.height()
                    );
                    storage.save_block(&block).await.unwrap();
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
                        message = "Saving deploy:",
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
                    info!("Finality signature, {}", fs);
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
        _ = sse_receiver => {
            info!("Receiver closed")
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