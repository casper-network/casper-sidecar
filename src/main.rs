extern crate core;

mod sqlite_db;
mod rest_server;
mod event_stream_server;
pub mod types;

use std::path::{Path, PathBuf};
use anyhow::Error;
use casper_node::types::Block;
use casper_types::{AsymmetricType, ProtocolVersion};
use sqlite_db::Database;
use sse_client::EventSource;
use tracing::{warn, info};
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

    let sse = EventSource::new(
        format!(
            "http://{ip}:{port}/events/{filter}",
            ip = node.ip_address,
            port = node.sse_port,
            filter = config.connection.sse_filter
        )
        .as_str(),
    )?;
    info!(
        message = "Connected to SSE",
        network = config.connection.network.as_str(),
        node_ip_address = node.ip_address.as_str()
    );

    // Create DB and create tables
    let storage: Database = Database::new(Path::new(&config.storage.db_path))?;

    let db_path = PathBuf::from(config.storage.db_path);
    let rest_server_handle = tokio::task::spawn(
        start_rest_server(
            db_path,
            config.rest_server.port,
        )
    );

    let mut event_stream_server = EventStreamServer::new(
        SseConfig::new(),
        PathBuf::from(config.storage.sse_cache),
        ProtocolVersion::from_parts(1,4,6)
    ).unwrap_or_else(|err| {
        panic!(err);
    });

    let sse_receiver = async {
        for (_index, evt) in sse.receiver().iter().enumerate() {

            let cloned_evt = evt.clone();
            let sse_data = serde_json::from_str(&cloned_evt.data)?;
            event_stream_server.broadcast(sse_data);

            let event = serde_json::from_str(&evt.data)?;

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
                    storage.save_block(&block).await?;
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
