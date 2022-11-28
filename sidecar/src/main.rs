extern crate core;

mod event_stream_server;
#[cfg(test)]
mod integration_tests;
#[cfg(test)]
mod performance_tests;
mod rest_server;
mod sql;
mod sqlite_database;
#[cfg(test)]
pub(crate) mod testing;
mod types;
mod utils;

use std::path::{Path, PathBuf};

use anyhow::{Context, Error};
use futures::future::join_all;
use hex_fmt::HexFmt;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::{debug, info, trace, warn};

use casper_event_listener::{EventListener, SseEvent};
use casper_event_types::SseData;

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

const CONFIG_PATH: &str = "EXAMPLE_CONFIG.toml";

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
    let mut event_listeners = Vec::with_capacity(config.connection.node_connections.len());

    for connection in &config.connection.node_connections {
        let bind_address = format!("{}:{}", connection.ip_address, connection.sse_port);
        let event_listener = EventListener::new(
            bind_address,
            connection.max_retries,
            connection.delay_between_retries_in_seconds,
            connection.allow_partial_connection,
            connection.filter_priority.clone(),
        )
        .await?;
        event_listeners.push(event_listener);
    }

    let api_versions_match = event_listeners
        .windows(2)
        .all(|w| w[0].api_version == w[1].api_version);

    if !api_versions_match {
        return Err(Error::msg("Connected nodes have mismatched API versions"));
    }

    let api_version = event_listeners[0].api_version;

    let path_to_database_dir = Path::new(&config.storage.storage_path);

    // Creates and initialises Sqlite database
    let sqlite_database =
        SqliteDatabase::new(path_to_database_dir, config.storage.sqlite_config.clone())
            .await
            .context("Error instantiating database")?;

    // Prepare the REST server task - this will be executed later
    let rest_server_handle = tokio::spawn(start_rest_server(
        config.rest_server.clone(),
        sqlite_database.clone(),
    ));

    // Adds space under setup logs before stream starts for readability
    println!();

    // This channel allows SseData to be sent from multiple connected nodes to the single EventStreamServer.
    let (sse_data_sender, mut sse_data_receiver) = unbounded_channel();

    // Create new instance for the Sidecar's Event Stream Server
    let mut event_stream_server = EventStreamServer::new(
        SseConfig::new(
            config.event_stream_server.port,
            Some(config.event_stream_server.event_stream_buffer_length),
            Some(config.event_stream_server.max_concurrent_subscribers),
        ),
        PathBuf::from(&config.storage.storage_path),
        api_version,
    )
    .context("Error starting EventStreamServer")?;

    // Task to manage incoming events from all three filters
    let listening_task_handle = tokio::spawn(async move {
        let mut join_handles = Vec::with_capacity(event_listeners.len());

        let connection_configs = config.connection.node_connections.clone();

        for (event_listener, connection_config) in
            event_listeners.into_iter().zip(connection_configs)
        {
            let join_handle = tokio::spawn(sse_processor(
                event_listener,
                sse_data_sender.clone(),
                sqlite_database.clone(),
                connection_config.enable_logging,
            ));

            join_handles.push(join_handle);
        }

        let _ = join_all(join_handles).await;

        Err::<(), Error>(Error::msg("Connected node(s) are unavailable"))
    });

    let event_broadcasting_handle = tokio::spawn(async move {
        while let Some(sse_data) = sse_data_receiver.recv().await {
            event_stream_server.broadcast(sse_data);
        }
        Err::<(), Error>(Error::msg("Event broadcasting finished"))
    });

    tokio::try_join!(
        flatten_handle(event_broadcasting_handle),
        flatten_handle(rest_server_handle),
        flatten_handle(listening_task_handle)
    )
    .map(|_| Ok(()))?
}

async fn flatten_handle<T>(handle: JoinHandle<Result<T, Error>>) -> Result<T, Error> {
    match handle.await {
        Ok(Ok(result)) => Ok(result),
        Ok(Err(err)) => Err(err),
        Err(join_err) => Err(Error::from(join_err)),
    }
}

async fn sse_processor(
    sse_event_listener: EventListener,
    sse_data_sender: UnboundedSender<SseData>,
    sqlite_database: SqliteDatabase,
    enable_event_logging: bool,
) {
    let mut sse_data_stream = sse_event_listener
        .consume_combine_streams()
        .await
        .unwrap_or_else(|_| {
            let (tx, rx) = unbounded_channel::<SseEvent>();
            let _ = tx.send(SseEvent {
                source: "".to_string(),
                id: None,
                data: SseData::Shutdown,
            });
            rx
        });

    while let Some(sse_event) = sse_data_stream.recv().await {
        match sse_event.data {
            SseData::ApiVersion(version) => {
                if enable_event_logging {
                    info!(%version, "API Version");
                }
            }
            SseData::BlockAdded { block, block_hash } => {
                if enable_event_logging {
                    let hex_block_hash = HexFmt(block_hash.inner());
                    info!("Block Added: {:18}", hex_block_hash);
                    debug!("Block Added: {}", hex_block_hash);
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
                        let _ = sse_data_sender.send(SseData::BlockAdded { block, block_hash });
                    }
                    Err(DatabaseWriteError::UniqueConstraint(uc_err)) => {
                        debug!(
                            "Already received BlockAdded ({}), logged in event_log",
                            HexFmt(block_hash.inner())
                        );
                        trace!(?uc_err);
                    }
                    Err(other_err) => warn!(?other_err, "Unexpected error saving BlockAdded"),
                }
            }
            SseData::DeployAccepted { deploy } => {
                if enable_event_logging {
                    let hex_deploy_hash = HexFmt(deploy.id().inner());
                    info!("Deploy Accepted: {:18}", hex_deploy_hash);
                    debug!("Deploy Accepted: {}", hex_deploy_hash);
                }
                let deploy_accepted = DeployAccepted::new(deploy.clone());
                let res = sqlite_database
                    .save_deploy_accepted(deploy_accepted, sse_event.id.unwrap(), sse_event.source)
                    .await;

                match res {
                    Ok(_) => {
                        let _ = sse_data_sender.send(SseData::DeployAccepted { deploy });
                    }
                    Err(DatabaseWriteError::UniqueConstraint(uc_err)) => {
                        debug!(
                            "Already received DeployAccepted ({}), logged in event_log",
                            HexFmt(deploy.id().inner())
                        );
                        trace!(?uc_err);
                    }
                    Err(other_err) => warn!(?other_err, "Unexpected error saving DeployAccepted"),
                }
            }
            SseData::DeployExpired { deploy_hash } => {
                if enable_event_logging {
                    let hex_deploy_hash = HexFmt(deploy_hash.inner());
                    info!("Deploy Expired: {:18}", hex_deploy_hash);
                    debug!("Deploy Expired: {}", hex_deploy_hash);
                }
                let res = sqlite_database
                    .save_deploy_expired(
                        DeployExpired::new(deploy_hash),
                        sse_event.id.unwrap(),
                        sse_event.source,
                    )
                    .await;

                match res {
                    Ok(_) => {
                        let _ = sse_data_sender.send(SseData::DeployExpired { deploy_hash });
                    }
                    Err(DatabaseWriteError::UniqueConstraint(uc_err)) => {
                        debug!(
                            "Already received DeployExpired ({}), logged in event_log",
                            HexFmt(deploy_hash.inner())
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
                if enable_event_logging {
                    let hex_deploy_hash = HexFmt(deploy_hash.inner());
                    info!("Deploy Processed: {:18}", hex_deploy_hash);
                    debug!("Deploy Processed: {}", hex_deploy_hash);
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
                    Ok(_) => {
                        let _ = sse_data_sender.send(SseData::DeployProcessed {
                            deploy_hash,
                            account,
                            timestamp,
                            ttl,
                            dependencies,
                            block_hash,
                            execution_result,
                        });
                    }
                    Err(DatabaseWriteError::UniqueConstraint(uc_err)) => {
                        debug!(
                            "Already received DeployProcessed ({}), logged in event_log",
                            HexFmt(deploy_hash.inner())
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
                    Ok(_) => {
                        let _ = sse_data_sender.send(SseData::Fault {
                            era_id,
                            timestamp,
                            public_key,
                        });
                    }
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
                    Ok(_) => {
                        let _ = sse_data_sender.send(SseData::FinalitySignature(fs));
                    }
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
                println!(
                    "Sidecar started handling Step at: {}\n",
                    chrono::Utc::now().time()
                );
                let sidecar_received_step_timestamp = Instant::now();
                let before = Instant::now();
                let step = Step::new(era_id, execution_effect.clone());
                println!(
                    "Took {}ms to clone Step",
                    (Instant::now() - before).as_millis()
                );
                if enable_event_logging {
                    info!("Step at era: {}", era_id.value());
                }
                let before = Instant::now();
                let res = sqlite_database
                    .save_step(step, sse_event.id.unwrap(), sse_event.source)
                    .await;
                println!(
                    "Took {}ms to save to DB",
                    (Instant::now() - before).as_millis()
                );

                match res {
                    Ok(_) => {
                        let _ = sse_data_sender.send(SseData::Step {
                            era_id,
                            execution_effect,
                        });
                    }
                    Err(DatabaseWriteError::UniqueConstraint(uc_err)) => {
                        debug!(
                            "Already received Step ({}), logged in event_log",
                            era_id.value()
                        );
                        trace!(?uc_err);
                    }
                    Err(other_err) => warn!(?other_err, "Unexpected error saving Step"),
                }
                println!(
                    "Took {}ms to handle step",
                    (Instant::now() - sidecar_received_step_timestamp).as_millis()
                );
                println!(
                    "\nSidecar finished handling Step at: {}",
                    chrono::Utc::now().time()
                );
            }
            SseData::Shutdown => {
                warn!("Node ({}) is unavailable", sse_event.source);
                break;
            }
        }
    }
}

#[test]
fn should_parse_config_toml() {
    read_config("../EXAMPLE_CONFIG.toml").expect("Error parsing EXAMPLE_CONFIG.toml");
}
