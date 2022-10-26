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
mod testing;
mod types;
mod utils;

use std::path::{Path, PathBuf};

use casper_event_listener::EventListener;
use casper_event_types::SseData;

use anyhow::{Context, Error};
use futures::future::join_all;
use hex_fmt::HexFmt;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
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

const CONFIG_PATH: &str = "config.toml";

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

    // Adds space under setup logs before stream starts for readability
    println!("\n\n");

    // This channel allows SseData to be sent from multiple connected nodes to the single EventStreamServer.
    let (sse_data_sender, mut sse_data_receiver) = unbounded_channel();

    // Task to manage incoming events from all three filters
    let listening_task_handle = async move {
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
    };

    tokio::spawn(async move {
        while let Some(sse_data) = sse_data_receiver.recv().await {
            event_stream_server.broadcast(sse_data);
        }
    });

    tokio::select! {
        result = rest_server_handle => { warn!(?result, "REST server shutting down") }
        result = listening_task_handle => { warn!(?result, "SSE processing ended") }
    }

    Ok(())
}

async fn sse_processor(
    sse_event_listener: EventListener,
    sse_data_sender: UnboundedSender<SseData>,
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
                        let _ = sse_data_sender.send(SseData::BlockAdded { block, block_hash });
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
                    Ok(_) => {
                        let _ = sse_data_sender.send(SseData::DeployAccepted { deploy });
                    }
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
                    Ok(_) => {
                        let _ = sse_data_sender.send(SseData::DeployExpired { deploy_hash });
                    }
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
                let step = Step::new(era_id, execution_effect.clone());
                if enable_event_logging {
                    info!("Step at era: {}", era_id.value());
                }
                let res = sqlite_database
                    .save_step(step, sse_event.id.unwrap(), sse_event.source)
                    .await;

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
            }
            SseData::Shutdown => {
                warn!("Node ({}) is unavailable", sse_event.source);
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
    fn should_parse_config_toml_files() {
        read_config("../config.toml").expect("Error parsing config.toml");
        read_config("config_test.toml").expect("Error parsing config_test.toml");
        read_config("config_perf_test.toml").expect("Error parsing config_perf_test.toml");
    }
}
