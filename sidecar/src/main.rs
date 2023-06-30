extern crate core;

mod event_stream_server;
#[cfg(test)]
mod integration_tests;
#[cfg(test)]
mod integration_tests_version_switch;
mod migration_manager;
#[cfg(test)]
mod performance_tests;
mod rest_server;
mod sql;
mod sqlite_database;
#[cfg(test)]
pub(crate) mod testing;
mod types;
mod utils;

use std::{
    net::IpAddr,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use crate::{
    event_stream_server::{Config as SseConfig, EventStreamServer},
    rest_server::run_server as start_rest_server,
    sqlite_database::SqliteDatabase,
    types::{
        config::{read_config, Config},
        database::{DatabaseWriteError, DatabaseWriter},
        sse_events::*,
    },
};
use anyhow::{Context, Error};
use casper_event_listener::{
    EventListener, EventListenerBuilder, NodeConnectionInterface, SseEvent,
};
use casper_event_types::{
    metrics::{self, register_metrics},
    sse_data::SseData,
    Filter,
};
use casper_types::ProtocolVersion;
use clap::Parser;
use futures::future::join_all;
use hex_fmt::HexFmt;
#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;
use tokio::{
    sync::{
        mpsc::{channel as mpsc_channel, Receiver, Sender},
        Mutex,
    },
    task::JoinHandle,
    time::sleep,
};
use tracing::{debug, error, info, trace, warn};
use types::database::DatabaseReader;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct CmdLineArgs {
    /// Path to the TOML-formatted config file
    #[arg(short, long, value_name = "FILE")]
    path_to_config: String,
}

const DEFAULT_CHANNEL_SIZE: usize = 1000;

#[tokio::main]
async fn main() -> Result<(), Error> {
    // Install global collector for tracing
    tracing_subscriber::fmt::init();

    let args = CmdLineArgs::parse();

    let path_to_config = args.path_to_config;

    let config: Config = read_config(&path_to_config).context("Error constructing config")?;

    info!("Configuration loaded");
    register_metrics();
    run(config).await
}

async fn run(config: Config) -> Result<(), Error> {
    // This is a temporary constraint whilst we iron out the details of handling connection to multiple nodes
    // Beofre we remove it we need to figure out how we want to handle api versioning for multiple nodes
    if config.connections.len() > 1 {
        return Err(Error::msg(
            "Unable to run with multiple connections specified in config",
        ));
    }

    if config
        .connections
        .iter()
        .any(|connection| connection.max_attempts < 1)
    {
        return Err(Error::msg(
            "Unable to run: max_attempts setting must be above 0 for the sidecar to attempt connection"
        ));
    }
    let mut event_listeners = Vec::with_capacity(config.connections.len());

    let mut sse_data_receivers = Vec::new();
    let (api_version_tx, mut api_version_rx) =
        mpsc_channel::<Result<ProtocolVersion, Error>>(config.connections.len() + 10);

    for connection in &config.connections {
        let (inbound_sse_data_sender, inbound_sse_data_receiver) =
            mpsc_channel(config.inbound_channel_size.unwrap_or(DEFAULT_CHANNEL_SIZE));

        sse_data_receivers.push(inbound_sse_data_receiver);

        let node_interface = NodeConnectionInterface {
            ip_address: IpAddr::from_str(&connection.ip_address)?,
            sse_port: connection.sse_port,
            rest_port: connection.rest_port,
        };

        let event_listener = EventListenerBuilder {
            node: node_interface,
            max_connection_attempts: connection.max_attempts,
            delay_between_attempts: Duration::from_secs(
                connection.delay_between_retries_in_seconds as u64,
            ),
            allow_partial_connection: connection.allow_partial_connection,
            sse_event_sender: inbound_sse_data_sender,
            connection_timeout: Duration::from_secs(
                connection.connection_timeout_in_seconds.unwrap_or(5) as u64,
            ),
            sleep_between_keep_alive_checks: Duration::from_secs(
                connection
                    .sleep_between_keep_alive_checks_in_seconds
                    .unwrap_or(60) as u64,
            ),
            no_message_timeout: Duration::from_secs(
                connection.no_message_timeout_in_seconds.unwrap_or(120) as u64,
            ),
        }
        .build();
        event_listeners.push(event_listener);
    }

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

    // This channel allows SseData to be sent from multiple connected nodes to the single EventStreamServer.
    let (outbound_sse_data_sender, mut outbound_sse_data_receiver) =
        mpsc_channel(config.outbound_channel_size.unwrap_or(DEFAULT_CHANNEL_SIZE));

    let connection_configs = config.connections.clone();
    let is_empty_database = check_if_database_is_empty(sqlite_database.clone()).await?;

    // Task to manage incoming events from all three filters
    let listening_task_handle = tokio::spawn(async move {
        let mut join_handles = Vec::with_capacity(event_listeners.len());

        for ((event_listener, connection_config), sse_data_receiver) in event_listeners
            .into_iter()
            .zip(connection_configs)
            .zip(sse_data_receivers)
        {
            let join_handle = tokio::spawn(sse_processor(
                event_listener,
                api_version_tx.clone(),
                sse_data_receiver,
                outbound_sse_data_sender.clone(),
                sqlite_database.clone(),
                connection_config.enable_logging,
                is_empty_database,
            ));

            join_handles.push(join_handle);
        }

        // The original sender needs to be dropped here as it's existence prevents the event broadcasting logic to proceed
        drop(api_version_tx);

        let _ = join_all(join_handles).await;
        //Send Shutdown to the sidecar sse endpoint
        let _ = outbound_sse_data_sender
            .send((SseData::Shutdown, None, None))
            .await;
        // Below sleep is a workaround to allow the above Shutdown to propagate.
        // If we don't do this there is a race condition between handling of the message and dropping of the outbound server
        // which happens when we leave this function and the `tokio::try_join!` exits due to this. This race condition causes 9 of 10
        // tries to not propagate the Shutdown (ususally drop happens faster than message propagation to outbound).
        // Fixing this race condition would require rewriting a lot of code. AFAICT the only drawback to this workaround is that the
        // rest server and the sse server will exit 200ms later than it would without it.
        sleep(Duration::from_millis(200)).await;
        Err::<(), Error>(Error::msg("Connected node(s) are unavailable"))
    });

    //TODO currently api version handling DOES NOT account for possibility of multiple connections
    let event_broadcasting_handle = tokio::spawn(async move {
        if let Some(api_fetch_res) = api_version_rx.recv().await {
            match api_fetch_res {
                Ok(_) => {
                    // We got a protocol version, event listener can start
                }
                Err(err) => {
                    error!("Error fetching API version from connected node(s): {err}");
                    return Err(err);
                }
            }
        }

        // Create new instance for the Sidecar's Event Stream Server
        let mut event_stream_server = EventStreamServer::new(
            SseConfig::new(
                config.event_stream_server.port,
                Some(config.event_stream_server.event_stream_buffer_length),
                Some(config.event_stream_server.max_concurrent_subscribers),
            ),
            PathBuf::from(&config.storage.storage_path),
        )
        .context("Error starting EventStreamServer")?;

        while let Some((sse_data, inbound_filter, maybe_json_data)) =
            outbound_sse_data_receiver.recv().await
        {
            event_stream_server.broadcast(sse_data, inbound_filter, maybe_json_data);
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

/// Function to handle single event in the sse_processor.
/// Returns false if the handling indicated that no other messages should be processed.
/// Returns true otherwise.
async fn handle_single_event(
    sse_event: SseEvent,
    sqlite_database: SqliteDatabase,
    enable_event_logging: bool,
    outbound_sse_data_sender: Sender<(SseData, Option<Filter>, Option<String>)>,
    last_reported_protocol_version: Arc<Mutex<Option<ProtocolVersion>>>,
) {
    match sse_event.data {
        SseData::ApiVersion(_) | SseData::Shutdown => {
            //don't do debug counting for ApiVersion since we don't store it
        }
        _ => {
            count_internal_event("main_inbound_sse_data", "event_received_start");
        }
    }
    match sse_event.data {
        SseData::SidecarVersion(_) => {
            //Do nothing -> the inbound shouldn't produce this endpoint, it can be only produced by sidecar to the outbound
        }
        SseData::ApiVersion(version) => {
            let mut guard = last_reported_protocol_version.lock().await;
            match &mut *guard {
                Some(old_version) if old_version.eq(&&version) => {
                    //do nothing
                }
                None | Some(_) => {
                    *guard = Some(version);
                    if let Err(error) = outbound_sse_data_sender
                        .send((
                            SseData::ApiVersion(version),
                            Some(sse_event.inbound_filter),
                            None,
                        ))
                        .await
                    {
                        debug!(
                            "Error when sending to outbound_sse_data_sender. Error: {}",
                            error
                        );
                    }
                }
            }
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
            count_internal_event("main_inbound_sse_data", "db_save_start");
            let res = sqlite_database
                .save_block_added(
                    BlockAdded::new(block_hash, block.clone()),
                    sse_event.id,
                    sse_event.source.to_string(),
                )
                .await;

            match res {
                Ok(_) => {
                    count_internal_event("main_inbound_sse_data", "db_save_end");
                    count_internal_event("main_inbound_sse_data", "outbound_sse_data_send_start");
                    if let Err(error) = outbound_sse_data_sender
                        .send((
                            SseData::BlockAdded { block, block_hash },
                            Some(sse_event.inbound_filter),
                            sse_event.json_data,
                        ))
                        .await
                    {
                        count_internal_event("main_inbound_sse_data", "outbound_sse_data_send_end");
                        debug!(
                            "Error when sending to outbound_sse_data_sender. Error: {}",
                            error
                        );
                    } else {
                        count_internal_event("main_inbound_sse_data", "outbound_sse_data_send_end");
                    }
                }
                Err(DatabaseWriteError::UniqueConstraint(uc_err)) => {
                    count_internal_event("main_inbound_sse_data", "db_save_end");
                    debug!(
                        "Already received BlockAdded ({}), logged in event_log",
                        HexFmt(block_hash.inner())
                    );
                    trace!(?uc_err);
                }
                Err(other_err) => {
                    count_internal_event("main_inbound_sse_data", "db_save_end");
                    count_error("db_save_error_block_added");
                    warn!(?other_err, "Unexpected error saving BlockAdded");
                }
            }
            count_internal_event("main_inbound_sse_data", "event_received_end");
        }
        SseData::DeployAccepted { deploy } => {
            if enable_event_logging {
                let hex_deploy_hash = HexFmt(deploy.hash().inner());
                info!("Deploy Accepted: {:18}", hex_deploy_hash);
                debug!("Deploy Accepted: {}", hex_deploy_hash);
            }
            let deploy_accepted = DeployAccepted::new(deploy.clone());
            count_internal_event("main_inbound_sse_data", "db_save_start");
            let res = sqlite_database
                .save_deploy_accepted(deploy_accepted, sse_event.id, sse_event.source.to_string())
                .await;

            match res {
                Ok(_) => {
                    count_internal_event("main_inbound_sse_data", "db_save_end");
                    count_internal_event("main_inbound_sse_data", "outbound_sse_data_send_start");
                    if let Err(error) = outbound_sse_data_sender
                        .send((
                            SseData::DeployAccepted { deploy },
                            Some(sse_event.inbound_filter),
                            sse_event.json_data,
                        ))
                        .await
                    {
                        count_internal_event("main_inbound_sse_data", "outbound_sse_data_send_end");
                        debug!(
                            "Error when sending to outbound_sse_data_sender. Error: {}",
                            error
                        );
                    } else {
                        count_internal_event("main_inbound_sse_data", "outbound_sse_data_send_end");
                    }
                }
                Err(DatabaseWriteError::UniqueConstraint(uc_err)) => {
                    count_internal_event("main_inbound_sse_data", "db_save_end");
                    debug!(
                        "Already received DeployAccepted ({}), logged in event_log",
                        HexFmt(deploy.hash().inner())
                    );
                    trace!(?uc_err);
                }
                Err(other_err) => {
                    count_internal_event("main_inbound_sse_data", "db_save_end");
                    count_error("db_save_error_deploy_accepted");
                    warn!(?other_err, "Unexpected error saving DeployAccepted");
                }
            }
            count_internal_event("main_inbound_sse_data", "event_received_end");
        }
        SseData::DeployExpired { deploy_hash } => {
            if enable_event_logging {
                let hex_deploy_hash = HexFmt(deploy_hash.inner());
                info!("Deploy Expired: {:18}", hex_deploy_hash);
                debug!("Deploy Expired: {}", hex_deploy_hash);
            }
            count_internal_event("main_inbound_sse_data", "db_save_start");
            let res = sqlite_database
                .save_deploy_expired(
                    DeployExpired::new(deploy_hash),
                    sse_event.id,
                    sse_event.source.to_string(),
                )
                .await;

            match res {
                Ok(_) => {
                    count_internal_event("main_inbound_sse_data", "db_save_end");
                    count_internal_event("main_inbound_sse_data", "outbound_sse_data_send_start");
                    if let Err(error) = outbound_sse_data_sender
                        .send((
                            SseData::DeployExpired { deploy_hash },
                            Some(sse_event.inbound_filter),
                            sse_event.json_data,
                        ))
                        .await
                    {
                        count_internal_event("main_inbound_sse_data", "outbound_sse_data_send_end");
                        debug!(
                            "Error when sending to outbound_sse_data_sender. Error: {}",
                            error
                        );
                    } else {
                        count_internal_event("main_inbound_sse_data", "outbound_sse_data_send_end");
                    }
                }
                Err(DatabaseWriteError::UniqueConstraint(uc_err)) => {
                    count_internal_event("main_inbound_sse_data", "db_save_end");
                    debug!(
                        "Already received DeployExpired ({}), logged in event_log",
                        HexFmt(deploy_hash.inner())
                    );
                    trace!(?uc_err);
                }
                Err(other_err) => {
                    count_internal_event("main_inbound_sse_data", "db_save_end");
                    count_error("db_save_error_deploy_expired");
                    warn!(?other_err, "Unexpected error saving DeployExpired");
                }
            }
            count_internal_event("main_inbound_sse_data", "event_received_end");
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
            count_internal_event("main_inbound_sse_data", "db_save_start");
            let res = sqlite_database
                .save_deploy_processed(
                    deploy_processed.clone(),
                    sse_event.id,
                    sse_event.source.to_string(),
                )
                .await;

            match res {
                Ok(_) => {
                    count_internal_event("main_inbound_sse_data", "db_save_end");
                    count_internal_event("main_inbound_sse_data", "outbound_sse_data_send_start");
                    if let Err(error) = outbound_sse_data_sender
                        .send((
                            SseData::DeployProcessed {
                                deploy_hash,
                                account,
                                timestamp,
                                ttl,
                                dependencies,
                                block_hash,
                                execution_result,
                            },
                            Some(sse_event.inbound_filter),
                            sse_event.json_data,
                        ))
                        .await
                    {
                        count_internal_event("main_inbound_sse_data", "outbound_sse_data_send_end");
                        debug!(
                            "Error when sending to outbound_sse_data_sender. Error: {}",
                            error
                        );
                    } else {
                        count_internal_event("main_inbound_sse_data", "outbound_sse_data_send_end");
                    }
                }
                Err(DatabaseWriteError::UniqueConstraint(uc_err)) => {
                    count_internal_event("main_inbound_sse_data", "db_save_end");
                    debug!(
                        "Already received DeployProcessed ({}), logged in event_log",
                        HexFmt(deploy_hash.inner())
                    );
                    trace!(?uc_err);
                }
                Err(other_err) => {
                    count_internal_event("main_inbound_sse_data", "db_save_end");
                    count_error("db_save_error_deploy_processed");
                    warn!(?other_err, "Unexpected error saving DeployProcessed");
                }
            }
            count_internal_event("main_inbound_sse_data", "event_received_end");
        }
        SseData::Fault {
            era_id,
            timestamp,
            public_key,
        } => {
            let fault = Fault::new(era_id, public_key.clone(), timestamp);
            warn!(%fault, "Fault reported");
            count_internal_event("main_inbound_sse_data", "db_save_start");
            let res = sqlite_database
                .save_fault(fault.clone(), sse_event.id, sse_event.source.to_string())
                .await;

            match res {
                Ok(_) => {
                    count_internal_event("main_inbound_sse_data", "db_save_end");
                    count_internal_event("main_inbound_sse_data", "outbound_sse_data_send_start");
                    if let Err(error) = outbound_sse_data_sender
                        .send((
                            SseData::Fault {
                                era_id,
                                timestamp,
                                public_key,
                            },
                            Some(sse_event.inbound_filter),
                            sse_event.json_data,
                        ))
                        .await
                    {
                        count_internal_event("main_inbound_sse_data", "outbound_sse_data_send_end");
                        debug!(
                            "Error when sending to outbound_sse_data_sender. Error: {}",
                            error
                        );
                    } else {
                        count_internal_event("main_inbound_sse_data", "outbound_sse_data_send_end");
                    }
                }
                Err(DatabaseWriteError::UniqueConstraint(uc_err)) => {
                    count_internal_event("main_inbound_sse_data", "db_save_end");
                    debug!("Already received Fault ({:#?}), logged in event_log", fault);
                    trace!(?uc_err);
                }
                Err(other_err) => {
                    count_internal_event("main_inbound_sse_data", "db_save_end");
                    count_error("db_save_error_fault");
                    warn!(?other_err, "Unexpected error saving Fault");
                }
            }
            count_internal_event("main_inbound_sse_data", "event_received_end");
        }
        SseData::FinalitySignature(fs) => {
            if enable_event_logging {
                debug!(
                    "Finality Signature: {} for {}",
                    fs.signature(),
                    fs.block_hash()
                );
            }
            let finality_signature = FinalitySignature::new(fs.clone());
            count_internal_event("main_inbound_sse_data", "db_save_start");
            let res = sqlite_database
                .save_finality_signature(
                    finality_signature.clone(),
                    sse_event.id,
                    sse_event.source.to_string(),
                )
                .await;

            match res {
                Ok(_) => {
                    count_internal_event("main_inbound_sse_data", "db_save_end");
                    count_internal_event("main_inbound_sse_data", "outbound_sse_data_send_start");
                    if let Err(error) = outbound_sse_data_sender
                        .send((
                            SseData::FinalitySignature(fs),
                            Some(sse_event.inbound_filter),
                            sse_event.json_data,
                        ))
                        .await
                    {
                        count_internal_event("main_inbound_sse_data", "outbound_sse_data_send_end");
                        debug!(
                            "Error when sending to outbound_sse_data_sender. Error: {}",
                            error
                        );
                    } else {
                        count_internal_event("main_inbound_sse_data", "outbound_sse_data_send_end");
                    }
                }
                Err(DatabaseWriteError::UniqueConstraint(uc_err)) => {
                    count_internal_event("main_inbound_sse_data", "db_save_end");
                    debug!(
                        "Already received FinalitySignature ({}), logged in event_log",
                        fs.signature()
                    );
                    trace!(?uc_err);
                }
                Err(other_err) => {
                    count_internal_event("main_inbound_sse_data", "db_save_end");
                    count_error("db_save_error_finality_signature");
                    warn!(?other_err, "Unexpected error saving FinalitySignature")
                }
            }
            count_internal_event("main_inbound_sse_data", "event_received_end");
        }
        SseData::Step {
            era_id,
            execution_effect,
        } => {
            let step = Step::new(era_id, execution_effect.clone());
            if enable_event_logging {
                info!("Step at era: {}", era_id.value());
            }
            count_internal_event("main_inbound_sse_data", "db_save_start");
            let res = sqlite_database
                .save_step(step, sse_event.id, sse_event.source.to_string())
                .await;

            match res {
                Ok(_) => {
                    count_internal_event("main_inbound_sse_data", "db_save_end");
                    count_internal_event("main_inbound_sse_data", "outbound_sse_data_send_start");
                    if let Err(error) = outbound_sse_data_sender
                        .send((
                            SseData::Step {
                                era_id,
                                execution_effect,
                            },
                            Some(sse_event.inbound_filter),
                            sse_event.json_data,
                        ))
                        .await
                    {
                        count_internal_event("main_inbound_sse_data", "outbound_sse_data_send_end");
                        debug!(
                            "Error when sending to outbound_sse_data_sender. Error: {}",
                            error
                        );
                    } else {
                        count_internal_event("main_inbound_sse_data", "outbound_sse_data_send_end");
                    }
                }
                Err(DatabaseWriteError::UniqueConstraint(uc_err)) => {
                    count_internal_event("main_inbound_sse_data", "db_save_end");
                    debug!(
                        "Already received Step ({}), logged in event_log",
                        era_id.value()
                    );
                    trace!(?uc_err);
                }
                Err(other_err) => {
                    count_internal_event("main_inbound_sse_data", "db_save_end");
                    count_error("db_save_error_step");
                    warn!(?other_err, "Unexpected error saving Step")
                }
            }
            count_internal_event("main_inbound_sse_data", "event_received_end");
        }
        SseData::Shutdown => {
            warn!("Node ({}) is unavailable", sse_event.source.to_string());
            let res = sqlite_database
                .save_shutdown(sse_event.id, sse_event.source.to_string())
                .await;
            match res {
                Ok(_) | Err(DatabaseWriteError::UniqueConstraint(_)) => {
                    // We push to outbound on UniqueConstraint error because in sse_server we match shutdowns to outbounds based on the filter they came from to prevent duplicates.
                    // But that also means that we need to pass through all the Shutdown events so the sse_server can determine to which outbound filters they need to be pushed (we
                    // don't store in DB the information from which filter did shutdown came).
                    if let Err(error) = outbound_sse_data_sender
                        .send((
                            SseData::Shutdown,
                            Some(sse_event.inbound_filter),
                            sse_event.json_data,
                        ))
                        .await
                    {
                        debug!(
                            "Error when sending to outbound_sse_data_sender. Error: {}",
                            error
                        );
                    }
                }
                Err(other_err) => {
                    count_error("db_save_error_shutdown");
                    warn!(?other_err, "Unexpected error saving Shutdown")
                }
            }
        }
    }
}

async fn sse_processor(
    mut sse_event_listener: EventListener,
    api_version_reporter: Sender<Result<ProtocolVersion, Error>>,
    mut inbound_sse_data_receiver: Receiver<SseEvent>,
    outbound_sse_data_sender: Sender<(SseData, Option<Filter>, Option<String>)>,
    sqlite_database: SqliteDatabase,
    enable_event_logging: bool,
    is_empty_database: bool,
) {
    // This task starts the listener pushing events to the sse_data_receiver
    tokio::spawn(async move {
        let _ = sse_event_listener
            .stream_aggregated_events(api_version_reporter, is_empty_database)
            .await;
    });
    let last_reported_api_version: Arc<Mutex<Option<ProtocolVersion>>> = Arc::new(Mutex::new(None));
    while let Some(sse_event) = inbound_sse_data_receiver.recv().await {
        handle_single_event(
            sse_event,
            sqlite_database.clone(),
            enable_event_logging,
            outbound_sse_data_sender.clone(),
            last_reported_api_version.clone(),
        )
        .await
    }
}

async fn check_if_database_is_empty(db: SqliteDatabase) -> Result<bool, Error> {
    db.get_number_of_events()
        .await
        .map(|i| i == 0)
        .map_err(|e| Error::msg(format!("Error when checking if database is empty {:?}", e)))
}

fn count_error(reason: &str) {
    metrics::ERROR_COUNTS
        .with_label_values(&["main", reason])
        .inc();
}

/// This metric is used for debugging of possible issues
/// with sidecar to determine at which step of processing there was a hang.
/// If we determine that this issue was fixed completely this can be removed
/// (the corresponding metric also).
fn count_internal_event(category: &str, reason: &str) {
    metrics::INTERNAL_EVENTS
        .with_label_values(&[category, reason])
        .inc();
}
