#![deny(clippy::complexity)]
#![deny(clippy::cognitive_complexity)]
#![deny(clippy::too_many_lines)]

extern crate core;
mod admin_server;
mod api_version_manager;
mod database;
mod event_stream_server;
pub mod rest_server;
mod sql;
#[cfg(test)]
pub(crate) mod testing;
#[cfg(test)]
pub(crate) mod tests;
mod types;
mod utils;

use std::collections::HashMap;
use std::process::ExitCode;
use std::{net::IpAddr, path::PathBuf, str::FromStr, time::Duration};

use crate::{
    event_stream_server::{Config as SseConfig, EventStreamServer},
    rest_server::run_server as start_rest_server,
    types::{
        database::{DatabaseWriteError, DatabaseWriter},
        sse_events::*,
    },
};
use anyhow::{Context, Error};
use api_version_manager::{ApiVersionManager, GuardedApiVersionManager};
use casper_event_listener::{
    EventListener, EventListenerBuilder, NodeConnectionInterface, SseEvent,
};
use casper_event_types::{sse_data::SseData, Filter};
use casper_types::ProtocolVersion;
use futures::future::join_all;
use hex_fmt::HexFmt;
use metrics::observe_error;
use metrics::sse::observe_contract_messages;
use tokio::{
    sync::mpsc::{channel as mpsc_channel, Receiver, Sender},
    task::JoinHandle,
    time::sleep,
};
use tracing::{debug, error, info, trace, warn};
use types::database::DatabaseReader;
#[cfg(feature = "additional-metrics")]
use utils::start_metrics_thread;

pub use admin_server::run_server as run_admin_server;
pub use database::DatabaseConfigError;
pub use types::{
    config::{
        AdminApiServerConfig, Connection, RestApiServerConfig, SseEventServerConfig, StorageConfig,
        StorageConfigSerdeTarget,
    },
    database::{Database, LazyDatabaseWrapper},
};

const DEFAULT_CHANNEL_SIZE: usize = 1000;

pub async fn run(
    config: SseEventServerConfig,
    database: Database,
    storage_path: String,
) -> Result<ExitCode, Error> {
    validate_config(&config)?;
    let (event_listeners, sse_data_receivers) = build_event_listeners(&config)?;
    // This channel allows SseData to be sent from multiple connected nodes to the single EventStreamServer.
    let (outbound_sse_data_sender, outbound_sse_data_receiver) =
        mpsc_channel(config.outbound_channel_size.unwrap_or(DEFAULT_CHANNEL_SIZE));
    let connection_configs = config.connections.clone();

    // Task to manage incoming events from all three filters
    let listening_task_handle = start_sse_processors(
        connection_configs,
        event_listeners,
        sse_data_receivers,
        database.clone(),
        outbound_sse_data_sender.clone(),
    );

    let event_broadcasting_handle =
        start_event_broadcasting(&config, storage_path, outbound_sse_data_receiver);
    info!(address = %config.event_stream_server.port, "started {} server", "SSE");
    tokio::try_join!(
        flatten_handle(event_broadcasting_handle),
        flatten_handle(listening_task_handle),
    )
    .map(|_| Ok(ExitCode::SUCCESS))?
}

fn start_event_broadcasting(
    config: &SseEventServerConfig,
    storage_path: String,
    mut outbound_sse_data_receiver: Receiver<(SseData, Option<Filter>, Option<String>)>,
) -> JoinHandle<Result<(), Error>> {
    let event_stream_server_port = config.event_stream_server.port;
    let buffer_length = config.event_stream_server.event_stream_buffer_length;
    let max_concurrent_subscribers = config.event_stream_server.max_concurrent_subscribers;
    tokio::spawn(async move {
        // Create new instance for the Sidecar's Event Stream Server
        let mut event_stream_server = EventStreamServer::new(
            SseConfig::new(
                event_stream_server_port,
                Some(buffer_length),
                Some(max_concurrent_subscribers),
            ),
            PathBuf::from(storage_path),
        )
        .context("Error starting EventStreamServer")?;
        while let Some((sse_data, inbound_filter, maybe_json_data)) =
            outbound_sse_data_receiver.recv().await
        {
            event_stream_server.broadcast(sse_data, inbound_filter, maybe_json_data);
        }
        Err::<(), Error>(Error::msg("Event broadcasting finished"))
    })
}

fn start_sse_processors(
    connection_configs: Vec<Connection>,
    event_listeners: Vec<EventListener>,
    sse_data_receivers: Vec<Receiver<SseEvent>>,
    database: Database,
    outbound_sse_data_sender: Sender<(SseData, Option<Filter>, Option<String>)>,
) -> JoinHandle<Result<(), Error>> {
    tokio::spawn(async move {
        let mut join_handles = Vec::with_capacity(event_listeners.len());
        let api_version_manager = ApiVersionManager::new();

        for ((mut event_listener, connection_config), sse_data_receiver) in event_listeners
            .into_iter()
            .zip(connection_configs)
            .zip(sse_data_receivers)
        {
            tokio::spawn(async move {
                let res = event_listener.stream_aggregated_events().await;
                if let Err(e) = res {
                    let addr = event_listener.get_node_interface().ip_address.to_string();
                    error!("Disconnected from {}. Reason: {}", addr, e.to_string());
                }
            });
            let join_handle = spawn_sse_processor(
                &database,
                sse_data_receiver,
                &outbound_sse_data_sender,
                connection_config,
                &api_version_manager,
            );
            join_handles.push(join_handle);
        }

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
    })
}

fn spawn_sse_processor(
    database: &Database,
    sse_data_receiver: Receiver<SseEvent>,
    outbound_sse_data_sender: &Sender<(SseData, Option<Filter>, Option<String>)>,
    connection_config: Connection,
    api_version_manager: &std::sync::Arc<tokio::sync::Mutex<ApiVersionManager>>,
) -> JoinHandle<Result<(), Error>> {
    match database.clone() {
        Database::SqliteDatabaseWrapper(db) => tokio::spawn(sse_processor(
            sse_data_receiver,
            outbound_sse_data_sender.clone(),
            db.clone(),
            false,
            connection_config.enable_logging,
            api_version_manager.clone(),
        )),
        Database::PostgreSqlDatabaseWrapper(db) => tokio::spawn(sse_processor(
            sse_data_receiver,
            outbound_sse_data_sender.clone(),
            db.clone(),
            true,
            connection_config.enable_logging,
            api_version_manager.clone(),
        )),
    }
}

pub async fn run_rest_server(
    rest_server_config: RestApiServerConfig,
    database: Database,
) -> Result<ExitCode, Error> {
    match database {
        Database::SqliteDatabaseWrapper(db) => start_rest_server(rest_server_config, db).await,
        Database::PostgreSqlDatabaseWrapper(db) => start_rest_server(rest_server_config, db).await,
    }
    .map(|_| ExitCode::SUCCESS)
}

fn build_event_listeners(
    config: &SseEventServerConfig,
) -> Result<(Vec<EventListener>, Vec<Receiver<SseEvent>>), Error> {
    let mut event_listeners = Vec::with_capacity(config.connections.len());
    let mut sse_data_receivers = Vec::new();
    for connection in &config.connections {
        let (inbound_sse_data_sender, inbound_sse_data_receiver) =
            mpsc_channel(config.inbound_channel_size.unwrap_or(DEFAULT_CHANNEL_SIZE));
        sse_data_receivers.push(inbound_sse_data_receiver);
        let event_listener = builder(connection, inbound_sse_data_sender)?.build();
        event_listeners.push(event_listener?);
    }
    Ok((event_listeners, sse_data_receivers))
}

fn builder(
    connection: &Connection,
    inbound_sse_data_sender: Sender<SseEvent>,
) -> Result<EventListenerBuilder, Error> {
    let node_interface = NodeConnectionInterface {
        ip_address: IpAddr::from_str(&connection.ip_address)?,
        sse_port: connection.sse_port,
        rest_port: connection.rest_port,
    };
    let event_listener_builder = EventListenerBuilder {
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
    };
    Ok(event_listener_builder)
}

fn validate_config(config: &SseEventServerConfig) -> Result<(), Error> {
    if config
        .connections
        .iter()
        .any(|connection| connection.max_attempts < 1)
    {
        return Err(Error::msg(
            "Unable to run: max_attempts setting must be above 0 for the sidecar to attempt connection"
        ));
    }
    Ok(())
}

async fn flatten_handle<T>(handle: JoinHandle<Result<T, Error>>) -> Result<T, Error> {
    match handle.await {
        Ok(Ok(result)) => Ok(result),
        Ok(Err(err)) => Err(err),
        Err(join_err) => Err(Error::from(join_err)),
    }
}

async fn handle_database_save_result<F>(
    entity_name: &str,
    entity_identifier: &str,
    res: Result<u64, DatabaseWriteError>,
    outbound_sse_data_sender: &Sender<(SseData, Option<Filter>, Option<String>)>,
    inbound_filter: Filter,
    json_data: Option<String>,
    build_sse_data: F,
) where
    F: FnOnce() -> SseData,
{
    match res {
        Ok(_) => {
            if let Err(error) = outbound_sse_data_sender
                .send((build_sse_data(), Some(inbound_filter), json_data))
                .await
            {
                debug!(
                    "Error when sending to outbound_sse_data_sender. Error: {}",
                    error
                );
            }
        }
        Err(DatabaseWriteError::UniqueConstraint(uc_err)) => {
            debug!(
                "Already received {} ({}), logged in event_log",
                entity_name, entity_identifier,
            );
            trace!(?uc_err);
        }
        Err(other_err) => {
            count_error(format!("db_save_error_{}", entity_name).as_str());
            warn!(?other_err, "Unexpected error saving {}", entity_identifier);
        }
    }
}

/// Function to handle single event in the sse_processor.
/// Returns false if the handling indicated that no other messages should be processed.
/// Returns true otherwise.
#[allow(clippy::too_many_lines)]
async fn handle_single_event<Db: DatabaseReader + DatabaseWriter + Clone + Send + Sync>(
    sse_event: SseEvent,
    database: Db,
    enable_event_logging: bool,
    outbound_sse_data_sender: Sender<(SseData, Option<Filter>, Option<String>)>,
    api_version_manager: GuardedApiVersionManager,
) {
    match sse_event.data {
        SseData::SidecarVersion(_) => {
            //Do nothing -> the inbound shouldn't produce this endpoint, it can be only produced by sidecar to the outbound
        }
        SseData::ApiVersion(version) => {
            handle_api_version(
                api_version_manager,
                version,
                &outbound_sse_data_sender,
                sse_event.inbound_filter,
                enable_event_logging,
            )
            .await;
        }
        SseData::BlockAdded { block, block_hash } => {
            if enable_event_logging {
                let hex_block_hash = HexFmt(block_hash.inner());
                info!("Block Added: {:18}", hex_block_hash);
                debug!("Block Added: {}", hex_block_hash);
            }
            let res = database
                .save_block_added(
                    BlockAdded::new(block_hash, block.clone()),
                    sse_event.id,
                    sse_event.source.to_string(),
                    sse_event.api_version,
                    sse_event.network_name,
                )
                .await;
            handle_database_save_result(
                "BlockAdded",
                HexFmt(block_hash.inner()).to_string().as_str(),
                res,
                &outbound_sse_data_sender,
                sse_event.inbound_filter,
                sse_event.json_data,
                || SseData::BlockAdded { block, block_hash },
            )
            .await;
        }
        SseData::TransactionAccepted(transaction) => {
            let transaction_accepted = TransactionAccepted::new(transaction.clone());
            let entity_identifier = transaction_accepted.identifier();
            if enable_event_logging {
                info!("Transaction Accepted: {:18}", entity_identifier);
                debug!("Transaction Accepted: {}", entity_identifier);
            }
            let res = database
                .save_transaction_accepted(
                    transaction_accepted,
                    sse_event.id,
                    sse_event.source.to_string(),
                    sse_event.api_version,
                    sse_event.network_name,
                )
                .await;
            handle_database_save_result(
                "TransactionAccepted",
                &entity_identifier,
                res,
                &outbound_sse_data_sender,
                sse_event.inbound_filter,
                sse_event.json_data,
                || SseData::TransactionAccepted(transaction),
            )
            .await;
        }
        SseData::TransactionExpired { transaction_hash } => {
            let transaction_expired = TransactionExpired::new(transaction_hash);
            let entity_identifier = transaction_expired.identifier();
            if enable_event_logging {
                info!("Transaction Expired: {:18}", entity_identifier);
                debug!("Transaction Expired: {}", entity_identifier);
            }
            let res = database
                .save_transaction_expired(
                    transaction_expired,
                    sse_event.id,
                    sse_event.source.to_string(),
                    sse_event.api_version,
                    sse_event.network_name,
                )
                .await;
            handle_database_save_result(
                "TransactionExpired",
                &entity_identifier,
                res,
                &outbound_sse_data_sender,
                sse_event.inbound_filter,
                sse_event.json_data,
                || SseData::TransactionExpired { transaction_hash },
            )
            .await;
        }
        SseData::TransactionProcessed {
            transaction_hash,
            initiator_addr,
            timestamp,
            ttl,
            block_hash,
            execution_result,
            messages,
        } => {
            if !messages.is_empty() {
                observe_contract_messages("all", messages.len());
            }
            let transaction_processed = TransactionProcessed::new(
                transaction_hash.clone(),
                initiator_addr.clone(),
                timestamp,
                ttl,
                block_hash.clone(),
                execution_result.clone(),
                messages.clone(),
            );
            let entity_identifier = transaction_processed.identifier();
            if enable_event_logging {
                info!("Transaction Processed: {:18}", entity_identifier);
                debug!("Transaction Processed: {}", entity_identifier);
            }
            let res = database
                .save_transaction_processed(
                    transaction_processed,
                    sse_event.id,
                    sse_event.source.to_string(),
                    sse_event.api_version,
                    sse_event.network_name,
                )
                .await;
            if res.is_ok() && !messages.is_empty() {
                observe_contract_messages("unique", messages.len());
            }
            handle_database_save_result(
                "TransactionProcessed",
                &entity_identifier,
                res,
                &outbound_sse_data_sender,
                sse_event.inbound_filter,
                sse_event.json_data,
                || SseData::TransactionProcessed {
                    transaction_hash,
                    initiator_addr,
                    timestamp,
                    ttl,
                    block_hash,
                    execution_result,
                    messages,
                },
            )
            .await;
        }
        SseData::Fault {
            era_id,
            timestamp,
            public_key,
        } => {
            let fault = Fault::new(era_id, public_key.clone(), timestamp);
            warn!(%fault, "Fault reported");
            let res = database
                .save_fault(
                    fault.clone(),
                    sse_event.id,
                    sse_event.source.to_string(),
                    sse_event.api_version,
                    sse_event.network_name,
                )
                .await;

            handle_database_save_result(
                "Fault",
                format!("{:#?}", fault).as_str(),
                res,
                &outbound_sse_data_sender,
                sse_event.inbound_filter,
                sse_event.json_data,
                || SseData::Fault {
                    era_id,
                    timestamp,
                    public_key,
                },
            )
            .await;
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
            let res = database
                .save_finality_signature(
                    finality_signature.clone(),
                    sse_event.id,
                    sse_event.source.to_string(),
                    sse_event.api_version,
                    sse_event.network_name,
                )
                .await;
            handle_database_save_result(
                "FinalitySignature",
                "",
                res,
                &outbound_sse_data_sender,
                sse_event.inbound_filter,
                sse_event.json_data,
                || SseData::FinalitySignature(fs),
            )
            .await;
        }
        SseData::Step {
            era_id,
            execution_effects,
        } => {
            let step = Step::new(era_id, execution_effects.clone());
            if enable_event_logging {
                info!("Step at era: {}", era_id.value());
            }
            let res = database
                .save_step(
                    step,
                    sse_event.id,
                    sse_event.source.to_string(),
                    sse_event.api_version,
                    sse_event.network_name,
                )
                .await;
            handle_database_save_result(
                "Step",
                format!("{}", era_id.value()).as_str(),
                res,
                &outbound_sse_data_sender,
                sse_event.inbound_filter,
                sse_event.json_data,
                || SseData::Step {
                    era_id,
                    execution_effects,
                },
            )
            .await;
        }
        SseData::Shutdown => handle_shutdown(sse_event, database, outbound_sse_data_sender).await,
    }
}

async fn handle_shutdown<Db: DatabaseReader + DatabaseWriter + Clone + Send + Sync>(
    sse_event: SseEvent,
    sqlite_database: Db,
    outbound_sse_data_sender: Sender<(SseData, Option<Filter>, Option<String>)>,
) {
    warn!("Node ({}) is unavailable", sse_event.source.to_string());
    let res = sqlite_database
        .save_shutdown(
            sse_event.id,
            sse_event.source.to_string(),
            sse_event.api_version,
            sse_event.network_name,
        )
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

async fn handle_api_version(
    api_version_manager: std::sync::Arc<tokio::sync::Mutex<ApiVersionManager>>,
    version: ProtocolVersion,
    outbound_sse_data_sender: &Sender<(SseData, Option<Filter>, Option<String>)>,
    filter: Filter,
    enable_event_logging: bool,
) {
    let mut manager_guard = api_version_manager.lock().await;
    let changed_newest_version = manager_guard.store_version(version);
    if changed_newest_version {
        if let Err(error) = outbound_sse_data_sender
            .send((SseData::ApiVersion(version), Some(filter), None))
            .await
        {
            debug!(
                "Error when sending to outbound_sse_data_sender. Error: {}",
                error
            );
        }
    }
    drop(manager_guard);
    if enable_event_logging {
        info!(%version, "API Version");
    }
}

async fn sse_processor<Db: DatabaseReader + DatabaseWriter + Clone + Send + Sync + 'static>(
    inbound_sse_data_receiver: Receiver<SseEvent>,
    outbound_sse_data_sender: Sender<(SseData, Option<Filter>, Option<String>)>,
    database: Db,
    database_supports_multithreaded_processing: bool,
    enable_event_logging: bool,
    api_version_manager: GuardedApiVersionManager,
) -> Result<(), Error> {
    #[cfg(feature = "additional-metrics")]
    let metrics_tx = start_metrics_thread("sse_save".to_string());
    // This task starts the listener pushing events to the sse_data_receiver
    if database_supports_multithreaded_processing {
        start_multi_threaded_events_consumer(
            inbound_sse_data_receiver,
            outbound_sse_data_sender,
            database,
            enable_event_logging,
            api_version_manager,
            #[cfg(feature = "additional-metrics")]
            metrics_tx,
        )
        .await;
    } else {
        start_single_threaded_events_consumer(
            inbound_sse_data_receiver,
            outbound_sse_data_sender,
            database,
            enable_event_logging,
            api_version_manager,
            #[cfg(feature = "additional-metrics")]
            metrics_tx,
        )
        .await;
    }
    Ok(())
}

fn handle_events_in_thread<Db: DatabaseReader + DatabaseWriter + Clone + Send + Sync + 'static>(
    mut queue_rx: Receiver<SseEvent>,
    database: Db,
    outbound_sse_data_sender: Sender<(SseData, Option<Filter>, Option<String>)>,
    api_version_manager: GuardedApiVersionManager,
    enable_event_logging: bool,
    #[cfg(feature = "additional-metrics")] metrics_sender: Sender<()>,
) {
    tokio::spawn(async move {
        while let Some(sse_event) = queue_rx.recv().await {
            handle_single_event(
                sse_event,
                database.clone(),
                enable_event_logging,
                outbound_sse_data_sender.clone(),
                api_version_manager.clone(),
            )
            .await;
            #[cfg(feature = "additional-metrics")]
            let _ = metrics_sender.send(()).await;
        }
    });
}

fn build_queues(cache_size: usize) -> HashMap<Filter, (Sender<SseEvent>, Receiver<SseEvent>)> {
    let mut map = HashMap::new();
    map.insert(Filter::Events, mpsc_channel(cache_size));
    map
}

async fn start_multi_threaded_events_consumer<
    Db: DatabaseReader + DatabaseWriter + Clone + Send + Sync + 'static,
>(
    mut inbound_sse_data_receiver: Receiver<SseEvent>,
    outbound_sse_data_sender: Sender<(SseData, Option<Filter>, Option<String>)>,
    database: Db,
    enable_event_logging: bool,
    api_version_manager: GuardedApiVersionManager,
    #[cfg(feature = "additional-metrics")] metrics_sender: Sender<()>,
) {
    let mut senders_and_receivers_map = build_queues(DEFAULT_CHANNEL_SIZE);
    let mut senders_map = HashMap::new();
    for (filter, (tx, rx)) in senders_and_receivers_map.drain() {
        handle_events_in_thread(
            rx,
            database.clone(),
            outbound_sse_data_sender.clone(),
            api_version_manager.clone(),
            enable_event_logging,
            #[cfg(feature = "additional-metrics")]
            metrics_sender.clone(),
        );
        senders_map.insert(filter, tx);
    }

    while let Some(sse_event) = inbound_sse_data_receiver.recv().await {
        if let Some(tx) = senders_map.get(&sse_event.inbound_filter) {
            tx.send(sse_event).await.unwrap()
        } else {
            error!(
                "Failed to find an sse handler queue for inbound filter {}",
                sse_event.inbound_filter
            );
            break;
        }
    }
}

async fn start_single_threaded_events_consumer<
    Db: DatabaseReader + DatabaseWriter + Clone + Send + Sync,
>(
    mut inbound_sse_data_receiver: Receiver<SseEvent>,
    outbound_sse_data_sender: Sender<(SseData, Option<Filter>, Option<String>)>,
    database: Db,
    enable_event_logging: bool,
    api_version_manager: GuardedApiVersionManager,
    #[cfg(feature = "additional-metrics")] metrics_sender: Sender<()>,
) {
    while let Some(sse_event) = inbound_sse_data_receiver.recv().await {
        handle_single_event(
            sse_event,
            database.clone(),
            enable_event_logging,
            outbound_sse_data_sender.clone(),
            api_version_manager.clone(),
        )
        .await;
        #[cfg(feature = "additional-metrics")]
        let _ = metrics_sender.send(()).await;
    }
}

fn count_error(reason: &str) {
    observe_error("main_loop", reason);
}
