#![deny(clippy::complexity)]
#![deny(clippy::cognitive_complexity)]
#![deny(clippy::too_many_lines)]

extern crate core;
mod admin_server;
mod api_version_manager;
mod database;
mod event_handling_service;
mod event_stream_server;
pub mod rest_server;
mod sql;
#[cfg(test)]
pub(crate) mod testing;
#[cfg(test)]
pub(crate) mod tests;
mod types;
mod utils;
use std::{collections::HashMap, path::PathBuf, process::ExitCode, sync::Arc, time::Duration};

use crate::types::config::LegacySseApiTag;
use crate::{
    event_stream_server::{Config as SseConfig, EventStreamServer},
    rest_server::run_server as start_rest_server,
    types::sse_events::*,
};
use anyhow::{Context, Error};
use api_version_manager::{ApiVersionManager, GuardedApiVersionManager};
use casper_event_listener::{
    EventListener, EventListenerBuilder, NodeConnectionInterface, SseEvent,
};
use casper_event_types::{sse_data::SseData, Filter};
use casper_types::ProtocolVersion;
use event_handling_service::{
    DbSavingEventHandlingService, EventHandlingService, NoDbEventHandlingService,
};
use futures::future::join_all;
use tokio::sync::Mutex;
use tokio::{
    sync::mpsc::{channel as mpsc_channel, Receiver, Sender},
    task::JoinHandle,
    time::sleep,
};
use tracing::{error, info};
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
    index_storage_folder: String,
    maybe_database: Option<Database>,
    maybe_network_name: Option<String>,
) -> Result<ExitCode, Error> {
    validate_config(&config)?;
    let (event_listeners, sse_data_receivers) = build_event_listeners(&config, maybe_network_name)?;
    // This channel allows SseData to be sent from multiple connected nodes to the single EventStreamServer.
    let (outbound_sse_data_sender, outbound_sse_data_receiver) =
        mpsc_channel(config.outbound_channel_size.unwrap_or(DEFAULT_CHANNEL_SIZE));
    let connection_configs = config.connections.clone();

    // Task to manage incoming events from all three filters
    let listening_task_handle = start_sse_processors(
        connection_configs,
        event_listeners,
        sse_data_receivers,
        maybe_database,
        outbound_sse_data_sender.clone(),
    );

    let event_broadcasting_handle = start_event_broadcasting(
        &config,
        index_storage_folder,
        outbound_sse_data_receiver,
        config
            .emulate_legacy_sse_apis
            .as_ref()
            .map(|v| v.contains(&LegacySseApiTag::V1))
            .unwrap_or(false),
    );
    info!(address = %config.event_stream_server.port, "started {} server", "SSE");
    tokio::try_join!(
        flatten_handle(event_broadcasting_handle),
        flatten_handle(listening_task_handle),
    )
    .map(|_| Ok(ExitCode::SUCCESS))?
}

fn start_event_broadcasting(
    config: &SseEventServerConfig,
    index_storage_folder: String,
    mut outbound_sse_data_receiver: Receiver<(SseData, Option<Filter>)>,
    enable_legacy_filters: bool,
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
            PathBuf::from(index_storage_folder),
            enable_legacy_filters,
        )
        .context("Error starting EventStreamServer")?;
        while let Some((sse_data, inbound_filter)) = outbound_sse_data_receiver.recv().await {
            event_stream_server.broadcast(sse_data, inbound_filter);
        }
        Err::<(), Error>(Error::msg("Event broadcasting finished"))
    })
}

fn start_sse_processors(
    connection_configs: Vec<Connection>,
    event_listeners: Vec<EventListener>,
    sse_data_receivers: Vec<Receiver<SseEvent>>,
    maybe_database: Option<Database>,
    outbound_sse_data_sender: Sender<(SseData, Option<Filter>)>,
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
                maybe_database.clone(),
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
            .send((SseData::Shutdown, None))
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
    maybe_database: Option<Database>,
    sse_data_receiver: Receiver<SseEvent>,
    outbound_sse_data_sender: &Sender<(SseData, Option<Filter>)>,
    connection_config: Connection,
    api_version_manager: &std::sync::Arc<tokio::sync::Mutex<ApiVersionManager>>,
) -> JoinHandle<Result<(), Error>> {
    match maybe_database {
        Some(Database::SqliteDatabaseWrapper(db)) => {
            let event_handling_service = DbSavingEventHandlingService::new(
                outbound_sse_data_sender.clone(),
                db,
                connection_config.enable_logging,
            );
            tokio::spawn(sse_processor(
                sse_data_receiver,
                event_handling_service,
                false,
                api_version_manager.clone(),
            ))
        }
        Some(Database::PostgreSqlDatabaseWrapper(db)) => {
            let event_handling_service = DbSavingEventHandlingService::new(
                outbound_sse_data_sender.clone(),
                db,
                connection_config.enable_logging,
            );
            tokio::spawn(sse_processor(
                sse_data_receiver,
                event_handling_service,
                true,
                api_version_manager.clone(),
            ))
        }
        None => {
            let event_handling_service = NoDbEventHandlingService::new(
                outbound_sse_data_sender.clone(),
                connection_config.enable_logging,
            );
            tokio::spawn(sse_processor(
                sse_data_receiver,
                event_handling_service,
                true,
                api_version_manager.clone(),
            ))
        }
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
    maybe_network_name: Option<String>,
) -> Result<(Vec<EventListener>, Vec<Receiver<SseEvent>>), Error> {
    let mut event_listeners = Vec::with_capacity(config.connections.len());
    let mut sse_data_receivers = Vec::new();
    for connection in &config.connections {
        let (inbound_sse_data_sender, inbound_sse_data_receiver) =
            mpsc_channel(config.inbound_channel_size.unwrap_or(DEFAULT_CHANNEL_SIZE));
        sse_data_receivers.push(inbound_sse_data_receiver);
        let event_listener = builder(
            connection,
            inbound_sse_data_sender,
            maybe_network_name.clone(),
        )?
        .build();
        event_listeners.push(event_listener?);
    }
    Ok((event_listeners, sse_data_receivers))
}

fn builder(
    connection: &Connection,
    inbound_sse_data_sender: Sender<SseEvent>,
    maybe_network_name: Option<String>,
) -> Result<EventListenerBuilder, Error> {
    let node_interface = NodeConnectionInterface {
        ip_address: connection.ip_address,
        sse_port: connection.sse_port,
        rest_port: connection.rest_port,
        network_name: maybe_network_name,
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

/// Function to handle single event in the sse_processor.
/// Returns false if the handling indicated that no other messages should be processed.
/// Returns true otherwise.
#[allow(clippy::too_many_lines)]
async fn handle_single_event<EHS: EventHandlingService + Send + Sync>(
    sse_event: SseEvent,
    event_handling_service: EHS,
    api_version_manager: GuardedApiVersionManager,
) {
    match &sse_event.data {
        SseData::SidecarVersion(_) => {
            error!("Received SseData::SidecarVersion on inbound SSE from the node which should never happen");
            //Do nothing -> the inbound shouldn't produce this endpoint, it can be only produced by sidecar
            //to the outbound
        }
        SseData::ApiVersion(version) => {
            handle_api_version(
                version,
                api_version_manager,
                &event_handling_service,
                sse_event.inbound_filter,
            )
            .await;
        }
        SseData::BlockAdded { block, block_hash } => {
            event_handling_service
                .handle_block_added(*block_hash, block.clone(), sse_event)
                .await;
        }
        SseData::TransactionAccepted(transaction) => {
            let transaction_accepted = TransactionAccepted::new(transaction.clone());
            event_handling_service
                .handle_transaction_accepted(transaction_accepted, sse_event)
                .await;
        }
        SseData::TransactionExpired { transaction_hash } => {
            event_handling_service
                .handle_transaction_expired(*transaction_hash, sse_event)
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
            let transaction_processed = TransactionProcessed::new(
                transaction_hash.clone(),
                initiator_addr.clone(),
                *timestamp,
                *ttl,
                block_hash.clone(),
                execution_result.clone(),
                messages.clone(),
            );
            event_handling_service
                .handle_transaction_processed(transaction_processed, sse_event)
                .await;
        }
        SseData::Fault {
            era_id,
            timestamp,
            public_key,
        } => {
            event_handling_service
                .handle_fault(*era_id, *timestamp, public_key.clone(), sse_event)
                .await;
        }
        SseData::FinalitySignature(fs) => {
            let finality_signature = FinalitySignature::new(fs.clone());
            event_handling_service
                .handle_finality_signature(finality_signature, sse_event)
                .await;
        }
        SseData::Step {
            era_id,
            execution_effects,
        } => {
            let step = Step::new(*era_id, execution_effects.clone());
            event_handling_service.handle_step(step, sse_event).await;
        }
        SseData::Shutdown => event_handling_service.handle_shutdown(sse_event).await,
    }
}

async fn handle_api_version<EHS: EventHandlingService + Send + Sync>(
    version: &ProtocolVersion,
    api_version_manager: Arc<Mutex<ApiVersionManager>>,
    event_handling_service: &EHS,
    inbound_filter: Filter,
) {
    let version = *version;
    let mut manager_guard = api_version_manager.lock().await;
    let changed_newest_version = manager_guard.store_version(version);
    if changed_newest_version {
        event_handling_service
            .handle_api_version(version, inbound_filter)
            .await;
    }
    drop(manager_guard);
}

async fn sse_processor<EHS: EventHandlingService + Clone + Send + Sync + 'static>(
    inbound_sse_data_receiver: Receiver<SseEvent>,
    event_handling_service: EHS,
    database_supports_multithreaded_processing: bool,
    api_version_manager: GuardedApiVersionManager,
) -> Result<(), Error> {
    #[cfg(feature = "additional-metrics")]
    let metrics_tx = start_metrics_thread("sse_save".to_string());
    // This task starts the listener pushing events to the sse_data_receiver
    if database_supports_multithreaded_processing {
        start_multi_threaded_events_consumer(
            inbound_sse_data_receiver,
            event_handling_service,
            api_version_manager,
            #[cfg(feature = "additional-metrics")]
            metrics_tx,
        )
        .await;
    } else {
        start_single_threaded_events_consumer(
            inbound_sse_data_receiver,
            event_handling_service,
            api_version_manager,
            #[cfg(feature = "additional-metrics")]
            metrics_tx,
        )
        .await;
    }
    Ok(())
}

fn handle_events_in_thread<EHS: EventHandlingService + Clone + Send + Sync + 'static>(
    mut queue_rx: Receiver<SseEvent>,
    event_handling_service: EHS,
    api_version_manager: GuardedApiVersionManager,
    #[cfg(feature = "additional-metrics")] metrics_sender: Sender<()>,
) {
    tokio::spawn(async move {
        while let Some(sse_event) = queue_rx.recv().await {
            handle_single_event(
                sse_event,
                event_handling_service.clone(),
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
    EHS: EventHandlingService + Clone + Send + Sync + 'static,
>(
    mut inbound_sse_data_receiver: Receiver<SseEvent>,
    event_handling_service: EHS,
    api_version_manager: GuardedApiVersionManager,
    #[cfg(feature = "additional-metrics")] metrics_sender: Sender<()>,
) {
    let mut senders_and_receivers_map = build_queues(DEFAULT_CHANNEL_SIZE);
    let mut senders_map = HashMap::new();
    for (filter, (tx, rx)) in senders_and_receivers_map.drain() {
        handle_events_in_thread(
            rx,
            event_handling_service.clone(),
            api_version_manager.clone(),
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
    EHS: EventHandlingService + Clone + Send + Sync + 'static,
>(
    mut inbound_sse_data_receiver: Receiver<SseEvent>,
    event_handling_service: EHS,
    api_version_manager: GuardedApiVersionManager,
    #[cfg(feature = "additional-metrics")] metrics_sender: Sender<()>,
) {
    while let Some(sse_event) = inbound_sse_data_receiver.recv().await {
        handle_single_event(
            sse_event,
            event_handling_service.clone(),
            api_version_manager.clone(),
        )
        .await;
        #[cfg(feature = "additional-metrics")]
        let _ = metrics_sender.send(()).await;
    }
}
