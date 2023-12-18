#![deny(clippy::complexity)]
#![deny(clippy::cognitive_complexity)]
#![deny(clippy::too_many_lines)]

mod connection_manager;
mod connection_tasks;
pub mod connections_builder;
mod keep_alive_monitor;
mod sse_connector;
mod types;
use anyhow::{anyhow, Context, Error};
use casper_event_types::{metrics, Filter};
use casper_types::ProtocolVersion;
use connection_manager::{ConnectionManager, ConnectionManagerError};
use connection_tasks::ConnectionTasks;
use connections_builder::{ConnectionsBuilder, DefaultConnectionsBuilder};
use once_cell::sync::Lazy;
use serde_json::Value;
use std::{collections::HashMap, str::FromStr, sync::Arc, time::Duration};
use tokio::{
    sync::{
        mpsc::{self, Sender},
        Mutex,
    },
    time::sleep,
};
use tracing::{debug, error, info, warn};
pub use types::{NodeConnectionInterface, SseEvent};
use url::Url;

const BUILD_VERSION_KEY: &str = "build_version";
pub static MINIMAL_NODE_VERSION: Lazy<ProtocolVersion> =
    Lazy::new(|| ProtocolVersion::from_parts(1, 5, 2));
const MAX_CONNECTION_ATTEMPTS_REACHED: &str = "Max connection attempts reached";

pub struct EventListenerBuilder {
    pub node: NodeConnectionInterface,
    pub max_connection_attempts: usize,
    pub delay_between_attempts: Duration,
    pub allow_partial_connection: bool,
    pub sse_event_sender: Sender<SseEvent>,
    pub connection_timeout: Duration,
    pub sleep_between_keep_alive_checks: Duration,
    pub no_message_timeout: Duration,
}

type FilterWithEventId = Sender<(Filter, u32)>;
type CurrentFilterToIdHolder = Arc<Mutex<HashMap<Filter, u32>>>;
impl EventListenerBuilder {
    pub fn build(&self) -> Result<EventListener, Error> {
        let connections_builder = Arc::new(DefaultConnectionsBuilder {
            sleep_between_keep_alive_checks: self.sleep_between_keep_alive_checks,
            no_message_timeout: self.no_message_timeout,
            max_connection_attempts: self.max_connection_attempts,
            connection_timeout: self.connection_timeout,
            sse_event_sender: self.sse_event_sender.clone(),
            ip_address: self.node.ip_address,
            sse_port: self.node.sse_port,
            allow_partial_connection: self.allow_partial_connection,
        });
        Ok(EventListener {
            node_build_version: ProtocolVersion::from_parts(1, 0, 0),
            node: self.node.clone(),
            max_connection_attempts: self.max_connection_attempts,
            delay_between_attempts: self.delay_between_attempts,
            allow_partial_connection: self.allow_partial_connection,
            connections_builder,
        })
    }
}

/// Listener that listens to a node and all the available filters it exposes.
pub struct EventListener {
    /// Version of the node the listener is listening to. This version is discovered by the Listener on connection.
    node_build_version: ProtocolVersion,
    /// Data pointing to the node
    node: NodeConnectionInterface,
    /// Maximum numbers the listener will retry connecting to the node.
    max_connection_attempts: usize,
    /// Time the listener will wait between connection attempts
    delay_between_attempts: Duration,
    /// If set to false, the listener needs to connect to all endpoints a node should expose in a given `node_build_version` for the listener to start processing data.
    /// If set to true the listen will proceed after connecting to at least one connection.
    allow_partial_connection: bool,
    connections_builder: Arc<dyn ConnectionsBuilder>,
}

/// Helper enum determining in what state connection to a node is in.
/// It's used to named different situations in which the connection can be.
pub enum EventListenerStatus {
    /// Event Listener has not yet started to attempt the connection
    Preparing,
    /// Event Listener started establishing relevant sse connections to filters of the node
    Connecting,
    /// Event Listener got data from at least one of the nodes sse connections.
    Connected,
    /// For some reason Event Listener lost connection to the node and is trying to establish it again
    Reconnecting,
    /// If Event Listener reports this state it means that it was unable to establish a connection
    /// with node and there are no more `max_connection_attempts` left. There will be no futhrer
    /// tries to establish the connection.
    Defunct,
    /// If Event Listener reports this state it means that the node it was trying to connect to has a
    /// version which sidecar can't work with
    IncompatibleVersion,
}

impl EventListenerStatus {
    pub fn log_status(&self, node_address: &str, sse_port: u16) {
        let status = match self {
            EventListenerStatus::Preparing => 0,
            EventListenerStatus::Connecting => 1,
            EventListenerStatus::Connected => 2,
            EventListenerStatus::Reconnecting => 3,
            EventListenerStatus::Defunct => -1,
            EventListenerStatus::IncompatibleVersion => -2,
        } as f64;
        let node_label = format!("{}:{}", node_address, sse_port);
        metrics::NODE_STATUSES
            .with_label_values(&[node_label.as_str()])
            .set(status);
    }
}

enum ConnectOutcome {
    ConnectionLost,
    SystemReconnect, //In this case we don't increase the current_attempt counter
}

enum BuildVersionFetchError {
    Error(anyhow::Error),
    VersionNotAcceptable(String),
}

enum GetVersionResult {
    Ok(Option<ProtocolVersion>),
    Retry,
    Error(Error),
}

impl EventListener {
    pub fn get_node_interface(&self) -> NodeConnectionInterface {
        self.node.clone()
    }

    async fn fetch_build_version(
        &self,
        curent_protocol_version: ProtocolVersion,
        current_attempt: usize,
    ) -> Result<Option<ProtocolVersion>, BuildVersionFetchError> {
        info!(
            "Attempting to connect {} \t{}/{}",
            self.node.ip_address, current_attempt, self.max_connection_attempts
        );
        match self.fetch_build_version_from_status().await {
            Ok(version) => {
                validate_version(&version).map_err(|err| {
                    log_status_for_event_listener(EventListenerStatus::IncompatibleVersion, self);
                    err
                })?;
                let new_node_build_version = version;
                // Compare versions to reset attempts in the case that the version changed.
                // This guards against endlessly retrying when the version hasn't changed, suggesting
                // that the node is unavailable.
                // If the connection has been failing and we see the build version change we reset
                // the attempts as it may have down for an upgrade.
                if new_node_build_version != curent_protocol_version {
                    return Ok(Some(new_node_build_version));
                }
                Ok(Some(curent_protocol_version))
            }
            Err(fetch_err) => {
                error!(
                    "Error fetching build version (for {}): {fetch_err}",
                    self.node.ip_address
                );
                if current_attempt >= self.max_connection_attempts {
                    log_status_for_event_listener(EventListenerStatus::Defunct, self);
                    return Err(BuildVersionFetchError::Error(Error::msg(
                        "Unable to retrieve build version from node status",
                    )));
                }
                Ok(None)
            }
        }
    }

    /// Spins up the connections and starts pushing data from node
    pub async fn stream_aggregated_events(&mut self) -> Result<(), Error> {
        log_status_for_event_listener(EventListenerStatus::Preparing, self);
        let (last_event_id_for_filter, last_seen_event_id_sender) =
            self.start_last_event_id_registry(self.node.ip_address.to_string(), self.node.sse_port);
        log_status_for_event_listener(EventListenerStatus::Connecting, self);
        let mut current_attempt = 1;
        while current_attempt <= self.max_connection_attempts {
            if current_attempt > 1 {
                sleep(self.delay_between_attempts).await;
            }
            match self.get_version(current_attempt).await {
                GetVersionResult::Ok(Some(protocol_version)) => {
                    self.node_build_version = protocol_version;
                    current_attempt = 1 // Restart counter if the nodes version changed
                }
                GetVersionResult::Retry => {
                    current_attempt += 1;
                    if current_attempt >= self.max_connection_attempts {
                        log_status_for_event_listener(EventListenerStatus::Defunct, self);
                    }
                    continue;
                }
                GetVersionResult::Error(e) => return Err(e),
                _ => {}
            }
            if let ConnectOutcome::ConnectionLost = self
                .do_connect(
                    last_event_id_for_filter.clone(),
                    last_seen_event_id_sender.clone(),
                )
                .await?
            {
                current_attempt += 1;
                warn_connection_lost(self, current_attempt);
            }
        }
        log_status_for_event_listener(EventListenerStatus::Defunct, self);
        Err(Error::msg(MAX_CONNECTION_ATTEMPTS_REACHED))
    }

    async fn do_connect(
        &mut self,
        last_event_id_for_filter: Arc<Mutex<HashMap<Filter, u32>>>,
        last_seen_event_id_sender: FilterWithEventId,
    ) -> Result<ConnectOutcome, Error> {
        let connections = self
            .connections_builder
            .build_connections(
                last_event_id_for_filter.clone(),
                last_seen_event_id_sender.clone(),
                self.node_build_version,
            )
            .await?;
        let connection_join_handles = start_connections(connections);
        if self.allow_partial_connection {
            // We wait until either
            //  * all of the connections return error OR
            //  * one of the connection returns Err(NonRecoverableError) OR
            //  * one of the connection returns Ok(()) -> this means that we need to do a force reconnect to the node
            Ok(self
                .allow_partial_connection_wait(connection_join_handles)
                .await)
        } else {
            // Return on the first completed connection
            let select_result = futures::future::select_all(connection_join_handles).await;
            let task_result = select_result.0;
            if let Ok(res) = task_result {
                if res.is_err() {
                    log_status_for_event_listener(EventListenerStatus::Reconnecting, self);
                    return Ok(ConnectOutcome::ConnectionLost);
                }
                Ok(ConnectOutcome::SystemReconnect)
            } else {
                log_status_for_event_listener(EventListenerStatus::Reconnecting, self);
                Ok(ConnectOutcome::ConnectionLost)
            }
        }
    }
    async fn allow_partial_connection_wait(
        &mut self,
        mut connection_join_handles: Vec<
            tokio::task::JoinHandle<Result<(), ConnectionManagerError>>,
        >,
    ) -> ConnectOutcome {
        loop {
            let select_result = futures::future::select_all(connection_join_handles).await;
            let task_result = select_result.0;
            let futures_left = select_result.2;
            if task_result.is_err() {
                return ConnectOutcome::ConnectionLost;
            }
            let res = task_result.unwrap();
            match res {
                Ok(_) => {
                    return ConnectOutcome::SystemReconnect;
                }
                Err(err) => {
                    match err {
                        ConnectionManagerError::NonRecoverableError { error } => {
                            error!(
                                "Restarting event listener {} because of NonRecoverableError: {}",
                                self.node.ip_address.to_string(),
                                error
                            );
                            log_status_for_event_listener(EventListenerStatus::Reconnecting, self);
                            return ConnectOutcome::ConnectionLost;
                        }
                        ConnectionManagerError::InitialConnectionError { error } => {
                            //No futures_left means no more filters active, we need to restart the whole listener
                            if futures_left.is_empty() {
                                error!("Restarting event listener {} because of no more active connections left: {}", self.node.ip_address.to_string(), error);
                                log_status_for_event_listener(
                                    EventListenerStatus::Reconnecting,
                                    self,
                                );
                                return ConnectOutcome::ConnectionLost;
                            }
                        }
                    }
                }
            }
            connection_join_handles = futures_left
        }
    }

    fn status_endpoint(&self) -> Result<Url, Error> {
        let status_endpoint_str = format!(
            "http://{}:{}/status",
            self.node.ip_address, self.node.rest_port
        );
        Url::from_str(&status_endpoint_str).map_err(Error::from)
    }

    // Fetch the build version by requesting the status from the node's rest server.
    async fn fetch_build_version_from_status(&self) -> Result<ProtocolVersion, Error> {
        debug!(
            "Fetching build version for {}",
            self.status_endpoint().unwrap()
        );

        let status_endpoint = self
            .status_endpoint()
            .context("Should have created status URL")?;

        let status_response = reqwest::get(status_endpoint)
            .await
            .context("Should have responded with status")?;

        // The exact structure of the response varies over the versions but there is an assertion made
        // that .build_version is always valid. So the key is accessed without deserialising the full response.
        let response_json: Value = status_response
            .json()
            .await
            .context("Should have parsed JSON from response")?;

        try_resolve_version(response_json)
    }

    async fn get_version(&mut self, current_attempt: usize) -> GetVersionResult {
        info!(
            "Attempting to connect...\t{}/{}",
            current_attempt, self.max_connection_attempts
        );
        let fetch_result = self
            .fetch_build_version(self.node_build_version, current_attempt)
            .await;
        match fetch_result {
            Ok(Some(new_node_build_version)) => {
                if self.node_build_version != new_node_build_version {
                    return GetVersionResult::Ok(Some(new_node_build_version));
                }
                GetVersionResult::Ok(None)
            }
            Err(BuildVersionFetchError::VersionNotAcceptable(msg)) => {
                log_status_for_event_listener(EventListenerStatus::IncompatibleVersion, self);
                //The node has a build version which sidecar can't talk to. Failing fast in this case.
                GetVersionResult::Error(Error::msg(msg))
            }
            _ => GetVersionResult::Retry,
        }
    }

    fn start_last_event_id_registry(
        &self,
        node_address: String,
        sse_port: u16,
    ) -> (CurrentFilterToIdHolder, FilterWithEventId) {
        let (last_seen_event_id_sender, mut last_seen_event_id_receiver) = mpsc::channel(10);
        let last_event_id_for_filter: CurrentFilterToIdHolder =
            Arc::new(Mutex::new(HashMap::<Filter, u32>::new()));
        let last_event_id_for_filter_for_thread = last_event_id_for_filter.clone();
        tokio::spawn(async move {
            while let Some((filter, id)) = last_seen_event_id_receiver.recv().await {
                EventListenerStatus::Connected.log_status(node_address.as_str(), sse_port);
                let last_event_id_for_filter_clone = last_event_id_for_filter_for_thread.clone();
                let mut guard = last_event_id_for_filter_clone.lock().await;
                guard.insert(filter, id);
                drop(guard);
            }
        });
        (last_event_id_for_filter, last_seen_event_id_sender)
    }
}

fn start_connections(
    connections: HashMap<Filter, Box<dyn ConnectionManager>>,
) -> Vec<tokio::task::JoinHandle<Result<(), ConnectionManagerError>>> {
    connections
        .into_iter()
        .map(|(filter, mut connection)| {
            debug!("Connecting filter... {}", filter);
            tokio::spawn(async move {
                let res = connection.start_handling().await;
                match res {
                    Ok(_) => Ok(()),
                    Err(e) => {
                        error!("Error on start_handling: {}", e);
                        Err(e)
                    }
                }
            })
        })
        .collect()
}

fn try_resolve_version(raw_response: Value) -> Result<ProtocolVersion, Error> {
    match raw_response.get(BUILD_VERSION_KEY) {
        Some(build_version_value) if build_version_value.is_string() => {
            let raw = build_version_value
                .as_str()
                .context("build_version_value should be a string")
                .map_err(|e| {
                    count_error("version_value_not_a_string");
                    e
                })?
                .split('-')
                .next()
                .context("splitting build_version_value should always return at least one slice")
                .map_err(|e| {
                    count_error("incomprehensible_build_version_form");
                    e
                })?;
            ProtocolVersion::from_str(raw).map_err(|error| {
                count_error("failed_parsing_protocol_version");
                anyhow!("failed parsing build version from '{}': {}", raw, error)
            })
        }
        _ => {
            count_error("failed_getting_status_from_payload");
            Err(anyhow!(
                "failed to get {} from status response {}",
                BUILD_VERSION_KEY,
                raw_response
            ))
        }
    }
}

fn log_status_for_event_listener(status: EventListenerStatus, event_listener: &EventListener) {
    let node_address = event_listener.node.ip_address.to_string();
    let sse_port = event_listener.node.sse_port;
    status.log_status(node_address.as_str(), sse_port);
}

fn count_error(reason: &str) {
    casper_event_types::metrics::ERROR_COUNTS
        .with_label_values(&["event_listener", reason])
        .inc();
}

fn validate_version(version: &ProtocolVersion) -> Result<(), BuildVersionFetchError> {
    if version.lt(&MINIMAL_NODE_VERSION) {
        let msg = format!(
            "Node version expected to be >= {}.",
            MINIMAL_NODE_VERSION.value(),
        );
        Err(BuildVersionFetchError::VersionNotAcceptable(msg))
    } else {
        Ok(())
    }
}

fn warn_connection_lost(listener: &EventListener, current_attempt: usize) {
    warn!(
        "Lost connection to node {}, on attempt {}/{}",
        listener.node.ip_address, current_attempt, listener.max_connection_attempts
    );
}
