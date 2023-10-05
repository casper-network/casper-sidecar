#![deny(clippy::complexity)]
#![deny(clippy::cognitive_complexity)]
#![deny(clippy::too_many_lines)]

mod connection_manager;
mod connection_tasks;
mod keep_alive_monitor;
mod sse_connector;
mod types;
use crate::connection_manager::ConnectionManagerBuilder;
use anyhow::{anyhow, Context, Error};
use casper_event_types::{metrics, Filter};
use casper_types::ProtocolVersion;
use connection_manager::{ConnectionManager, ConnectionManagerError};
use connection_tasks::ConnectionTasks;
use keep_alive_monitor::KeepAliveMonitor;
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
use tracing::{debug, error, info, trace};
pub use types::{NodeConnectionInterface, SseEvent};
use url::Url;

const BUILD_VERSION_KEY: &str = "build_version";
pub static MINIMAL_NODE_VERSION: Lazy<ProtocolVersion> =
    Lazy::new(|| ProtocolVersion::from_parts(1, 5, 2));

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
    pub fn build(&self) -> EventListener {
        EventListener {
            node_build_version: ProtocolVersion::from_parts(1, 0, 0),
            node: self.node.clone(),
            max_connection_attempts: self.max_connection_attempts,
            delay_between_attempts: self.delay_between_attempts,
            allow_partial_connection: self.allow_partial_connection,
            sse_event_sender: self.sse_event_sender.clone(),
            connection_timeout: self.connection_timeout,
            sleep_between_keep_alive_checks: self.sleep_between_keep_alive_checks,
            no_message_timeout: self.no_message_timeout,
        }
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
    /// Channel to which data from the node is pushed
    sse_event_sender: Sender<SseEvent>,
    /// Maximum duration we will wait to establish a connection to the node
    connection_timeout: Duration,
    /// Time the KeepAliveMonitor wait between checks
    sleep_between_keep_alive_checks: Duration,
    /// Time of inactivity of a node connection that is allowed by KeepAliveMonitor
    no_message_timeout: Duration,
}

/// Helper enum determining in what state connection to a node is in.
/// It's used to named different situations in which the connection can be.
enum EventListenerStatus {
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
}

impl EventListenerStatus {
    fn log_status_for_event_listener(&self, event_listener: &EventListener) {
        let node_address = event_listener.node.ip_address.to_string();
        let sse_port = event_listener.node.sse_port;
        EventListenerStatus::log_status(self, node_address.as_str(), sse_port);
    }

    fn log_status(&self, node_address: &str, sse_port: u16) {
        let status = match self {
            EventListenerStatus::Preparing => 0,
            EventListenerStatus::Connecting => 1,
            EventListenerStatus::Connected => 2,
            EventListenerStatus::Reconnecting => 3,
            EventListenerStatus::Defunct => -1,
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
    fn filtered_sse_url(&self, filter: &Filter) -> Result<Url, Error> {
        let url_str = format!(
            "http://{}:{}/{}",
            self.node.ip_address, self.node.sse_port, filter
        );
        Url::parse(&url_str).map_err(Error::from)
    }

    async fn fetch_build_version(
        &self,
        curent_protocol_version: ProtocolVersion,
        current_attempt: usize,
    ) -> Result<Option<ProtocolVersion>, BuildVersionFetchError> {
        info!(
            "Attempting to connect...\t{}/{}",
            current_attempt, self.max_connection_attempts
        );
        match self.fetch_build_version_from_status().await {
            Ok(version) => {
                validate_version(&version)?;
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
                    EventListenerStatus::Defunct.log_status_for_event_listener(self);
                    return Err(BuildVersionFetchError::Error(Error::msg(
                        "Unable to retrieve build version from node status",
                    )));
                }
                Ok(None)
            }
        }
    }

    /// Spins up the connections and starts pushing data from node
    ///
    /// * `is_empty_database` - if set to true, sidecar will connect to the node and fetch all the events the node has in it's cache.
    pub async fn stream_aggregated_events(&mut self, is_empty_database: bool) -> Result<(), Error> {
        EventListenerStatus::Preparing.log_status_for_event_listener(self);
        let (last_event_id_for_filter, last_seen_event_id_sender) =
            self.start_last_event_id_registry(self.node.ip_address.to_string(), self.node.sse_port);
        EventListenerStatus::Connecting.log_status_for_event_listener(self);
        let mut current_attempt = 1;
        while current_attempt <= self.max_connection_attempts {
            match self.get_version(current_attempt).await {
                GetVersionResult::Ok(Some(protocol_version)) => {
                    self.node_build_version = protocol_version;
                    current_attempt = 1
                }
                GetVersionResult::Retry => {
                    sleep(self.delay_between_attempts).await;
                    current_attempt += 1;
                    continue;
                }
                GetVersionResult::Error(e) => return Err(e),
                _ => {}
            }
            match self
                .do_connect(
                    last_event_id_for_filter.clone(),
                    is_empty_database,
                    last_seen_event_id_sender.clone(),
                )
                .await?
            {
                ConnectOutcome::ConnectionLost => current_attempt += 1,
                ConnectOutcome::SystemReconnect => {}
            };
            sleep(Duration::from_secs(1)).await;
        }
        EventListenerStatus::Defunct.log_status_for_event_listener(self);
        Ok(())
    }

    async fn do_connect(
        &mut self,
        last_event_id_for_filter: Arc<Mutex<HashMap<Filter, u32>>>,
        is_empty_database: bool,
        last_seen_event_id_sender: FilterWithEventId,
    ) -> Result<ConnectOutcome, Error> {
        let connections = self
            .build_connections(
                last_event_id_for_filter.clone(),
                is_empty_database,
                last_seen_event_id_sender.clone(),
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
            if task_result.is_ok() {
                let res = task_result.unwrap();
                if res.is_err() {
                    EventListenerStatus::Reconnecting.log_status_for_event_listener(self);
                    return Ok(ConnectOutcome::ConnectionLost);
                }
                Ok(ConnectOutcome::SystemReconnect)
            } else {
                EventListenerStatus::Reconnecting.log_status_for_event_listener(self);
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
                            EventListenerStatus::Reconnecting.log_status_for_event_listener(self);
                            return ConnectOutcome::ConnectionLost;
                        }
                        ConnectionManagerError::InitialConnectionError { error } => {
                            //No futures_left means no more filters active, we need to restart the whole listener
                            if futures_left.is_empty() {
                                error!("Restarting event listener {} because of no more active connections left: {}", self.node.ip_address.to_string(), error);
                                EventListenerStatus::Reconnecting
                                    .log_status_for_event_listener(self);
                                return ConnectOutcome::ConnectionLost;
                            }
                        }
                    }
                }
            }
            connection_join_handles = futures_left
        }
    }

    async fn build_connections(
        &mut self,
        last_event_id_for_filter: Arc<Mutex<HashMap<Filter, u32>>>,
        is_empty_database: bool,
        last_seen_event_id_sender: FilterWithEventId,
    ) -> Result<HashMap<Filter, ConnectionManager>, Error> {
        let filters = filters_from_version(self.node_build_version);
        let mut connections = HashMap::new();
        let maybe_tasks =
            (!self.allow_partial_connection).then(|| ConnectionTasks::new(filters.len()));
        let guard = last_event_id_for_filter.lock().await;
        for filter in filters {
            let mut start_from_event_id = guard.get(&filter).copied();
            if is_empty_database && start_from_event_id.is_none() {
                start_from_event_id = Some(0);
            }
            let connection = self
                .build_connection(
                    maybe_tasks.clone(),
                    start_from_event_id,
                    filter.clone(),
                    last_seen_event_id_sender.clone(),
                )
                .await?;
            connections.insert(filter, connection);
        }
        drop(guard);
        Ok(connections)
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

    async fn build_connection(
        &self,
        maybe_tasks: Option<ConnectionTasks>,
        start_from_event_id: Option<u32>,
        filter: Filter,
        last_seen_event_id_sender: FilterWithEventId,
    ) -> Result<ConnectionManager, Error> {
        let bind_address_for_filter = self.filtered_sse_url(&filter)?;
        let keep_alive_monitor = KeepAliveMonitor::new(
            bind_address_for_filter.clone(),
            self.connection_timeout,
            self.sleep_between_keep_alive_checks,
            self.no_message_timeout,
        );
        keep_alive_monitor.start().await;
        let cancellation_token = keep_alive_monitor.get_cancellation_token();
        let builder = ConnectionManagerBuilder {
            bind_address: bind_address_for_filter,
            max_attempts: self.max_connection_attempts,
            sse_data_sender: self.sse_event_sender.clone(),
            maybe_tasks,
            connection_timeout: self.connection_timeout,
            start_from_event_id,
            filter,
            current_event_id_sender: last_seen_event_id_sender,
            cancellation_token,
        };
        Ok(builder.build())
    }

    async fn get_version(&mut self, current_attempt: usize) -> GetVersionResult {
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
    connections: HashMap<Filter, ConnectionManager>,
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

fn filters_from_version(build_version: ProtocolVersion) -> Vec<Filter> {
    trace!("Getting filters for version...\t{}", build_version);

    // Prior to this the node only had an /events endpoint producing all events.
    let one_three_zero = ProtocolVersion::from_parts(1, 3, 0);
    // From 1.3.X the node had /events/main and /events/sigs
    let one_four_zero = ProtocolVersion::from_parts(1, 4, 0);
    // From 1.4.X the node added /events/deploys (in addition to /events/main and /events/sigs)

    let mut filters = Vec::new();

    if build_version.lt(&one_three_zero) {
        filters.push(Filter::Events);
    } else if build_version.lt(&one_four_zero) {
        filters.push(Filter::Main);
        filters.push(Filter::Sigs);
    } else {
        filters.push(Filter::Main);
        filters.push(Filter::Sigs);
        filters.push(Filter::Deploys);
    }

    filters
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

#[cfg(test)]
mod tests {

    use anyhow::Error;
    use casper_types::{ProtocolVersion, SemVer};
    use serde_json::json;

    use crate::{try_resolve_version, BUILD_VERSION_KEY};

    #[test]
    fn try_resolve_version_should_interpret_correct_build_version() {
        let mut protocol = test_by_build_version(Some("5.1.111-b94c4f79a")).unwrap();
        assert_eq!(protocol, ProtocolVersion::new(SemVer::new(5, 1, 111)));

        protocol = test_by_build_version(Some("6.2.112-b94c4f79a-casper-mainnet")).unwrap();
        assert_eq!(protocol, ProtocolVersion::new(SemVer::new(6, 2, 112)));

        protocol = test_by_build_version(Some("7.3.113")).unwrap();
        assert_eq!(protocol, ProtocolVersion::new(SemVer::new(7, 3, 113)));
    }

    #[test]
    fn try_resolve_should_fail_if_build_version_is_absent() {
        let ret = test_by_build_version(None);
        assert!(ret.is_err());
    }

    #[test]
    fn try_resolve_should_fail_if_build_version_is_invalid() {
        let ret = test_by_build_version(Some("not-a-semver"));
        assert!(ret.is_err());
    }

    fn test_by_build_version(build_version: Option<&str>) -> Result<ProtocolVersion, Error> {
        let json_object = match build_version {
            Some(version) => json!({ BUILD_VERSION_KEY: version }),
            None => json!({}),
        };
        try_resolve_version(json_object)
    }
}
