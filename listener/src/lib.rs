mod connection_manager;
mod connection_tasks;
mod keep_alive_monitor;
mod types;
use crate::connection_manager::ConnectionManagerBuilder;
use anyhow::{anyhow, Context, Error};
use casper_event_types::{metrics, Filter};
use casper_types::ProtocolVersion;
use connection_manager::{ConnectionManager, ConnectionManagerError};
use connection_tasks::ConnectionTasks;
use keep_alive_monitor::KeepAliveMonitor;
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
    SettingUp,
    Connecting,
    Connected,
    Reconnecting,
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
            EventListenerStatus::SettingUp => 0,
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

impl EventListener {
    fn filtered_sse_url(&self, filter: &Filter) -> Result<Url, Error> {
        let url_str = format!(
            "http://{}:{}/{}",
            self.node.ip_address, self.node.sse_port, filter
        );
        Url::parse(&url_str).map_err(Error::from)
    }

    /// Spins up the connections and starts pushing data from node
    ///
    /// * `is_empty_database` - if set to true, sidecar will connect to the node and fetch all the events the node has in it's cache.
    pub async fn stream_aggregated_events(&mut self, is_empty_database: bool) -> Result<(), Error> {
        EventListenerStatus::SettingUp.log_status_for_event_listener(self);
        let mut attempts = 1;
        let last_event_id_for_filter = Arc::new(Mutex::new(HashMap::<Filter, u32>::new()));
        let (last_seen_event_id_sender, mut last_seen_event_id_receiver) = mpsc::channel(10);

        let local_last_event_id_for_filter = last_event_id_for_filter.clone();
        let node_address = self.node.ip_address.to_string();
        let sse_port = self.node.sse_port;
        tokio::spawn(async move {
            while let Some((filter, id)) = last_seen_event_id_receiver.recv().await {
                EventListenerStatus::Connected.log_status(node_address.as_str(), sse_port);
                let last_event_id_for_filter_clone = local_last_event_id_for_filter.clone();
                let mut guard = last_event_id_for_filter_clone.lock().await;
                guard.insert(filter, id);
                drop(guard);
            }
        });
        EventListenerStatus::Connecting.log_status_for_event_listener(self);
        while attempts <= self.max_connection_attempts {
            info!(
                "Attempting to connect...\t{}/{}",
                attempts, self.max_connection_attempts
            );
            match self.fetch_build_version_from_status().await {
                Ok(version) => {
                    let new_node_build_version = version;
                    // Compare versions to reset attempts in the case that the version changed.
                    // This guards against endlessly retrying when the version hasn't changed, suggesting
                    // that the node is unavailable.
                    // If the connection has been failing and we see the build version change we reset
                    // the attempts as it may have down for an upgrade.
                    if new_node_build_version != self.node_build_version {
                        attempts = 1;
                    }

                    self.node_build_version = new_node_build_version;
                }
                Err(fetch_err) => {
                    error!(
                        "Error fetching build version (for {}): {fetch_err}",
                        self.node.ip_address
                    );

                    if attempts >= self.max_connection_attempts {
                        EventListenerStatus::Defunct.log_status_for_event_listener(self);
                        return Err(Error::msg(
                            "Unable to retrieve build version from node status",
                        ));
                    }

                    sleep(self.delay_between_attempts).await;
                    attempts += 1;
                    continue;
                }
            }
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

            let mut connection_join_handles = Vec::new();

            for (filter, mut connection) in connections.drain() {
                debug!("Connecting filter... {}", filter);
                let handle = tokio::spawn(async move {
                    let res = connection.start_handling().await;
                    match res {
                        Ok(_) => Ok(()),
                        Err(e) => {
                            error!("Error on start_handling: {}", e);
                            Err(e)
                        }
                    }
                });
                connection_join_handles.push(handle);
            }

            if self.allow_partial_connection {
                // We wait until either
                //  * all of the connections return error OR
                //  * one of the connection returns Err(NonRecoverableError) OR
                //  * one of the connection returns Ok(()) -> this means that we need to do a force reconnect to the node
                loop {
                    let select_result = futures::future::select_all(connection_join_handles).await;
                    let task_result = select_result.0;
                    let futures_left = select_result.2;
                    if task_result.is_err() {
                        break;
                    }
                    let res = task_result.unwrap();
                    match res {
                        Ok(_) => {
                            //Not increasing attempts on "clean exit" of connection manager
                            break;
                        }
                        Err(err) => {
                            match err {
                                ConnectionManagerError::NonRecoverableError { error } => {
                                    error!(
                                        "Restarting event listener {} because of NonRecoverableError: {}",
                                        self.node.ip_address.to_string(),
                                        error
                                    );
                                    attempts += 1;
                                    EventListenerStatus::Reconnecting
                                        .log_status_for_event_listener(self);
                                    break;
                                }
                                ConnectionManagerError::InitialConnectionError { error } => {
                                    //No futures_left means no more filters active, we need to restart the whole listener
                                    if futures_left.is_empty() {
                                        error!("Restarting event listener {} because of no more active connections left: {}", self.node.ip_address.to_string(), error);
                                        attempts += 1;
                                        EventListenerStatus::Reconnecting
                                            .log_status_for_event_listener(self);
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    connection_join_handles = futures_left
                }
            } else {
                // Return on the first completed connection
                let select_result = futures::future::select_all(connection_join_handles).await;
                let task_result = select_result.0;
                if task_result.is_ok() {
                    let res = task_result.unwrap();
                    if res.is_err() {
                        attempts += 1;
                        EventListenerStatus::Reconnecting.log_status_for_event_listener(self);
                    }
                    //Not increasing attempts on "clean exit" of connection manager
                } else {
                    attempts += 1;
                    EventListenerStatus::Reconnecting.log_status_for_event_listener(self);
                }
            }
        }
        EventListenerStatus::Defunct.log_status_for_event_listener(self);
        Ok(())
    }

    fn status_endpoint(&self) -> Result<Url, Error> {
        let status_endpoint_str = format!(
            "http://{}:{}/status",
            self.node.ip_address, self.node.rest_port
        );
        Url::from_str(&status_endpoint_str).map_err(Error::from)
    }

    // Fetch the build version by requesting the status from the node's rest server.
    async fn fetch_build_version_from_status(&mut self) -> Result<ProtocolVersion, Error> {
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
        last_seen_event_id_sender: Sender<(Filter, u32)>,
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
            node_build_version: self.node_build_version,
            current_event_id_sender: last_seen_event_id_sender,
            cancellation_token,
        };
        Ok(builder.build())
    }
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
