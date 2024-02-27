#![deny(clippy::complexity)]
#![deny(clippy::cognitive_complexity)]
#![deny(clippy::too_many_lines)]

mod connection_manager;
mod connection_tasks;
pub mod connections_builder;
mod event_listener_status;
mod keep_alive_monitor;
mod sse_connector;
mod types;
mod version_fetcher;
use crate::event_listener_status::*;
use anyhow::Error;
use casper_event_types::Filter;
use casper_types::ProtocolVersion;
use connection_manager::{ConnectionManager, ConnectionManagerError};
use connection_tasks::ConnectionTasks;
use connections_builder::{ConnectionsBuilder, DefaultConnectionsBuilder};
use std::{collections::HashMap, net::IpAddr, str::FromStr, sync::Arc, time::Duration};
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
use version_fetcher::{for_status_endpoint, BuildVersionFetchError, VersionFetcher};

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
        let status_endpoint = status_endpoint(self.node.ip_address, self.node.rest_port)?;
        let version_fetcher = Arc::new(for_status_endpoint(status_endpoint));
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
            version_fetcher,
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
    /// Fetches the build version of the node
    version_fetcher: Arc<dyn VersionFetcher>,
    /// Builder of the connections to the node
    connections_builder: Arc<dyn ConnectionsBuilder>,
}

enum ConnectOutcome {
    ConnectionLost,
    SystemReconnect, //In this case we don't increase the current_attempt counter
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
            if let Ok(ConnectOutcome::ConnectionLost) = self
                .do_connect(
                    last_event_id_for_filter.clone(),
                    last_seen_event_id_sender.clone(),
                )
                .await
            {
                warn_connection_lost(self, current_attempt);
            }
            current_attempt += 1;
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
                                "Restarting event listener {}:{} because of NonRecoverableError: {}",
                                self.node.ip_address.to_string(),
                                self.node.sse_port,
                                error
                            );
                            log_status_for_event_listener(EventListenerStatus::Reconnecting, self);
                            return ConnectOutcome::ConnectionLost;
                        }
                        ConnectionManagerError::InitialConnectionError { error } => {
                            //No futures_left means no more filters active, we need to restart the whole listener
                            if futures_left.is_empty() {
                                error!("Restarting event listener {}:{} because of no more active connections left: {}", self.node.ip_address.to_string(), self.node.sse_port, error);
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

    async fn get_version(&mut self, current_attempt: usize) -> GetVersionResult {
        info!(
            "Attempting to connect...\t{}/{}",
            current_attempt, self.max_connection_attempts
        );
        let fetch_result = self.version_fetcher.fetch().await;
        match fetch_result {
            Ok(new_node_build_version) => {
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
            Err(BuildVersionFetchError::Error(err)) => {
                error!(
                    "Error fetching build version (for {}): {err}",
                    self.node.ip_address
                );
                GetVersionResult::Retry
            }
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

fn log_status_for_event_listener(status: EventListenerStatus, event_listener: &EventListener) {
    let node_address = event_listener.node.ip_address.to_string();
    let sse_port = event_listener.node.sse_port;
    status.log_status(node_address.as_str(), sse_port);
}

fn status_endpoint(ip_address: IpAddr, rest_port: u16) -> Result<Url, Error> {
    let status_endpoint_str = format!("http://{}:{}/status", ip_address, rest_port);
    Url::from_str(&status_endpoint_str).map_err(Error::from)
}

fn warn_connection_lost(listener: &EventListener, current_attempt: usize) {
    warn!(
        "Lost connection to node {}, on attempt {}/{}",
        listener.node.ip_address, current_attempt, listener.max_connection_attempts
    );
}

#[cfg(test)]
mod tests {
    use crate::{
        connections_builder::tests::MockConnectionsBuilder,
        version_fetcher::{tests::MockVersionFetcher, BuildVersionFetchError},
        EventListener, NodeConnectionInterface,
    };
    use anyhow::Error;
    use casper_types::ProtocolVersion;
    use std::{collections::HashSet, str::FromStr, sync::Arc, time::Duration};

    #[tokio::test]
    async fn given_event_listener_should_not_connect_when_incompatible_version() {
        let version_fetcher = MockVersionFetcher::new(vec![Err(
            BuildVersionFetchError::VersionNotAcceptable("1.5.10".to_string()),
        )]);
        let connections_builder = Arc::new(MockConnectionsBuilder::default());

        let err = run_event_listener(2, version_fetcher, connections_builder.clone(), true).await;

        assert!(err.to_string().contains("1.5.10"));
    }

    #[tokio::test]
    async fn given_event_listener_should_retry_version_fetch_when_first_response_is_error() {
        let protocol_version = ProtocolVersion::from_str("1.5.10").unwrap();
        let version_fetcher = MockVersionFetcher::new(vec![
            Err(BuildVersionFetchError::Error(Error::msg("retryable error"))),
            Ok(protocol_version),
        ]);
        let connections_builder = Arc::new(MockConnectionsBuilder::one_ok());

        let err = run_event_listener(2, version_fetcher, connections_builder.clone(), true).await;

        let received_data = connections_builder.get_received_data().await;
        assert_eq!(received_data.len(), 1);
        assert!(set_contains(received_data, vec!["events-1"],));
        assert!(err.to_string().contains("Max connection attempts reached"));
    }

    #[tokio::test]
    async fn given_event_listener_should_fail_if_connection_fails() {
        let version_fetcher = MockVersionFetcher::repeatable_from_protocol_version("1.5.10");
        let connections_builder = Arc::new(MockConnectionsBuilder::connection_fails());

        let err = run_event_listener(1, version_fetcher, connections_builder.clone(), true).await;

        assert!(err.to_string().contains("Max connection attempts reached"));
        let received_data = connections_builder.get_received_data().await;
        assert!(received_data.is_empty());
    }

    #[tokio::test]
    async fn given_event_listener_should_fetch_data_if_enough_reconnections() {
        let version_fetcher = MockVersionFetcher::repeatable_from_protocol_version("2.0.0");
        let connections_builder = Arc::new(MockConnectionsBuilder::ok_after_two_fails());

        let err = run_event_listener(3, version_fetcher, connections_builder.clone(), true).await;

        let received_data = connections_builder.get_received_data().await;
        assert_eq!(received_data.len(), 1);
        assert!(set_contains(received_data, vec!["events-2"],));
        assert!(err.to_string().contains("Max connection attempts reached"));
    }

    #[tokio::test]
    async fn given_event_listener_should_give_up_retrying_if_runs_out() {
        let version_fetcher = MockVersionFetcher::repeatable_from_protocol_version("1.5.10");
        let connections_builder = Arc::new(MockConnectionsBuilder::ok_after_two_fails());

        let err = run_event_listener(2, version_fetcher, connections_builder.clone(), true).await;
        assert!(err.to_string().contains("Max connection attempts reached"));
        let received_data = connections_builder.get_received_data().await;
        assert!(received_data.is_empty());
    }

    async fn run_event_listener(
        max_connection_attempts: usize,
        version_fetcher: MockVersionFetcher,
        connections_builder: Arc<MockConnectionsBuilder>,
        allow_partial_connection: bool,
    ) -> Error {
        let mut listener = EventListener {
            node_build_version: ProtocolVersion::from_parts(1, 0, 0),
            node: NodeConnectionInterface::default(),
            max_connection_attempts,
            delay_between_attempts: Duration::from_secs(1),
            allow_partial_connection,
            version_fetcher: Arc::new(version_fetcher),
            connections_builder,
        };
        listener.stream_aggregated_events().await.unwrap_err()
    }

    fn set_contains(set: HashSet<String>, value: Vec<&str>) -> bool {
        value.iter().all(|v| set.contains(*v))
    }
}
