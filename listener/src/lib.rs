mod connection_manager;
mod connection_tasks;

use std::{
    collections::HashMap,
    fmt::{Display, Formatter},
    net::IpAddr,
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use anyhow::{anyhow, Context, Error};
use casper_types::ProtocolVersion;
pub use connection_manager::SseEvent;
use connection_manager::{ConnectionManager, ConnectionManagerError};
use connection_tasks::ConnectionTasks;
use serde_json::Value;
use tokio::sync::{mpsc::Sender, Mutex};
use tracing::{debug, error, info, trace};
use url::Url;
const BUILD_VERSION_KEY: &str = "build_version";

pub struct NodeConnectionInterface {
    pub ip_address: IpAddr,
    pub sse_port: u16,
    pub rest_port: u16,
}

#[derive(Hash, Eq, PartialEq, Debug, Clone)]
enum Filter {
    Events,
    Main,
    Deploys,
    Sigs,
}

impl Display for Filter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Filter::Events => write!(f, "events"),
            Filter::Main => write!(f, "events/main"),
            Filter::Deploys => write!(f, "events/deploys"),
            Filter::Sigs => write!(f, "events/sigs"),
        }
    }
}

pub struct EventListener {
    api_version: ProtocolVersion,
    node: NodeConnectionInterface,
    max_connection_attempts: usize,
    delay_between_attempts: Duration,
    allow_partial_connection: bool,
    sse_event_sender: Sender<SseEvent>,
    connection_timeout: Duration,
}

impl EventListener {
    pub fn new(
        node: NodeConnectionInterface,
        max_connection_attempts: usize,
        delay_between_attempts: Duration,
        allow_partial_connection: bool,
        sse_event_sender: Sender<SseEvent>,
        connection_timeout: Duration,
    ) -> Self {
        Self {
            api_version: ProtocolVersion::from_parts(1, 0, 0),
            node,
            max_connection_attempts,
            delay_between_attempts,
            allow_partial_connection,
            sse_event_sender,
            connection_timeout,
        }
    }

    fn filtered_sse_url(&self, filter: &Filter) -> Result<Url, Error> {
        let url_str = format!(
            "http://{}:{}/{}",
            self.node.ip_address, self.node.sse_port, filter
        );
        Url::parse(&url_str).map_err(Error::from)
    }

    pub async fn stream_aggregated_events(
        &mut self,
        initial_api_version_sender: Sender<Result<ProtocolVersion, Error>>,
    ) -> Result<(), Error> {
        let mut attempts = 0;
        let last_event_ids_for_filter = Arc::new(Mutex::new(HashMap::<Filter, u32>::new()));

        // Wrap the sender in an Option so it can be exhausted with .take() on the initial connection.
        let mut single_use_reporter = Some(initial_api_version_sender);
        let last_event_ids_for_filter_clone = last_event_ids_for_filter.clone();
        while attempts < self.max_connection_attempts {
            let last_event_ids_for_filter_clone = last_event_ids_for_filter_clone.clone();
            attempts += 1;

            info!(
                "Attempting to connect...\t{}/{}",
                attempts, self.max_connection_attempts
            );

            match self.fetch_api_version_from_status().await {
                Ok(version) => {
                    let new_api_version = version;

                    if let Some(sender) = single_use_reporter.take() {
                        let _ = sender.send(Ok(new_api_version)).await;
                    }

                    // Compare versions to reset attempts in the case that the version changed.
                    // This guards against endlessly retrying when the version hasn't changed, suggesting
                    // that the node is unavailable.
                    // If the connection has been failing and we see the API version change we reset
                    // the attempts as it may have down for an upgrade.
                    if new_api_version != self.api_version {
                        attempts = 0;
                    }

                    self.api_version = new_api_version;
                }
                Err(fetch_err) => {
                    error!(
                        "Error fetching API version (for {}): {fetch_err}",
                        self.node.ip_address
                    );

                    if attempts >= self.max_connection_attempts {
                        return Err(Error::msg(
                            "Unable to retrieve API version from node status",
                        ));
                    }

                    tokio::time::sleep(self.delay_between_attempts).await;
                    continue;
                }
            }

            let filters = filters_from_version(self.api_version);

            let mut connections = HashMap::new();
            let maybe_tasks =
                (!self.allow_partial_connection).then(|| ConnectionTasks::new(filters.len()));

            for filter in filters {
                let last_event_ids_for_filter_clone = last_event_ids_for_filter_clone.clone();
                let guard = last_event_ids_for_filter_clone.lock().await;
                let maybe_event_id = guard.get(&filter).copied();
                drop(guard);
                let bind_address_for_filter = self.filtered_sse_url(&filter)?;
                let connection = ConnectionManager::new(
                    bind_address_for_filter,
                    self.max_connection_attempts,
                    self.sse_event_sender.clone(),
                    maybe_tasks.clone(),
                    self.connection_timeout,
                    maybe_event_id,
                    filter.clone(),
                );

                connections.insert(filter, connection);
            }

            let mut connection_join_handles = Vec::new();

            for (filter, mut connection) in connections.drain() {
                let filter_map_lock_clone = last_event_ids_for_filter_clone.clone();
                debug!("Connecting filter... {}", filter);
                let handle = tokio::spawn(async move {
                    let (filter, maybe_last_event_id, err) = connection.start_handling().await;
                    error!("Error on start_handling: {}", err);
                    let mut guard = filter_map_lock_clone.lock().await;
                    if let Some(event_id) = maybe_last_event_id {
                        guard.insert(filter, event_id);
                    } else {
                        guard.remove(&filter);
                    }
                    drop(guard);
                    err
                });
                connection_join_handles.push(handle);
            }

            if self.allow_partial_connection {
                // Await completion (which would be caused by an error) of all connections OR one of the connection raising NonRecoverableError
                loop {
                    let select_result = futures::future::select_all(connection_join_handles).await;
                    let task_result = select_result.0;
                    let futures_left = select_result.2;
                    if task_result.is_err() {
                        break;
                    }
                    match task_result.unwrap() {
                        ConnectionManagerError::NonRecoverableError { error } => {
                            error!(
                                "Restarting event listener {} because of NonRecoverableError: {} ",
                                self.node.ip_address.to_string(),
                                error
                            );
                            break;
                        }
                        ConnectionManagerError::InitialConnectionError { error } => {
                            //No futures_left means no more filters active, we need to restart the whole listener
                            if futures_left.is_empty() {
                                error!("Restarting event listener {} because of NonRecoverableError: {} ", self.node.ip_address.to_string(), error);
                                break;
                            }
                        }
                    }
                    connection_join_handles = futures_left
                }
            } else {
                // Return on the first completed (which would be caused by an error) connection
                let _ = futures::future::select_all(connection_join_handles).await;
            }
        }
        Ok(())
    }

    fn status_endpoint(&self) -> Result<Url, Error> {
        let status_endpoint_str = format!(
            "http://{}:{}/status",
            self.node.ip_address, self.node.rest_port
        );
        Url::from_str(&status_endpoint_str).map_err(Error::from)
    }

    // Fetch the api version by requesting the status from the node's rest server.
    async fn fetch_api_version_from_status(&mut self) -> Result<ProtocolVersion, Error> {
        debug!(
            "Fetching API version for {}",
            self.status_endpoint().unwrap()
        );

        let status_endpoint = self
            .status_endpoint()
            .context("Should have created status URL")?;

        let status_response = reqwest::get(status_endpoint)
            .await
            .context("Should have responded with status")?;

        // The exact structure of the response varies over the versions but there is an assertion made
        // that .api_version is always valid. So the key is accessed without deserialising the full response.
        let response_json: Value = status_response
            .json()
            .await
            .context("Should have parsed JSON from response")?;

        try_resolve_version(response_json)
    }
}

fn try_resolve_version(raw_response: Value) -> Result<ProtocolVersion, Error> {
    match raw_response.get(BUILD_VERSION_KEY) {
        Some(build_version_value) if build_version_value.is_string() => {
            let raw = build_version_value
                .as_str()
                .context("build_version_value should be a string")?
                .split('-')
                .next()
                .context("splitting build_version_value should always return at least one slice")?;
            ProtocolVersion::from_str(raw)
                .map_err(|error| anyhow!("failed parsing build version from '{}': {}", raw, error))
        }
        _ => Err(anyhow!(
            "failed to get {} from status response {}",
            BUILD_VERSION_KEY,
            raw_response
        )),
    }
}

fn filters_from_version(api_version: ProtocolVersion) -> Vec<Filter> {
    trace!("Getting filters for version...\t{}", api_version);

    // Prior to this the node only had an /events endpoint producing all events.
    let one_three_zero = ProtocolVersion::from_parts(1, 3, 0);
    // From 1.3.X the node had /events/main and /events/sigs
    let one_four_zero = ProtocolVersion::from_parts(1, 4, 0);
    // From 1.4.X the node added /events/deploys (in addition to /events/main and /events/sigs)

    let mut filters = Vec::new();

    if api_version.lt(&one_three_zero) {
        filters.push(Filter::Events);
    } else if api_version.lt(&one_four_zero) {
        filters.push(Filter::Main);
        filters.push(Filter::Sigs);
    } else {
        filters.push(Filter::Main);
        filters.push(Filter::Sigs);
        filters.push(Filter::Deploys);
    }

    filters
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
