mod connection_manager;
use std::{
    collections::HashMap,
    fmt::{Display, Formatter},
    net::IpAddr,
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Error};
use casper_types::ProtocolVersion;
pub use connection_manager::{ConnectionManager, SseEvent};
use serde_json::Value;
use tokio::sync::{mpsc::Sender, Barrier};
use tracing::{debug, error, info, trace};
use url::Url;
const API_VERSION_KEY: &str = "api_version";

pub struct NodeConnectionInterface {
    pub ip_address: IpAddr,
    pub sse_port: u16,
    pub rest_port: u16,
}

#[derive(Hash, Eq, PartialEq)]
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

        // Wrap the sender in an Option so it can be exhausted with .take() on the initial connection.
        let mut single_use_reporter = Some(initial_api_version_sender);
        let connection_synchronization_timeout = Duration::from_secs(
            ((self.max_connection_attempts) as u64) * self.connection_timeout.as_secs(),
        );
        while attempts < self.max_connection_attempts {
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
            let wait_for_others_to_connect_barrier = Arc::new(Barrier::new(filters.len()));
            let connected_barrier = Arc::new(Barrier::new(filters.len()));

            for filter in filters {
                let mut exhaustible_initial_barrier = None;
                let mut exhaustible_connected_barrier = None;
                if !self.allow_partial_connection {
                    exhaustible_initial_barrier = Some(wait_for_others_to_connect_barrier.clone());
                    exhaustible_connected_barrier = Some(connected_barrier.clone())
                }
                let bind_address_for_filter = self.filtered_sse_url(&filter)?;
                let connection = ConnectionManager::new(
                    bind_address_for_filter,
                    self.max_connection_attempts,
                    self.sse_event_sender.clone(),
                    exhaustible_initial_barrier,
                    exhaustible_connected_barrier,
                    connection_synchronization_timeout,
                    self.connection_timeout,
                );

                connections.insert(filter, connection);
            }

            let mut connection_join_handles = Vec::new();

            for (filter, mut connection) in connections.drain() {
                debug!("Connecting filter... {}", filter);
                let handle = tokio::spawn(async move {
                    let err = connection.start_handling().await;
                    error!("Error on start_handling: {}", err);
                });
                connection_join_handles.push(handle);
            }

            if self.allow_partial_connection {
                // Await completion (which would be caused by an error) of all connections
                futures::future::join_all(connection_join_handles).await;
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

        let maybe_api_version_json = response_json
            .get(API_VERSION_KEY)
            .context("Response should have contained API version")?
            .to_owned();

        serde_json::from_value(maybe_api_version_json).map_err(Error::from)
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
