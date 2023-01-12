use anyhow::{Context, Error};
use casper_event_types::SseData;
use casper_types::ProtocolVersion;
use eventsource_stream::{Event, Eventsource};
use futures::{Stream, StreamExt};
use reqwest::Client;
use serde_json::Value;
use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::net::IpAddr;
use std::str::FromStr;
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use tracing::log::warn;
use tracing::{debug, error, trace};
use url::Url;

const API_VERSION_KEY: &str = "api_version";

pub struct NodeConnectionInterface {
    pub ip_address: IpAddr,
    pub sse_port: u16,
    pub rest_port: u16,
}

pub struct SseEvent {
    id: u32,
    data: SseData,
    source: Url,
}

impl SseEvent {
    fn new(id: u32, data: SseData, mut source: Url) -> Self {
        // This is to remove the path e.g. /events/main
        // Leaving just the IP and port
        source.set_path("");
        Self { id, data, source }
    }

    pub fn id(&self) -> u32 {
        self.id
    }

    pub fn data(&self) -> SseData {
        self.data.clone()
    }

    pub fn source(&self) -> Url {
        self.source.clone()
    }
}

impl Display for SseEvent {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{id: {}, source: {}, event: {:?}}}",
            self.id, self.source, self.data
        )
    }
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
}

impl EventListener {
    pub fn new(
        node: NodeConnectionInterface,
        max_connection_attempts: usize,
        delay_between_attempts: Duration,
        allow_partial_connection: bool,
        sse_event_sender: Sender<SseEvent>,
    ) -> Self {
        Self {
            api_version: ProtocolVersion::from_parts(0, 0, 0),
            node,
            max_connection_attempts,
            delay_between_attempts,
            allow_partial_connection,
            sse_event_sender,
        }
    }

    fn filtered_sse_url(&self, filter: &Filter) -> Result<Url, Error> {
        let url_str = format!(
            "http://{}:{}/{}",
            self.node.ip_address, self.node.sse_port, filter
        );
        Url::parse(&url_str).map_err(Error::from)
    }

    pub async fn stream_aggregated_events(&mut self) -> Result<(), Error> {
        let mut attempts = 0;

        while attempts < self.max_connection_attempts {
            attempts += 1;

            let mut api_fetch_attempts = 0;
            loop {
                if api_fetch_attempts > self.max_connection_attempts {
                    return Err(Error::msg(
                        "Unable to retrieve API version from node status",
                    ));
                }

                let api_version_res = self.fetch_api_version_from_status().await;
                if api_version_res.is_ok() {
                    let new_api_version = api_version_res.unwrap();

                    // Compare versions to reset attempts in the case that the version changed.
                    // This guards against endlessly retrying when the version hasn't changed, suggesting
                    // that the node is unavailable.
                    if new_api_version != self.api_version {
                        attempts = 0;
                    }

                    self.api_version = new_api_version;

                    break;
                }

                error!(
                    "Error fetching API version: {}",
                    api_version_res.err().unwrap()
                );
                tokio::time::sleep(self.delay_between_attempts).await;
                api_fetch_attempts += 1;
            }

            let filters = filters_from_version(self.api_version);

            let mut connections = HashMap::new();

            for filter in filters {
                let bind_address_for_filter = self.filtered_sse_url(&filter)?;

                let connection = ConnectionManager::new(
                    bind_address_for_filter,
                    self.max_connection_attempts,
                    self.sse_event_sender.clone(),
                );

                connections.insert(filter, connection);
            }

            let mut connection_join_handles = Vec::new();

            for (filter, mut connection) in connections.drain() {
                debug!("Connecting filter... {}", filter);
                let handle = tokio::spawn(async move {
                    connection.connect_with_retry().await;
                });
                connection_join_handles.push(handle);
            }

            if self.allow_partial_connection {
                // Await completion of all connections
                futures::future::join_all(connection_join_handles).await;
            } else {
                // Return on the first completed connection
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

struct ConnectionManager {
    bind_address: Url,
    current_event_id: Option<u32>,
    attempts: usize,
    max_attempts: usize,
    delay_between_attempts: Duration,
    sse_event_sender: Sender<SseEvent>,
}

impl ConnectionManager {
    fn new(bind_address: Url, max_attempts: usize, sse_data_sender: Sender<SseEvent>) -> Self {
        trace!("Creating connection manager for: {}", bind_address);

        Self {
            bind_address,
            current_event_id: None,
            attempts: 0,
            max_attempts,
            delay_between_attempts: Duration::from_secs(1),
            sse_event_sender: sse_data_sender,
        }
    }

    fn attempts(&self) -> usize {
        self.attempts
    }

    fn increment_attempts(&mut self) {
        self.attempts += 1;
        trace!(
            "Incrementing attempts...{}/{}",
            self.attempts,
            self.max_attempts
        );
    }

    fn reset_attempts(&mut self) {
        trace!("Resetting attempts...");
        self.attempts = 0;
    }

    async fn connect_with_retry(&mut self) {
        while self.attempts() <= self.max_attempts {
            let mut bind_address = self.bind_address.clone();

            if let Some(event_id) = self.current_event_id {
                let query = format!("start_from={}", event_id);
                bind_address.set_query(Some(&query))
            }

            debug!("Connecting to node...\t{}", bind_address);
            let sse_response = Client::new().get(bind_address).send().await;

            match sse_response {
                Ok(response) => {
                    self.reset_attempts();

                    let event_source = response.bytes_stream().eventsource();

                    self.handle_stream(event_source).await;
                }
                Err(req_err) => {
                    error!("EventStream Error: {}", req_err);
                    self.increment_attempts();
                    tokio::time::sleep(self.delay_between_attempts).await;
                }
            }
        }
    }

    async fn handle_stream<E, S>(&mut self, mut event_stream: S)
    where
        E: Debug,
        S: Stream<Item = Result<Event, E>> + Sized + Unpin,
    {
        while let Some(event) = event_stream.next().await {
            match event {
                Ok(event) => {
                    self.reset_attempts();

                    match event.id.parse::<u32>() {
                        Ok(id) => self.current_event_id = Some(id),
                        Err(parse_error) => {
                            self.increment_attempts();
                            // ApiVersion events have no ID so this may be a trivial error
                            warn!("Parse Error: {}", parse_error);
                        }
                    }

                    self.handle_event(event).await;
                }
                Err(stream_error) => {
                    println!("EventStream Error: {:?}", stream_error);
                    self.increment_attempts();
                }
            }
        }
    }

    async fn handle_event(&mut self, event: Event) {
        match serde_json::from_str::<SseData>(&event.data) {
            Ok(sse_data) => {
                self.reset_attempts();

                let sse_event = SseEvent::new(
                    event.id.parse().unwrap_or(0),
                    sse_data,
                    self.bind_address.clone(),
                );
                let _ = self.sse_event_sender.send(sse_event).await;
            }
            Err(serde_error) => {
                self.increment_attempts();
                error!("Serde Error: {}", serde_error);
            }
        }
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

    // filters

    vec![Filter::Main, Filter::Deploys, Filter::Sigs]
}
