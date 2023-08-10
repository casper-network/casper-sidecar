use super::ConnectionTasks;
use crate::{
    sse_connector::{ConnectToSseUrl, StreamConnector},
    SseEvent,
};
use anyhow::Error;
use casper_event_types::{
    metrics,
    sse_data::{deserialize, SseData, SseDataDeserializeError},
    Filter,
};
use casper_types::ProtocolVersion;
use eventsource_stream::Event;
use reqwest::Url;
use std::{
    fmt::{self, Debug, Display},
    time::Duration,
};
use tokio::{
    select,
    sync::mpsc::{Receiver, Sender},
};
use tokio_util::sync::CancellationToken;
use tracing::{error, trace, warn};

type DeserializationFn = fn(&str) -> Result<(SseData, bool), SseDataDeserializeError>;
const FETCHING_FROM_STREAM_FAILED: &str = "fetching_from_stream_failed";
const DESERIALIZATION_ERROR: &str = "deserialization_error";
const EVENT_WITHOUT_ID: &str = "event_without_id";
const SENDING_FAILED: &str = "sending_downstream_failed";
const API_VERSION_SENDING_FAILED: &str = "api_version_sending_failed";
const API_VERSION_DESERIALIZATION_FAILED: &str = "api_version_deserialization_failed";
const API_VERSION_EXPECTED: &str = "api_version_expected";

/// Implementation of a connection to a single sse endpoint of a node.
pub(super) struct ConnectionManager {
    connector: Box<dyn StreamConnector + Send + Sync>,
    bind_address: Url,
    current_event_id: Option<u32>,
    sse_event_sender: Sender<SseEvent>,
    maybe_tasks: Option<ConnectionTasks>,
    filter: Filter,
    deserialization_fn: DeserializationFn,
    current_event_id_sender: Sender<(Filter, u32)>,
    cancellation_token: CancellationToken,
}

pub enum ConnectionManagerError {
    NonRecoverableError { error: Error },
    InitialConnectionError { error: Error },
}

impl Display for ConnectionManagerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::NonRecoverableError { error } => {
                write!(f, "NonRecoverableError: {}", error)
            }
            Self::InitialConnectionError { error } => {
                write!(f, "InitialConnectionError: {}", error)
            }
        }
    }
}

/// Builder for [ConnectionManager]
pub struct ConnectionManagerBuilder {
    /// Address of the node
    pub(super) bind_address: Url,
    /// Maximum attempts the connection manager will try to (initially) connect.
    /// After connecting, if we loose connection, there will be no reconnects.
    /// Reconnection in this scenario should be handled in EventListener by
    /// creating a new ConnectionManager. This is done intentionally so if
    /// the node goes down to change it's version we can do a full reconnect to
    ///  establish the deserialization protocol.
    pub(super) max_attempts: usize,
    /// Sender to which we will be pushing the data we collected from nodes endpoint
    pub(super) sse_data_sender: Sender<SseEvent>,
    /// Optional synchronisation mechanism between connections. See docs of ConnectionTasks
    pub(super) maybe_tasks: Option<ConnectionTasks>,
    /// Max duration we wait for one connection attempt to succeed
    pub(super) connection_timeout: Duration,
    /// If this is set the connection will connect to the node with query param `&start_from=<start_from_event_id>`
    pub(super) start_from_event_id: Option<u32>,
    /// Nodes filter to which we are connected
    pub(super) filter: Filter,
    /// Build version of the node. It's necessary because after 1.2 non-backwards compatible changes to the event
    /// structure were introduced and we need to apply different deserialization logic
    pub(super) node_build_version: ProtocolVersion,
    /// Channel via which we inform that this filter observed a specific event_id so the ConnectionListener can give
    /// a correct start_from_event_id parameter in case of a connection restart
    pub(super) current_event_id_sender: Sender<(Filter, u32)>,
    /// Cancel token which can force ConnectionManager to halt if external curcomstances require it
    pub(super) cancellation_token: CancellationToken,
}

impl ConnectionManagerBuilder {
    pub(super) fn build(self) -> ConnectionManager {
        trace!("Creating connection manager for: {}", self.bind_address);
        let connector = Box::new(ConnectToSseUrl {
            max_attempts: self.max_attempts,
            delay_between_attempts: Duration::from_secs(1),
            connection_timeout: self.connection_timeout,
        });
        ConnectionManager {
            connector,
            bind_address: self.bind_address,
            current_event_id: self.start_from_event_id,
            sse_event_sender: self.sse_data_sender,
            maybe_tasks: self.maybe_tasks,
            filter: self.filter,
            deserialization_fn: determine_deserializer(self.node_build_version),
            current_event_id_sender: self.current_event_id_sender,
            cancellation_token: self.cancellation_token,
        }
    }
}

impl ConnectionManager {
    /// Start handling traffic from nodes endpoint. This function is blocking, it will return a
    /// ConnectionManagerError result if something went wrong while processing.
    pub(super) async fn start_handling(&mut self) -> Result<(), ConnectionManagerError> {
        self.do_start_handling().await
    }

    async fn connect(&mut self) -> Result<Receiver<Result<Event, Error>>, ConnectionManagerError> {
        let mut bind_address = self.bind_address.clone();

        if let Some(event_id) = self.current_event_id {
            let query = format!("start_from={}", event_id);
            bind_address.set_query(Some(&query))
        }
        let maybe_event_tx = self.connector.connect(bind_address).await;
        if let Ok(event_tx) = maybe_event_tx {
            self.consume_api_version(event_tx).await
        } else {
            maybe_event_tx
        }
    }

    async fn do_start_handling(&mut self) -> Result<(), ConnectionManagerError> {
        let receiver = match self.connect().await {
            Ok(stream) => {
                if let Some(tasks) = &self.maybe_tasks {
                    tasks.register_success()
                };
                stream
            }
            Err(error) => {
                if let Some(tasks) = &self.maybe_tasks {
                    tasks.register_failure()
                };
                return Err(error);
            }
        };

        if let Some(tasks) = &self.maybe_tasks {
            if !tasks.wait().await {
                let error = Error::msg(format!(
                    "Failed to connect to all filters. Disconnecting from {}",
                    self.bind_address
                ));
                return Err(non_recoverable_error(error));
            }
        }
        //If we loose connection for some reason we need to go back to the event listener and do
        // the whole handshake process again. The Ok branch here means that we need to do a force restart of connection manager
        self.handle_stream(receiver).await
    }

    async fn handle_stream<E>(
        &mut self,
        mut receiver: Receiver<Result<Event, E>>,
    ) -> Result<(), ConnectionManagerError>
    where
        E: Debug,
    {
        loop {
            select! {
                maybe_event = receiver.recv() => {
                    if let Some(event) = maybe_event {
                        match event {
                            Ok(event) => {
                                match event.id.parse::<u32>() {
                                    Ok(id) => {
                                        self.current_event_id = Some(id);
                                        self.current_event_id_sender.send((self.filter.clone(), id)).await.map_err(|err| non_recoverable_error(Error::msg(format!("Error when trying to report observed event id {}", err))))?;
                                    },
                                    Err(parse_error) => {
                                        // ApiVersion events have no ID so parsing "" to u32 will fail.
                                        // This gate saves displaying a warning for a trivial error.
                                        if !event.data.contains("ApiVersion") {
                                            count_error(EVENT_WITHOUT_ID);
                                            warn!("Parse Error: {}", parse_error);
                                        }
                                    }
                                }
                                self.handle_event(event)
                                    .await
                                    .map_err(non_recoverable_error)?;
                            }
                            Err(stream_error) => {
                                count_error(FETCHING_FROM_STREAM_FAILED);
                                let error_message = format!("EventStream Error: {:?}", stream_error);
                                return Err(non_recoverable_error(Error::msg(error_message)));
                            }
                        }
                    } else {
                        return Err(decorate_with_event_stream_closed(self.bind_address.clone()))
                    }
                }
                _ = self.cancellation_token.cancelled() => {
                    count_internal_event("connection_manager", "cancellation");
                    return Err(non_recoverable_error(Error::msg(format!("Detected sse connection inactivity on filter {}", self.bind_address.as_str()))))
                }
            }
        }
    }

    async fn handle_event(&mut self, event: Event) -> Result<(), Error> {
        match (self.deserialization_fn)(&event.data) {
            Err(serde_error) => {
                count_error(DESERIALIZATION_ERROR);
                let error_message = format!("Serde Error: {}", serde_error);
                error!(error_message);
                return Err(Error::msg(error_message));
            }
            Ok((sse_data, needs_raw_json)) => {
                let payload_size = event.data.len();
                let mut raw_json_data = None;
                if needs_raw_json {
                    raw_json_data = Some(event.data);
                }
                self.observe_bytes(payload_size);
                let sse_event = SseEvent::new(
                    event.id.parse().unwrap_or(0),
                    sse_data,
                    self.bind_address.clone(),
                    raw_json_data,
                    self.filter.clone(),
                );
                self.sse_event_sender.send(sse_event).await.map_err(|_| {
                    count_error(SENDING_FAILED);
                    Error::msg(
                        "Error when trying to send message in ConnectionManager#handle_event",
                    )
                })?;
            }
        }
        Ok(())
    }

    async fn consume_api_version<E>(
        &mut self,
        mut receiver: Receiver<Result<Event, E>>,
    ) -> Result<Receiver<Result<Event, E>>, ConnectionManagerError>
    where
        E: Debug,
    {
        // We want to see if the first message got from a connection is ApiVersion. That is the protocols guarantee.
        // If it's not - something went very wrong and we shouldn't consider this connection valid
        match receiver.recv().await {
            None => Err(recoverable_error(Error::msg("First event was empty"))),
            Some(Err(error)) => Err(failed_to_get_first_event(error)),
            Some(Ok(event)) => {
                let payload_size = event.data.len();
                self.observe_bytes(payload_size);
                if event.data.contains("ApiVersion") {
                    match deserialize(&event.data) {
                        //at this point we
                        // are assuming that it's an ApiVersion and ApiVersion is the same across all semvers
                        Ok((SseData::ApiVersion(semver), _)) => {
                            let sse_event = SseEvent::new(
                                0,
                                SseData::ApiVersion(semver),
                                self.bind_address.clone(),
                                None,
                                self.filter.clone(),
                            );
                            self.sse_event_sender.send(sse_event).await.map_err(|_| {
                                count_error(API_VERSION_SENDING_FAILED);
                                non_recoverable_error(Error::msg(
                                "Error when trying to send message in ConnectionManager#handle_event",
                            ))
                        })?
                        }
                        Ok(_sse_data) => {
                            count_error(API_VERSION_EXPECTED);
                            return Err(non_recoverable_error(Error::msg(
                                "When trying to deserialize ApiVersion got other type of message",
                            )));
                        }
                        Err(x) => {
                            count_error(API_VERSION_DESERIALIZATION_FAILED);
                            return Err(non_recoverable_error(Error::msg(format!(
                            "Error when trying to deserialize ApiVersion {}. Raw data of event: {}",
                            x, event.data
                        ))));
                        }
                    }
                    Ok(receiver)
                } else {
                    Err(expected_first_message_to_be_api_version(event.data))
                }
            }
        }
    }

    fn observe_bytes(&self, payload_size: usize) {
        metrics::RECEIVED_BYTES
            .with_label_values(&[self.filter.to_string().as_str()])
            .observe(payload_size as f64);
    }
}

fn count_internal_event(category: &str, reason: &str) {
    metrics::INTERNAL_EVENTS
        .with_label_values(&[category, reason])
        .inc();
}

pub fn non_recoverable_error(error: Error) -> ConnectionManagerError {
    ConnectionManagerError::NonRecoverableError { error }
}

pub fn recoverable_error(error: Error) -> ConnectionManagerError {
    ConnectionManagerError::InitialConnectionError { error }
}

fn decorate_with_event_stream_closed(address: Url) -> ConnectionManagerError {
    let message = format!("Event stream closed for filter: {:?}", address.as_str());
    ConnectionManagerError::NonRecoverableError {
        error: Error::msg(message),
    }
}

fn failed_to_get_first_event<T>(error: T) -> ConnectionManagerError
where
    T: Debug,
{
    non_recoverable_error(Error::msg(format!(
        "failed to get first event: {:?}",
        error
    )))
}

fn expected_first_message_to_be_api_version(data: String) -> ConnectionManagerError {
    non_recoverable_error(Error::msg(format!(
        "Expected first message to be ApiVersion, got: {:?}",
        data
    )))
}

fn determine_deserializer(node_build_version: ProtocolVersion) -> DeserializationFn {
    let one_two_zero = ProtocolVersion::from_parts(1, 2, 0);
    if node_build_version.lt(&one_two_zero) {
        casper_event_types::sse_data_1_0_0::deserialize
    } else {
        deserialize
    }
}

fn count_error(reason: &str) {
    metrics::ERROR_COUNTS
        .with_label_values(&["connection_manager", reason])
        .inc();
}

#[cfg(test)]
mod tests {
    use crate::{
        connection_manager::{ConnectionManager, ConnectionManagerError},
        sse_connector::{MockSseConnection, StreamConnector},
        SseEvent,
    };
    use casper_event_types::{sse_data::test_support::*, sse_data_1_0_0::test_support::*, Filter};
    use tokio::sync::mpsc::{channel, Receiver};
    use tokio_util::sync::CancellationToken;
    use url::Url;

    #[tokio::test]
    async fn given_connection_fail_should_return_error() {
        let connector = Box::new(MockSseConnection::build_failing_on_connection());
        let (mut connection_manager, _, _) = build_manager(connector);
        let res = connection_manager.do_start_handling().await;
        if let Err(ConnectionManagerError::NonRecoverableError { error }) = res {
            assert_eq!(error.to_string(), "Some error on connection");
        } else {
            unreachable!();
        }
    }

    #[tokio::test]
    async fn given_failure_on_message_should_return_error() {
        let connector = Box::new(MockSseConnection::build_failing_on_message());
        let (mut connection_manager, _, _) = build_manager(connector);
        let res = connection_manager.do_start_handling().await;
        if let Err(ConnectionManagerError::InitialConnectionError { error }) = res {
            assert_eq!(error.to_string(), "First event was empty");
        } else {
            unreachable!();
        }
    }

    #[tokio::test]
    async fn given_data_without_api_version_should_fail() {
        let data = vec![
            example_block_added_1_0_0(BLOCK_HASH_1, "1"),
            example_block_added_1_0_0(BLOCK_HASH_2, "2"),
        ];
        let connector = Box::new(MockSseConnection::build_with_data(data));
        let (mut connection_manager, _, _) = build_manager(connector);
        let res = connection_manager.do_start_handling().await;
        if let Err(ConnectionManagerError::NonRecoverableError { error }) = res {
            assert!(error
                .to_string()
                .starts_with("Expected first message to be ApiVersion"));
        } else {
            unreachable!();
        }
    }

    #[tokio::test]
    async fn given_data_should_pass_data() {
        let data = vec![
            example_api_version(),
            example_block_added_1_0_0(BLOCK_HASH_1, "1"),
            example_block_added_1_0_0(BLOCK_HASH_2, "2"),
        ];
        let connector = Box::new(MockSseConnection::build_with_data(data));
        let (mut connection_manager, data_tx, event_ids) = build_manager(connector);
        let events_join = tokio::spawn(async move { poll_events(data_tx).await });
        let event_ids_join = tokio::spawn(async move { poll_events(event_ids).await });
        tokio::spawn(async move { connection_manager.do_start_handling().await });
        let events = events_join.await.unwrap();
        assert_eq!(events.len(), 3);
        let event_ids = event_ids_join.await.unwrap();
        assert_eq!(event_ids.len(), 2);
    }

    #[tokio::test]
    async fn given_data_containing_non_deserializable_data_should_fail_on_that_message() {
        let data = vec![
            example_api_version(),
            "XYZ".to_string(),
            example_block_added_1_0_0(BLOCK_HASH_2, "2"),
        ];
        let connector = Box::new(MockSseConnection::build_with_data(data));
        let (mut connection_manager, data_tx, _event_ids) = build_manager(connector);
        let events_join = tokio::spawn(async move { poll_events(data_tx).await });
        let connection_manager_joiner =
            tokio::spawn(async move { connection_manager.do_start_handling().await });
        let events = events_join.await.unwrap();
        assert_eq!(events.len(), 1);
        let res = connection_manager_joiner.await;
        if let Ok(Err(ConnectionManagerError::NonRecoverableError { error })) = res {
            assert_eq!(
                error.to_string(),
                "Serde Error: Couldn't deserialize Serde Error: expected value at line 1 column 1"
            )
        } else {
            unreachable!();
        }
    }

    pub async fn poll_events<T>(mut receiver: Receiver<T>) -> Vec<T> {
        let mut events_received = Vec::new();
        while let Some(event) = receiver.recv().await {
            events_received.push(event);
        }
        events_received
    }

    fn build_manager(
        connector: Box<dyn StreamConnector + Send + Sync>,
    ) -> (
        ConnectionManager,
        Receiver<SseEvent>,
        Receiver<(Filter, u32)>,
    ) {
        let bind_address = Url::parse("http://localhost:123").unwrap();
        let (data_tx, data_rx) = channel(100);
        let (event_id_tx, event_id_rx) = channel(100);
        let cancellation_token = CancellationToken::new();
        let manager = ConnectionManager {
            connector,
            bind_address,
            current_event_id: None,
            sse_event_sender: data_tx,
            maybe_tasks: None,
            filter: Filter::Sigs,
            deserialization_fn: casper_event_types::sse_data_1_0_0::deserialize,
            current_event_id_sender: event_id_tx,
            cancellation_token,
        };
        (manager, data_rx, event_id_rx)
    }
}

#[cfg(test)]
mod deserialization_tests {
    use casper_event_types::{
        sse_data::test_support::{example_block_added_1_4_10, BLOCK_HASH_1},
        sse_data_1_0_0::test_support::example_block_added_1_0_0,
    };
    use serde_json::Value;

    use super::*;

    #[tokio::test]
    async fn given_determine_deserializer_and_1_0_0_should_return_1_0_0_deserializer() {
        let legacy_block_added_raw = example_block_added_1_0_0(BLOCK_HASH_1, "1");
        let new_format_block_added_raw = example_block_added_1_4_10(BLOCK_HASH_1, "1");
        let protocol_version = ProtocolVersion::from_parts(1, 0, 0);
        let deserializer = determine_deserializer(protocol_version);
        let tuple = (deserializer)(&legacy_block_added_raw).unwrap();
        let sse_data = tuple.0;
        assert!(tuple.1);

        assert!(matches!(sse_data, SseData::BlockAdded { .. }));
        if let SseData::BlockAdded {
            block_hash: _,
            block,
        } = sse_data.clone()
        {
            assert!(block.proofs.is_empty());
            assert_eq!(
                serde_json::to_value(sse_data).unwrap(),
                serde_json::from_str::<Value>(&new_format_block_added_raw).unwrap()
            );
        }
    }

    #[tokio::test]
    async fn given_determine_deserializer_and_1_1_0_should_return_generic_deserializer_which_fails_on_contemporary_block_added(
    ) {
        let new_format_block_added_raw = example_block_added_1_4_10(BLOCK_HASH_1, "1");
        let protocol_version = ProtocolVersion::from_parts(1, 1, 0);
        let deserializer = determine_deserializer(protocol_version);
        let result = (deserializer)(&new_format_block_added_raw);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn given_determine_deserializer_and_1_1_0_should_return_generic_deserializer_which_deserializes_legacy_block_added(
    ) {
        let legacy_block_added_raw = example_block_added_1_0_0(BLOCK_HASH_1, "1");
        let new_format_block_added_raw = example_block_added_1_4_10(BLOCK_HASH_1, "1");
        let protocol_version = ProtocolVersion::from_parts(1, 1, 0);
        let deserializer = determine_deserializer(protocol_version);
        let tuple = (deserializer)(&legacy_block_added_raw).unwrap();
        let sse_data = tuple.0;
        assert!(tuple.1);

        assert!(matches!(sse_data, SseData::BlockAdded { .. }));
        if let SseData::BlockAdded {
            block_hash: _,
            block,
        } = sse_data.clone()
        {
            assert!(block.proofs.is_empty());
            let sse_data_1_4_10 =
                serde_json::from_str::<Value>(&new_format_block_added_raw).unwrap();
            assert_eq!(serde_json::to_value(sse_data).unwrap(), sse_data_1_4_10);
        }
    }

    #[tokio::test]
    async fn given_determine_deserializer_and_1_2_0_should_return_generic_deserializer_which_fails_on_legacy_block_added(
    ) {
        let legacy_block_added_raw = example_block_added_1_0_0(BLOCK_HASH_1, "1");
        let protocol_version = ProtocolVersion::from_parts(1, 2, 0);
        let deserializer = determine_deserializer(protocol_version);
        let result = (deserializer)(&legacy_block_added_raw);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn given_determine_deserializer_and_1_2_0_should_deserialize_contemporary_block_added_payload(
    ) {
        let block_added_raw = example_block_added_1_4_10(BLOCK_HASH_1, "1");
        let protocol_version = ProtocolVersion::from_parts(1, 2, 0);
        let deserializer = determine_deserializer(protocol_version);
        let tuple = (deserializer)(&block_added_raw).unwrap();
        let sse_data = tuple.0;
        assert!(!tuple.1);

        assert!(matches!(sse_data, SseData::BlockAdded { .. }));
        if let SseData::BlockAdded {
            block_hash: _,
            block,
        } = sse_data.clone()
        {
            assert!(block.proofs.is_empty());
            let raw = serde_json::to_string(&sse_data);
            assert_eq!(raw.unwrap(), block_added_raw);
        }
    }

    #[tokio::test]
    async fn given_determine_deserializer_and_1_4_10_should_return_generic_deserializer_which_fails_on_legacy_block_added(
    ) {
        let block_added_raw = example_block_added_1_0_0(BLOCK_HASH_1, "1");
        let protocol_version = ProtocolVersion::from_parts(1, 2, 0);
        let deserializer = determine_deserializer(protocol_version);
        let result = (deserializer)(&block_added_raw);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn given_determine_deserializer_and_1_4_10_should_deserialize_contemporary_block_added_payload(
    ) {
        let block_added_raw = example_block_added_1_4_10(BLOCK_HASH_1, "1");
        let protocol_version = ProtocolVersion::from_parts(1, 4, 10);
        let deserializer = determine_deserializer(protocol_version);
        let tuple = (deserializer)(&block_added_raw).unwrap();
        let sse_data = tuple.0;
        assert!(!tuple.1);

        assert!(matches!(sse_data, SseData::BlockAdded { .. }));
        if let SseData::BlockAdded {
            block_hash: _,
            block,
        } = sse_data.clone()
        {
            assert!(block.proofs.is_empty());
            let raw = serde_json::to_string(&sse_data);
            assert_eq!(raw.unwrap(), block_added_raw);
        }
    }
}
