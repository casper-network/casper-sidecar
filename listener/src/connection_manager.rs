use super::ConnectionTasks;
use anyhow::Error;
use bytes::Bytes;
use casper_event_types::{
    metrics,
    sse_data::{deserialize, SseData, SseDataDeserializeError},
    Filter,
};
use casper_types::ProtocolVersion;
use eventsource_stream::{Event, EventStream, Eventsource};
use futures::StreamExt;
use reqwest::Client;
use reqwest::Url;
use std::{
    fmt::{self, Debug, Display, Formatter},
    time::Duration,
};
use tokio::{
    select,
    sync::{broadcast::Sender as BroadcastSender, mpsc::Sender},
};
use tokio_stream::Stream;
use tracing::{debug, error, trace, warn};

pub struct SseEvent {
    pub id: u32,
    pub data: SseData,
    pub source: Url,
    pub json_data: Option<String>,
    pub inbound_filter: Filter,
}

impl SseEvent {
    pub fn new(
        id: u32,
        data: SseData,
        mut source: Url,
        json_data: Option<String>,
        inbound_filter: Filter,
    ) -> Self {
        // This is to remove the path e.g. /events/main
        // Leaving just the IP and port
        source.set_path("");
        Self {
            id,
            data,
            source,
            json_data,
            inbound_filter,
        }
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

type DeserializationFn = fn(&str) -> Result<(SseData, bool), SseDataDeserializeError>;

pub(super) struct ConnectionManager {
    bind_address: Url,
    current_event_id: Option<u32>,
    max_attempts: usize,
    delay_between_attempts: Duration,
    sse_event_sender: Sender<SseEvent>,
    maybe_tasks: Option<ConnectionTasks>,
    connection_timeout: Duration,
    filter: Filter,
    deserialization_fn: DeserializationFn,
    current_event_id_sender: Sender<(Filter, u32)>,
    poison_pill_channel: BroadcastSender<()>,
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

fn non_recoverable_error(error: Error) -> ConnectionManagerError {
    ConnectionManagerError::NonRecoverableError { error }
}
fn recoverable_error(error: Error) -> ConnectionManagerError {
    ConnectionManagerError::InitialConnectionError { error }
}
pub struct ConnectionManagerBuilder {
    pub(super) bind_address: Url,
    pub(super) max_attempts: usize,
    pub(super) sse_data_sender: Sender<SseEvent>,
    pub(super) maybe_tasks: Option<ConnectionTasks>,
    pub(super) connection_timeout: Duration,
    pub(super) start_from_event_id: Option<u32>,
    pub(super) filter: Filter,
    pub(super) node_build_version: ProtocolVersion,
    pub(super) current_event_id_sender: Sender<(Filter, u32)>,
    pub(super) poison_pill_channel: BroadcastSender<()>,
}

impl ConnectionManagerBuilder {
    pub(super) fn build(self) -> ConnectionManager {
        trace!("Creating connection manager for: {}", self.bind_address);
        ConnectionManager {
            bind_address: self.bind_address,
            current_event_id: self.start_from_event_id,
            max_attempts: self.max_attempts,
            delay_between_attempts: Duration::from_secs(1),
            sse_event_sender: self.sse_data_sender,
            maybe_tasks: self.maybe_tasks,
            connection_timeout: self.connection_timeout,
            filter: self.filter,
            deserialization_fn: determine_deserializer(self.node_build_version),
            current_event_id_sender: self.current_event_id_sender,
            poison_pill_channel: self.poison_pill_channel,
        }
    }
}

impl ConnectionManager {
    ///This function is blocking, it will return an ConnectionManagerError result if something went wrong while processing.
    pub(super) async fn start_handling(&mut self) -> Result<(), ConnectionManagerError> {
        self.do_start_handling().await
    }

    async fn connect_with_retries(
        &mut self,
    ) -> Result<
        EventStream<impl Stream<Item = reqwest::Result<Bytes>> + Sized>,
        ConnectionManagerError,
    > {
        let mut retry_count = 0;
        let mut last_error = None;
        while retry_count <= self.max_attempts {
            match self.connect().await {
                Ok(event_stream) => return Ok(event_stream),
                Err(ConnectionManagerError::NonRecoverableError { error }) => {
                    return Err(ConnectionManagerError::NonRecoverableError { error })
                }
                Err(err) => last_error = Some(err),
            }
            retry_count += 1;
            tokio::time::sleep(self.delay_between_attempts).await;
        }
        Err(couldnt_connect(
            last_error,
            self.bind_address.clone(),
            self.max_attempts,
        ))
    }

    async fn connect(
        &mut self,
    ) -> Result<
        EventStream<impl Stream<Item = reqwest::Result<Bytes>> + Sized>,
        ConnectionManagerError,
    > {
        let mut bind_address = self.bind_address.clone();

        if let Some(event_id) = self.current_event_id {
            let query = format!("start_from={}", event_id);
            bind_address.set_query(Some(&query))
        }

        debug!("Connecting to node...\t{}", bind_address);
        let client = Client::builder()
            .connect_timeout(self.connection_timeout)
            .build()
            .map_err(|err| recoverable_error(Error::new(err)))?;
        let sse_response = client
            .get(bind_address)
            .send()
            .await
            .map_err(|err| recoverable_error(Error::new(err)))?;
        let event_source = sse_response.bytes_stream().eventsource();
        // Parse the first event to see if the connection was successful
        self.consume_api_version(event_source).await
    }

    async fn do_start_handling(&mut self) -> Result<(), ConnectionManagerError> {
        let event_stream = match self.connect_with_retries().await {
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
        self.handle_stream(event_stream).await
    }

    async fn handle_stream<E, S>(
        &mut self,
        mut event_stream: S,
    ) -> Result<(), ConnectionManagerError>
    where
        E: Debug,
        S: Stream<Item = Result<Event, E>> + Sized + Unpin,
    {
        let mut poison_pill_channel = self.poison_pill_channel.subscribe();
        loop {
            select! {
                maybe_event = event_stream.next() => {
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
                                            count_error("event_without_id");
                                            warn!("Parse Error: {}", parse_error);
                                        }
                                    }
                                }
                                self.handle_event(event)
                                    .await
                                    .map_err(non_recoverable_error)?;
                            }
                            Err(stream_error) => {
                                count_error("fetching_from_stream_failed");
                                let error_message = format!("EventStream Error: {:?}", stream_error);
                                return Err(non_recoverable_error(Error::msg(error_message)));
                            }
                        }
                    } else {
                        return Err(decorate_with_event_stream_closed(self.bind_address.clone()))
                    }
                }
                poison_pill_result = poison_pill_channel.recv() => {
                    return match poison_pill_result {
                        Ok(_) => Ok(()),
                        Err(err) => Err(non_recoverable_error(Error::msg(format!("Error when waiting for force reconnection signal, error: {}", err)))),
                    }
                }
            }
        }
    }

    async fn handle_event(&mut self, event: Event) -> Result<(), Error> {
        match (self.deserialization_fn)(&event.data) {
            Err(serde_error) => {
                count_error("deserialization_error");
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
                    count_error("sending_upstream_failed");
                    Error::msg(
                        "Error when trying to send message in ConnectionManager#handle_event",
                    )
                })?;
            }
        }
        Ok(())
    }

    async fn consume_api_version<S, E>(
        &mut self,
        mut stream: EventStream<S>,
    ) -> Result<EventStream<S>, ConnectionManagerError>
    where
        E: Debug,
        S: Stream<Item = Result<Bytes, E>> + Sized + Unpin,
    {
        match stream.next().await {
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
                                count_error("api_version_sending_upstream_failed");
                                non_recoverable_error(Error::msg(
                                "Error when trying to send message in ConnectionManager#handle_event",
                            ))
                        })?
                        }
                        Ok(_sse_data) => {
                            count_error("api_version_expected");
                            return Err(non_recoverable_error(Error::msg(
                                "When trying to deserialize ApiVersion got other type of message",
                            )));
                        }
                        Err(x) => {
                            count_error("api_version_deserialization_failed");
                            return Err(non_recoverable_error(Error::msg(format!(
                            "Error when trying to deserialize ApiVersion {}. Raw data of event: {}",
                            x, event.data
                        ))));
                        }
                    }
                    Ok(stream)
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

fn couldnt_connect(
    last_error: Option<ConnectionManagerError>,
    url: Url,
    attempts: usize,
) -> ConnectionManagerError {
    let message = format!(
        "Couldn't connect to address {:?} in {:?} attempts",
        url.as_str(),
        attempts
    );
    match last_error {
        None => non_recoverable_error(Error::msg(message)),
        Some(ConnectionManagerError::InitialConnectionError { error }) => {
            ConnectionManagerError::InitialConnectionError {
                error: error.context(message),
            }
        }
        Some(ConnectionManagerError::NonRecoverableError { error }) => {
            ConnectionManagerError::NonRecoverableError {
                error: error.context(message),
            }
        }
    }
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
    //AFAIK the format of messages changed in a backwards-incompatible way in casper-node v1.2.0
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
