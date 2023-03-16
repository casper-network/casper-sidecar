use std::{
    fmt::{self, Debug, Display, Formatter},
    time::Duration,
};

use anyhow::Error;
use bytes::Bytes;
use casper_types::ProtocolVersion;
use eventsource_stream::{Event, EventStream, Eventsource};
use futures::StreamExt;
use reqwest::Client;

use serde_json::Value;
use tokio_stream::Stream;
use tracing::{debug, error, trace, warn};

use reqwest::Url;
use tokio::sync::mpsc::Sender;

use crate::Filter;

use super::ConnectionTasks;
use casper_event_types::sse_data::{deserialize, SseData, SseDataDeserializeError};

pub struct SseEvent {
    pub id: u32,
    pub data: SseData,
    pub source: Url,
    pub json_data: Option<serde_json::Value>,
}

impl SseEvent {
    pub fn new(
        id: u32,
        data: SseData,
        mut source: Url,
        json_data: Option<serde_json::Value>,
    ) -> Self {
        // This is to remove the path e.g. /events/main
        // Leaving just the IP and port
        source.set_path("");
        Self {
            id,
            data,
            source,
            json_data,
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

type DeserializationFn = fn(&str) -> Result<SseData, SseDataDeserializeError>;

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
        }
    }
}

impl ConnectionManager {
    ///This function is blocking, it will return an ConnectionManagerError result if something went wrong while processing.
    pub(super) async fn start_handling(&mut self) -> (Filter, Option<u32>, ConnectionManagerError) {
        let err = match self.do_start_handling().await {
            Ok(_) => non_recoverable_error(Error::msg("Unexpected Ok() from do_start_handling")),
            Err(e) => e,
        };
        (self.filter.clone(), self.current_event_id, err)
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

        let outcome = self.handle_stream(event_stream).await;
        //If we loose connection for some reason we need to go back to the event listener and do
        // the whole handshake process again
        Err(match outcome {
            Ok(_) => decorate_with_event_stream_closed(self.bind_address.clone()),
            Err(err) => err,
        })
    }

    async fn handle_stream<E, S>(
        &mut self,
        mut event_stream: S,
    ) -> Result<(), ConnectionManagerError>
    where
        E: Debug,
        S: Stream<Item = Result<Event, E>> + Sized + Unpin,
    {
        while let Some(event) = event_stream.next().await {
            match event {
                Ok(event) => {
                    match event.id.parse::<u32>() {
                        Ok(id) => self.current_event_id = Some(id),
                        Err(parse_error) => {
                            // ApiVersion events have no ID so parsing "" to u32 will fail.
                            // This gate saves displaying a warning for a trivial error.
                            if !event.data.contains("ApiVersion") {
                                warn!("Parse Error: {}", parse_error);
                            }
                        }
                    }
                    self.handle_event(event)
                        .await
                        .map_err(non_recoverable_error)?;
                }
                Err(stream_error) => {
                    let error_message = format!("EventStream Error: {:?}", stream_error);
                    error!(error_message);
                    return Err(non_recoverable_error(Error::msg(error_message)));
                }
            }
        }
        Ok(())
    }

    async fn handle_event(&mut self, event: Event) -> Result<(), Error> {
        match (self.deserialization_fn)(&event.data) {
            Ok(SseData::Shutdown) => {
                error!("Received Shutdown message ({})", self.bind_address);
            }
            Err(serde_error) => {
                let error_message = format!("Serde Error: {}", serde_error);
                error!(error_message);
                return Err(Error::msg(error_message));
            }
            Ok(sse_data) => {
                let json_data: Value = serde_json::from_str(&event.data)?;
                let sse_event = SseEvent::new(
                    event.id.parse().unwrap_or(0),
                    sse_data,
                    self.bind_address.clone(),
                    Some(json_data),
                );
                self.sse_event_sender.send(sse_event).await.map_err(|_| {
                    Error::msg(
                        "Error when trying to send message in ConnectionManager#handle_event",
                    )
                })?
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
                if event.data.contains("ApiVersion") {
                    match deserialize(&event.data) {
                        //at this point we
                        // are assuming that it's an ApiVersion and ApiVersion is the same across all semvers
                        Ok(SseData::ApiVersion(semver)) => {
                            let sse_event = SseEvent::new(
                                0,
                                SseData::ApiVersion(semver),
                                self.bind_address.clone(),
                                None,
                            );
                            self.sse_event_sender.send(sse_event).await.map_err(|_| {
                                non_recoverable_error(Error::msg(
                                "Error when trying to send message in ConnectionManager#handle_event",
                            ))
                        })?
                        }
                        Ok(_sse_data) => {
                            return Err(non_recoverable_error(Error::msg(
                                "When trying to deserialize ApiVersion got other type of message",
                            )))
                        }
                        Err(x) => {
                            return Err(non_recoverable_error(Error::msg(format!(
                            "Error when trying to deserialize ApiVersion {}. Raw data of event: {}",
                            x, event.data
                        ))))
                        }
                    }
                    Ok(stream)
                } else {
                    Err(expected_first_message_to_be_api_version(event.data))
                }
            }
        }
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

#[cfg(test)]
mod tests {
    use casper_event_types::{
        sse_data::test_support::{example_block_added_1_4_10, BLOCK_HASH_1},
        sse_data_1_0_0::test_support::example_block_added_1_0_0,
    };

    use super::*;

    #[tokio::test]
    async fn given_determine_deserializer_and_1_0_0_should_return_1_0_0_deserializer() {
        let legacy_block_added_raw = example_block_added_1_0_0(BLOCK_HASH_1, "1");
        let new_format_block_added_raw = example_block_added_1_4_10(BLOCK_HASH_1, "1");
        let protocol_version = ProtocolVersion::from_parts(1, 0, 0);
        let deserializer = determine_deserializer(protocol_version);
        let sse_data = (deserializer)(&legacy_block_added_raw).unwrap();

        assert!(matches!(sse_data, SseData::BlockAdded { .. }));
        if let SseData::BlockAdded {
            block_hash: _,
            block,
        } = sse_data.clone()
        {
            assert!(block.proofs.is_empty());
            //let raw = serde_json::to_string(&sse_data);
            assert_eq!(
                sse_data,
                serde_json::from_str::<SseData>(&new_format_block_added_raw).unwrap()
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
        let sse_data = (deserializer)(&legacy_block_added_raw).unwrap();

        assert!(matches!(sse_data, SseData::BlockAdded { .. }));
        if let SseData::BlockAdded {
            block_hash: _,
            block,
        } = sse_data.clone()
        {
            assert!(block.proofs.is_empty());
            let sse_data_1_4_10 =
                serde_json::from_str::<SseData>(&new_format_block_added_raw).unwrap();
            assert_eq!(sse_data, sse_data_1_4_10);
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
        let sse_data = (deserializer)(&block_added_raw).unwrap();

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
        let sse_data = (deserializer)(&block_added_raw).unwrap();

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
