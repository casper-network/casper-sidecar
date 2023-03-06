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

use super::ConnectionTasks;
use casper_event_types::sse_data::{deserialize, SseData};

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

pub(super) struct ConnectionManager {
    bind_address: Url,
    current_event_id: Option<u32>,
    attempts: usize,
    max_attempts: usize,
    delay_between_attempts: Duration,
    sse_event_sender: Sender<SseEvent>,
    maybe_tasks: Option<ConnectionTasks>,
    connection_timeout: Duration,
    api_version: ProtocolVersion,
    deserialization_fn: fn(&str) -> Result<SseData, Error>,
}

pub enum ConnectionManagerError {
    ProtocolChangedError {
        endpoint: String,
        old: ProtocolVersion,
        new: ProtocolVersion,
    },
    OtherError {
        error: Error,
    },
}

impl Display for ConnectionManagerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::OtherError { error } => {
                write!(f, "ConnectionManagerError: {}", error)
            }
            Self::ProtocolChangedError { endpoint, old, new } => {
                write!(f, "ProtocolChangedError: endpoint {} got an ApiVersion message that changed the protocol from {} to {}", endpoint, old, new)
            }
        }
    }
}

impl ConnectionManagerError {
    fn to_other_error(error: Error) -> ConnectionManagerError {
        ConnectionManagerError::OtherError { error }
    }
}

impl ConnectionManager {
    pub(super) fn new(
        bind_address: Url,
        max_attempts: usize,
        sse_data_sender: Sender<SseEvent>,
        maybe_tasks: Option<ConnectionTasks>,
        connection_timeout: Duration,
        api_version: ProtocolVersion,
    ) -> Self {
        trace!("Creating connection manager for: {}", bind_address);
        let deserialization_fn = determine_deserializer(api_version);

        Self {
            bind_address,
            current_event_id: None,
            attempts: 0,
            max_attempts,
            delay_between_attempts: Duration::from_secs(1),
            sse_event_sender: sse_data_sender,
            maybe_tasks,
            connection_timeout,
            api_version,
            deserialization_fn,
        }
    }

    ///This function is blocking, it will return an ConnectionManagerError result if something went wrong while processing.
    pub(super) async fn start_handling(&mut self) -> ConnectionManagerError {
        match self.do_start_handling().await {
            Ok(_) => ConnectionManagerError::to_other_error(Error::msg(
                "Unexpected Ok() from do_start_handling",
            )),
            Err(e) => e,
        }
    }

    fn increment_attempts(&mut self) -> bool {
        self.attempts += 1;
        if self.attempts >= self.max_attempts {
            warn!(
                "Max attempts reached whilst incrementing attempts... {}/{}",
                self.attempts, self.max_attempts
            );
            false
        } else {
            trace!(
                "Incrementing attempts...{}/{}",
                self.attempts,
                self.max_attempts
            );
            true
        }
    }

    fn reset_attempts(&mut self) {
        trace!("Resetting attempts...");
        self.attempts = 0;
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
                Err(ConnectionManagerError::ProtocolChangedError { endpoint, old, new }) => {
                    return Err(ConnectionManagerError::ProtocolChangedError { endpoint, old, new })
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
            .map_err(|err| ConnectionManagerError::to_other_error(Error::new(err)))?;
        let sse_response = client
            .get(bind_address)
            .send()
            .await
            .map_err(|err| ConnectionManagerError::to_other_error(Error::new(err)))?;
        let event_source = sse_response.bytes_stream().eventsource();
        // Parse the first event to see if the connection was successful
        self.consume_api_version(event_source).await
    }

    async fn do_start_handling(&mut self) -> Result<(), ConnectionManagerError> {
        let mut event_stream = match self.connect_with_retries().await {
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
                return Err(ConnectionManagerError::to_other_error(error));
            }
        }

        loop {
            let outcome = self.handle_stream(event_stream).await;
            let error = match outcome {
                Ok(_) => {
                    return Err(decorate_with_event_stream_closed(self.bind_address.clone()));
                }
                Err(ConnectionManagerError::ProtocolChangedError { endpoint, old, new }) => {
                    //This error can't be retried, we need to restart the whole event listener
                    return Err(ConnectionManagerError::ProtocolChangedError {
                        endpoint,
                        old,
                        new,
                    });
                }
                Err(ConnectionManagerError::OtherError { error }) => error,
            };
            if !self.increment_attempts() {
                // We are counting these attempts in case there is some weird scenario in which we are
                // able to connect to the nodes filter, but we are unable to understand the events from it
                return Err(decorate_with_error_handling_stream(
                    error,
                    self.bind_address.clone(),
                ));
            }
            event_stream = self.connect_with_retries().await?;
            self.reset_attempts();
        }
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
                            if event.data.contains("ApiVersion") {
                                self.validate_api_version(&event.data)?;
                            } else {
                                warn!("Parse Error: {}", parse_error);
                            }
                        }
                    }
                    self.handle_event(event)
                        .await
                        .map_err(ConnectionManagerError::to_other_error)?;
                }
                Err(stream_error) => {
                    let error_message = format!("EventStream Error: {:?}", stream_error);
                    error!(error_message);
                    return Err(ConnectionManagerError::to_other_error(Error::msg(
                        error_message,
                    )));
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
                self.reset_attempts();
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
        &self,
        mut stream: EventStream<S>,
    ) -> Result<EventStream<S>, ConnectionManagerError>
    where
        E: Debug,
        S: Stream<Item = Result<Bytes, E>> + Sized + Unpin,
    {
        match stream.next().await {
            None => Err(ConnectionManagerError::to_other_error(Error::msg(
                "First event was empty",
            ))),
            Some(Err(error)) => Err(failed_to_get_first_event(error)),
            Some(Ok(event)) => {
                if event.data.contains("ApiVersion") {
                    self.validate_api_version(&event.data)?;
                    match (self.deserialization_fn)(&event.data) {
                        Ok(SseData::ApiVersion(semver)) => {
                            let sse_event = SseEvent::new(
                                0,
                                SseData::ApiVersion(semver),
                                self.bind_address.clone(),
                                None,
                            );
                            self.sse_event_sender.send(sse_event).await.map_err(|_| {
                            ConnectionManagerError::to_other_error(Error::msg(
                                "Error when trying to send message in ConnectionManager#handle_event",
                            ))
                        })?
                        }
                        Ok(_sse_data) => {
                            return Err(ConnectionManagerError::to_other_error(Error::msg(
                                "When trying to deserialize ApiVersion got other type of message",
                            )))
                        }
                        Err(x) => {
                            return Err(ConnectionManagerError::to_other_error(Error::msg(
                                format!("Error when trying to deserialize ApiVersion {}", x),
                            )))
                        }
                    }

                    Ok(stream)
                } else {
                    Err(expected_first_message_to_be_api_version(event.data))
                }
            }
        }
    }

    fn validate_api_version(&self, data: &str) -> Result<(), ConnectionManagerError> {
        let sse_data = serde_json::from_str::<SseData>(data).map_err(|serde_error| {
            ConnectionManagerError::to_other_error(Error::msg(serde_error).context("Serde Error"))
        })?;
        match sse_data {
            SseData::ApiVersion(semver) if !semver.eq(&self.api_version) => {
                Err(ConnectionManagerError::ProtocolChangedError {
                    endpoint: self.bind_address.to_string(),
                    old: self.api_version,
                    new: semver,
                })
            }
            SseData::ApiVersion(..) => Ok(()),
            _ => {
                let err = Error::msg("Received a message that contains \"ApiVersion\", but is not an ApiVersion message...");
                Err(ConnectionManagerError::to_other_error(err))
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
        None => ConnectionManagerError::to_other_error(Error::msg(message)),
        Some(ConnectionManagerError::OtherError { error }) => ConnectionManagerError::OtherError {
            error: error.context(message),
        },
        Some(x) => x, //This shouldn't happen in a realistic scenario
    }
}

fn decorate_with_event_stream_closed(address: Url) -> ConnectionManagerError {
    let message = format!("Event stream closed for filter: {:?}", address.as_str());
    ConnectionManagerError::OtherError {
        error: Error::msg(message),
    }
}

fn decorate_with_error_handling_stream(error: Error, address: Url) -> ConnectionManagerError {
    let message = format!(
        "Error while handling event stream for filter {:?}",
        address.as_str()
    );
    ConnectionManagerError::OtherError {
        error: error.context(message),
    }
}

fn failed_to_get_first_event<T>(error: T) -> ConnectionManagerError
where
    T: Debug,
{
    ConnectionManagerError::to_other_error(Error::msg(format!(
        "failed to get first event: {:?}",
        error
    )))
}

fn expected_first_message_to_be_api_version(data: String) -> ConnectionManagerError {
    ConnectionManagerError::to_other_error(Error::msg(format!(
        "Expected first message to be ApiVersion, got: {:?}",
        data
    )))
}

fn determine_deserializer(api_version: ProtocolVersion) -> fn(&str) -> Result<SseData, Error> {
    let one_zero_zero = ProtocolVersion::from_parts(1, 0, 0);
    if api_version.eq(&one_zero_zero) {
        casper_event_types::sse_data_1_0_0::deserialize
    } else {
        deserialize
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    macro_rules! is_of_var {
        ($val:ident, $var:path) => {
            match $val {
                $var { .. } => true,
                _ => false,
            }
        };
    }

    #[tokio::test]
    async fn given_determine_deserializer_and_1_0_0_should_return_1_0_0_deserializer() {
        let legacy_block_added_raw = "{\"BlockAdded\":{\"block\":{\"body\":{\"deploy_hashes\": [], \"proposer\": \"0190c434129ecbaeb34d33185ab6bf97c3c493fc50121a56a9ed8c4c52855b5ac1\", \"transfer_hashes\": []}, \"hash\": \"e871418f5d47424514b385f48cf1b56c2e7309fbe47746d396b573111a6c9cd9\", \"header\":{\"accumulated_seed\": \"32f33577d757609af354f4e18f21af684ae793fe9469388aa605a75bad9dc86b\", \"body_hash\": \"43f3da1d596220f2d7202ca222c3c11f1ec100102e7fa0bee1e8e40f8ed4497c\", \"era_end\":{\"era_report\":{\"equivocators\": [], \"inactive_validators\": [], \"rewards\":{\"01026ca707c348ed8012ac6a1f28db031fadd6eb67203501a353b867a08c8b9a80\": 1559249876159, \"010427c1d1227c9d2aafe8c06c6e6b276da8dcd8fd170ca848b8e3e8e1038a6dc8\": 25892675444}}, \"next_era_validator_weights\":{\"01026ca707c348ed8012ac6a1f28db031fadd6eb67203501a353b867a08c8b9a80\": \"50359181028146696\", \"010427c1d1227c9d2aafe8c06c6e6b276da8dcd8fd170ca848b8e3e8e1038a6dc8\": \"836257197475108\"}}, \"era_id\": 18, \"height\": 2075, \"parent_hash\": \"d2e919792d752bc58ca3df1351e3d569cebf6bf77470475ab8321d6b2abb7ad4\", \"protocol_version\": \"1.0.0\", \"random_bit\": true, \"state_root_hash\": \"442e82950da702cc47460b318e43ace0cd3345fe2288280d304409adebb3744d\", \"timestamp\": \"2021-04-02T05:03:29.792Z\"}, \"proofs\": []}, \"block_hash\": \"e871418f5d47424514b385f48cf1b56c2e7309fbe47746d396b573111a6c9cd9\"}}".to_string();
        let new_format_block_added_raw = "{\"BlockAdded\":{\"block_hash\":\"e871418f5d47424514b385f48cf1b56c2e7309fbe47746d396b573111a6c9cd9\",\"block\":{\"hash\":\"e871418f5d47424514b385f48cf1b56c2e7309fbe47746d396b573111a6c9cd9\",\"header\":{\"parent_hash\":\"d2e919792d752bc58ca3df1351e3d569cebf6bf77470475ab8321d6b2abb7ad4\",\"state_root_hash\":\"442e82950da702cc47460b318e43ace0cd3345fe2288280d304409adebb3744d\",\"body_hash\":\"43f3da1d596220f2d7202ca222c3c11f1ec100102e7fa0bee1e8e40f8ed4497c\",\"random_bit\":true,\"accumulated_seed\":\"32f33577d757609af354f4e18f21af684ae793fe9469388aa605a75bad9dc86b\",\"era_end\":{\"era_report\":{\"equivocators\":[],\"rewards\":[{\"validator\":\"01026ca707c348ed8012ac6a1f28db031fadd6eb67203501a353b867a08c8b9a80\",\"amount\":1559249876159},{\"validator\":\"010427c1d1227c9d2aafe8c06c6e6b276da8dcd8fd170ca848b8e3e8e1038a6dc8\",\"amount\":25892675444}],\"inactive_validators\":[]},\"next_era_validator_weights\":[{\"validator\":\"01026ca707c348ed8012ac6a1f28db031fadd6eb67203501a353b867a08c8b9a80\",\"weight\":\"50359181028146696\"},{\"validator\":\"010427c1d1227c9d2aafe8c06c6e6b276da8dcd8fd170ca848b8e3e8e1038a6dc8\",\"weight\":\"836257197475108\"}]},\"timestamp\":\"2021-04-02T05:03:29.792Z\",\"era_id\":18,\"height\":2075,\"protocol_version\":\"1.0.0\"},\"body\":{\"proposer\":\"0190c434129ecbaeb34d33185ab6bf97c3c493fc50121a56a9ed8c4c52855b5ac1\",\"deploy_hashes\":[],\"transfer_hashes\":[]},\"proofs\":[]}}}".to_string();
        let protocol_version = ProtocolVersion::from_parts(1, 0, 0);
        let deserializer = determine_deserializer(protocol_version);
        let sse_data = (deserializer)(&legacy_block_added_raw).unwrap();

        assert!(is_of_var!(sse_data, SseData::BlockAdded));
        if let SseData::BlockAdded {
            block_hash: _,
            block,
        } = sse_data.clone()
        {
            assert!(block.proofs.is_empty());
            let raw = serde_json::to_string(&sse_data);
            assert_eq!(raw.unwrap(), new_format_block_added_raw);
        }
    }

    #[tokio::test]
    async fn given_determine_deserializer_and_1_1_0_should_return_generic_deserializer_which_fails_on_legacy_block_added(
    ) {
        let legacy_block_added_raw = "{\"BlockAdded\":{\"block\":{\"body\":{\"deploy_hashes\": [], \"proposer\": \"0190c434129ecbaeb34d33185ab6bf97c3c493fc50121a56a9ed8c4c52855b5ac1\", \"transfer_hashes\": []}, \"hash\": \"e871418f5d47424514b385f48cf1b56c2e7309fbe47746d396b573111a6c9cd9\", \"header\":{\"accumulated_seed\": \"32f33577d757609af354f4e18f21af684ae793fe9469388aa605a75bad9dc86b\", \"body_hash\": \"43f3da1d596220f2d7202ca222c3c11f1ec100102e7fa0bee1e8e40f8ed4497c\", \"era_end\":{\"era_report\":{\"equivocators\": [], \"inactive_validators\": [], \"rewards\":{\"01026ca707c348ed8012ac6a1f28db031fadd6eb67203501a353b867a08c8b9a80\": 1559249876159, \"010427c1d1227c9d2aafe8c06c6e6b276da8dcd8fd170ca848b8e3e8e1038a6dc8\": 25892675444}}, \"next_era_validator_weights\":{\"01026ca707c348ed8012ac6a1f28db031fadd6eb67203501a353b867a08c8b9a80\": \"50359181028146696\", \"010427c1d1227c9d2aafe8c06c6e6b276da8dcd8fd170ca848b8e3e8e1038a6dc8\": \"836257197475108\"}}, \"era_id\": 18, \"height\": 2075, \"parent_hash\": \"d2e919792d752bc58ca3df1351e3d569cebf6bf77470475ab8321d6b2abb7ad4\", \"protocol_version\": \"1.0.0\", \"random_bit\": true, \"state_root_hash\": \"442e82950da702cc47460b318e43ace0cd3345fe2288280d304409adebb3744d\", \"timestamp\": \"2021-04-02T05:03:29.792Z\"}, \"proofs\": []}, \"block_hash\": \"e871418f5d47424514b385f48cf1b56c2e7309fbe47746d396b573111a6c9cd9\"}}".to_string();
        let protocol_version = ProtocolVersion::from_parts(1, 1, 0);
        let deserializer = determine_deserializer(protocol_version);
        let result = (deserializer)(&legacy_block_added_raw);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn given_determine_deserializer_and_1_1_0_should_return_generic_deserializer_which_deserializes_new_block_added(
    ) {
        let block_added_raw = "{\"BlockAdded\":{\"block_hash\":\"e871418f5d47424514b385f48cf1b56c2e7309fbe47746d396b573111a6c9cd9\",\"block\":{\"hash\":\"e871418f5d47424514b385f48cf1b56c2e7309fbe47746d396b573111a6c9cd9\",\"header\":{\"parent_hash\":\"d2e919792d752bc58ca3df1351e3d569cebf6bf77470475ab8321d6b2abb7ad4\",\"state_root_hash\":\"442e82950da702cc47460b318e43ace0cd3345fe2288280d304409adebb3744d\",\"body_hash\":\"43f3da1d596220f2d7202ca222c3c11f1ec100102e7fa0bee1e8e40f8ed4497c\",\"random_bit\":true,\"accumulated_seed\":\"32f33577d757609af354f4e18f21af684ae793fe9469388aa605a75bad9dc86b\",\"era_end\":{\"era_report\":{\"equivocators\":[],\"rewards\":[{\"validator\":\"01026ca707c348ed8012ac6a1f28db031fadd6eb67203501a353b867a08c8b9a80\",\"amount\":1559249876159},{\"validator\":\"010427c1d1227c9d2aafe8c06c6e6b276da8dcd8fd170ca848b8e3e8e1038a6dc8\",\"amount\":25892675444}],\"inactive_validators\":[]},\"next_era_validator_weights\":[{\"validator\":\"01026ca707c348ed8012ac6a1f28db031fadd6eb67203501a353b867a08c8b9a80\",\"weight\":\"50359181028146696\"},{\"validator\":\"010427c1d1227c9d2aafe8c06c6e6b276da8dcd8fd170ca848b8e3e8e1038a6dc8\",\"weight\":\"836257197475108\"}]},\"timestamp\":\"2021-04-02T05:03:29.792Z\",\"era_id\":18,\"height\":2075,\"protocol_version\":\"1.0.0\"},\"body\":{\"proposer\":\"0190c434129ecbaeb34d33185ab6bf97c3c493fc50121a56a9ed8c4c52855b5ac1\",\"deploy_hashes\":[],\"transfer_hashes\":[]},\"proofs\":[]}}}".to_string();
        let protocol_version = ProtocolVersion::from_parts(1, 1, 0);
        let deserializer = determine_deserializer(protocol_version);
        let sse_data = (deserializer)(&block_added_raw).unwrap();

        assert!(is_of_var!(sse_data, SseData::BlockAdded));
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
