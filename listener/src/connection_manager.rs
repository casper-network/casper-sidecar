use std::{
    fmt::{Debug, Display, Formatter},
    sync::Arc,
    time::Duration,
};

use anyhow::Error;
use bytes::Bytes;
use eventsource_stream::{Event, EventStream, Eventsource};
use futures::StreamExt;
use reqwest::Client;

use tokio_stream::Stream;
use tracing::{debug, error, trace, warn};

use casper_event_types::SseData;
use reqwest::Url;
use tokio::{
    sync::{mpsc::Sender, Barrier},
    time::timeout,
};

pub struct SseEvent {
    id: u32,
    data: SseData,
    source: Url,
}

impl SseEvent {
    pub fn new(id: u32, data: SseData, mut source: Url) -> Self {
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

pub struct ConnectionManager {
    bind_address: Url,
    current_event_id: Option<u32>,
    attempts: usize,
    max_attempts: usize,
    delay_between_attempts: Duration,
    sse_event_sender: Sender<SseEvent>,
    exhaustible_initial_barrier: Option<Arc<Barrier>>,
    exhaustible_connected_barrier: Option<Arc<Barrier>>,
    connection_synchronization_timeout: Duration,
    connection_timeout: Duration,
}

impl ConnectionManager {
    pub fn new(
        bind_address: Url,
        max_attempts: usize,
        sse_data_sender: Sender<SseEvent>,
        exhaustible_initial_barrier: Option<Arc<Barrier>>,
        exhaustible_connected_barrier: Option<Arc<Barrier>>,
        connection_synchronization_timeout: Duration,
        connection_timeout: Duration,
    ) -> Self {
        trace!("Creating connection manager for: {}", bind_address);

        Self {
            bind_address,
            current_event_id: None,
            attempts: 0,
            max_attempts,
            delay_between_attempts: Duration::from_secs(1),
            sse_event_sender: sse_data_sender,
            exhaustible_initial_barrier,
            exhaustible_connected_barrier,
            connection_synchronization_timeout,
            connection_timeout,
        }
    }

    ///This function is blocking, it will return an Error result if something went wrong while processing.
    pub async fn start_handling(&mut self) -> Error {
        match self.do_start_handling().await {
            Ok(_) => Error::msg("Unexpected Ok() from do_start_handling"),
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
    ) -> Result<EventStream<impl Stream<Item = reqwest::Result<Bytes>> + Sized>, Error> {
        let mut retry_count = 0;
        let mut last_error = None;
        while retry_count <= self.max_attempts {
            match self.connect().await {
                Ok(event_stream) => return Ok(event_stream),
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
    ) -> Result<EventStream<impl Stream<Item = reqwest::Result<Bytes>> + Sized>, Error> {
        let mut bind_address = self.bind_address.clone();

        if let Some(event_id) = self.current_event_id {
            let query = format!("start_from={}", event_id);
            bind_address.set_query(Some(&query))
        }

        debug!("Connecting to node...\t{}", bind_address);
        let client = Client::builder()
            .connect_timeout(self.connection_timeout)
            .build()?;
        let sse_response = client.get(bind_address).send().await?;
        let event_source = sse_response.bytes_stream().eventsource();
        // Parse the first event to see if the connection was successful
        consume_api_version(event_source).await
    }

    async fn do_start_handling(&mut self) -> Result<(), Error> {
        let mut event_stream = self.connect_with_retries().await?;

        if let Some(barrier) = self.exhaustible_initial_barrier.take() {
            if timeout(self.connection_synchronization_timeout, barrier.wait())
                .await
                .is_err()
            {
                return Err(didnt_get_synchronised(
                    self.bind_address.clone(),
                    self.connection_synchronization_timeout,
                ));
            } else {
                match self.exhaustible_connected_barrier.take() {
                    None => {
                        return Err(wrong_barrier_setup(self.bind_address.clone()));
                    }
                    Some(barrier) => {
                        if timeout(self.connection_synchronization_timeout, barrier.wait())
                            .await
                            .is_err()
                        {
                            return Err(didnt_get_synchronised(
                                self.bind_address.clone(),
                                self.connection_synchronization_timeout,
                            ));
                        }
                    }
                }
            }
        }

        loop {
            let outcome = self.handle_stream(event_stream).await;
            let error = match outcome {
                Ok(_) => {
                    return Err(decorate_with_event_stream_closed(self.bind_address.clone()));
                }
                Err(error) => error,
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

    async fn handle_stream<E, S>(&mut self, mut event_stream: S) -> Result<(), Error>
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
                    self.handle_event(event).await?;
                }
                Err(stream_error) => {
                    let error_message = format!("EventStream Error: {:?}", stream_error);
                    error!(error_message);
                    return Err(Error::msg(error_message));
                }
            }
        }
        Ok(())
    }

    async fn handle_event(&mut self, event: Event) -> Result<(), Error> {
        match serde_json::from_str::<SseData>(&event.data) {
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

                let sse_event = SseEvent::new(
                    event.id.parse().unwrap_or(0),
                    sse_data,
                    self.bind_address.clone(),
                );
                let _ = self.sse_event_sender.send(sse_event).await;
            }
        }
        Ok(())
    }
}

async fn consume_api_version<S, E>(mut stream: EventStream<S>) -> Result<EventStream<S>, Error>
where
    E: Debug,
    S: Stream<Item = Result<Bytes, E>> + Sized + Unpin,
{
    match stream.next().await {
        None => Err(Error::msg("First event was empty")),
        Some(Err(error)) => Err(failed_to_get_first_event(error)),
        Some(Ok(event)) => {
            if event.data.contains("ApiVersion") {
                Ok(stream)
            } else {
                Err(expected_first_message_to_be_api_version(event.data))
            }
        }
    }
}

fn wrong_barrier_setup(url: Url) -> Error {
    let message = format!(
        "Error in initialization, cannot have exhaustible_initial_barrier Some and exhaustible_connected_barrier None. Filter: {:?}",
        url.as_str()
    );
    Error::msg(message)
}

fn couldnt_connect(last_error: Option<Error>, url: Url, attempts: usize) -> Error {
    let message = format!(
        "Couldn't connect to address {:?} in {:?} attempts",
        url.as_str(),
        attempts
    );
    match last_error {
        None => Error::msg(message),
        Some(err) => err.context(message),
    }
}
fn didnt_get_synchronised(url: Url, timeout: Duration) -> Error {
    let message = format!(
        "Didn't get synchronized with other filters. Filter: {:?}. Waited for {:?}",
        url.as_str(),
        timeout
    );
    Error::msg(message)
}

fn decorate_with_event_stream_closed(address: Url) -> Error {
    let message = format!("Event stream closed for filter: {:?}", address.as_str());
    Error::msg(message)
}

fn decorate_with_error_handling_stream(error: Error, address: Url) -> Error {
    let message = format!(
        "Error while handling event stream for filter {:?}",
        address.as_str()
    );
    error.context(message)
}

fn failed_to_get_first_event<T>(error: T) -> Error
where
    T: Debug,
{
    Error::msg(format!("failed to get first event: {:?}", error))
}

fn expected_first_message_to_be_api_version(data: String) -> Error {
    Error::msg(format!(
        "Expected first message to be ApiVersion, got: {:?}",
        data
    ))
}
