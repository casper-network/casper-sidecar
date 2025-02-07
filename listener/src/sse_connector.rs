use crate::{
    connection_manager::{non_recoverable_error, recoverable_error, ConnectionManagerError},
    keep_alive_monitor::KeepAliveMonitor,
};
use anyhow::Error;
use async_stream::stream;
use async_trait::async_trait;
use bytes::Bytes;
use eventsource_stream::{Event, EventStream, EventStreamError, Eventsource};
use futures::StreamExt;
use reqwest::Client;
use std::{fmt::Debug, pin::Pin, time::Duration};
use tokio::select;
use tokio_stream::Stream;
use tracing::{debug, trace};
use url::Url;

#[derive(Clone, Debug)]
pub enum SseDataStreamingError {
    NoDataTimeout(),
    ConnectionError(),
}

pub type EventResult = Result<Event, EventStreamError<SseDataStreamingError>>;
/// Abstraction over sse connection which hides all the http details and allows mocks for testing.
/// It returns a channel which passes data from stream.
#[async_trait]
pub trait StreamConnector {
    async fn connect(
        &mut self,
        current_event_id: Option<u32>,
    ) -> Result<Pin<Box<dyn Stream<Item = EventResult> + Send + 'static>>, ConnectionManagerError>;
}

/// Implementation of [StreamConnector] which connects to an sse http endpoint. Includes retries of initial connection.
/// By design if there is some error after the initial connection it will close the channel and not retry forcing upstream
/// code to handle the disconnect.
pub struct SseConnection {
    pub max_attempts: usize,
    pub delay_between_attempts: Duration,
    pub connection_timeout: Duration,
    pub bind_address: Url,
    pub sleep_between_keepalive_checks: Duration,
    pub no_message_timeout: Duration,
}

impl SseConnection {
    async fn internal_connect(
        &mut self,
        url: Url,
    ) -> Result<
        Pin<Box<EventStream<impl Stream<Item = Result<Bytes, SseDataStreamingError>>>>>,
        ConnectionManagerError,
    > {
        debug!("Connecting to node...\t{}", url);
        let client = Client::builder()
            .connect_timeout(self.connection_timeout)
            .build()
            .map_err(|err| recoverable_error(Error::new(err)))?;
        let sse_response = client
            .get(url)
            .send()
            .await
            .map_err(|err| recoverable_error(Error::new(err)))?;
        let stream = self.build_byte_stream(sse_response).await;
        Ok(Box::pin(stream.eventsource()))
    }

    async fn build_byte_stream(
        &mut self,
        sse_response: reqwest::Response,
    ) -> impl Stream<Item = Result<Bytes, SseDataStreamingError>> {
        let monitor =
            KeepAliveMonitor::new(self.sleep_between_keepalive_checks, self.no_message_timeout);
        monitor.start().await;
        let cancellation_token = monitor.get_cancellation_token();
        let mut stream = sse_response.bytes_stream();
        stream! {
            loop {
                select! {
                    maybe_bytes = stream.next() => {
                        if let Some(res_bytes) = maybe_bytes {
                            match res_bytes {
                                Ok(bytes) => {
                                    monitor.tick().await;
                                    yield Ok(bytes);
                                },
                                Err(e) => {
                                    trace!("Error when receiving bytes: {}", e);
                                    yield Err(SseDataStreamingError::ConnectionError());
                                    break;
                                }
                            }
                        } else {
                            break;
                        }
                    }
                    _ = cancellation_token.cancelled() => {
                        yield Err(SseDataStreamingError::NoDataTimeout());
                        break;
                    }
                }
            }
        }
    }
}

#[async_trait]
impl StreamConnector for SseConnection {
    async fn connect(
        &mut self,
        current_event_id: Option<u32>,
    ) -> Result<Pin<Box<dyn Stream<Item = EventResult> + Send + 'static>>, ConnectionManagerError>
    {
        let mut bind_address = self.bind_address.clone();
        if let Some(event_id) = current_event_id {
            let query = format!("start_from={}", event_id);
            bind_address.set_query(Some(&query));
        }
        let mut retry_count = 0;
        let mut last_error = None;
        while retry_count <= self.max_attempts {
            match self.internal_connect(bind_address.clone()).await {
                Ok(event_stream) => {
                    return Ok(event_stream);
                }
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
            bind_address.clone(),
            self.max_attempts,
        ))
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

#[cfg(test)]
pub mod tests {
    use crate::{
        connection_manager::ConnectionManagerError,
        sse_connector::{SseConnection, StreamConnector},
    };
    use anyhow::Error;
    use async_stream::stream;
    use async_trait::async_trait;
    use eventsource_stream::{Event, Eventsource};
    use futures_util::{stream::iter, Stream};
    use std::{
        convert::Infallible,
        pin::Pin,
        time::{Duration, Instant},
    };
    use tokio::sync::mpsc::channel;
    use tokio::time::{sleep, timeout};
    use tokio_stream::StreamExt;
    use url::Url;
    use warp::{sse::Event as SseEvent, Filter};

    use super::{EventResult, SseDataStreamingError};

    pub struct MockSseConnection {
        data: Vec<Event>,
        failure_on_connection: Option<ConnectionManagerError>,
        failure_on_message: Option<SseDataStreamingError>,
    }

    impl MockSseConnection {
        pub fn build_with_data(input_data: Vec<String>) -> Self {
            let mut data = vec![];
            for (i, raw) in input_data.iter().enumerate() {
                let event = Event {
                    event: "".to_string(),
                    data: raw.clone(),
                    id: i.to_string(),
                    retry: None,
                };
                data.push(event);
            }
            MockSseConnection {
                data,
                failure_on_connection: None,
                failure_on_message: None,
            }
        }

        pub fn build_failing_on_connection() -> Self {
            let e = Error::msg("Some error on connection");
            MockSseConnection {
                data: vec![],
                failure_on_connection: Some(ConnectionManagerError::NonRecoverableError {
                    error: e,
                }),
                failure_on_message: None,
            }
        }

        pub fn build_failing_on_message() -> Self {
            let e = SseDataStreamingError::ConnectionError();
            MockSseConnection {
                data: vec![],
                failure_on_connection: None,
                failure_on_message: Some(e),
            }
        }
    }

    #[async_trait]
    impl StreamConnector for MockSseConnection {
        async fn connect(
            &mut self,
            _current_event_id: Option<u32>,
        ) -> Result<Pin<Box<dyn Stream<Item = EventResult> + Send + 'static>>, ConnectionManagerError>
        {
            if let Some(err) = self.failure_on_connection.take() {
                return Err(err);
            }
            let data = self.data.clone();
            let (tx, mut rx) = channel(100);
            let maybe_fail_on_message = self.failure_on_message.take();
            tokio::spawn(async move {
                let mut maybe_fail_on_message = maybe_fail_on_message;
                for datum in data {
                    if let Some(err) = maybe_fail_on_message.take() {
                        let _ = tx.send(Err(err)).await;
                        break;
                    }

                    let res = tx
                        .send(Ok(bytes::Bytes::from(build_sse_raw_from_event(datum))))
                        .await;
                    if res.is_err() {
                        break;
                    }
                }
                drop(tx);
            });
            Ok(Box::pin(
                stream! {
                    while let Some(res) = rx.recv().await {
                        yield res;
                    }
                }
                .eventsource(),
            ))
        }
    }

    fn build_sse_raw_from_event(datum: Event) -> String {
        let mut event_raw = String::default();
        let mut is_empty = true;
        if !datum.data.is_empty() {
            let field_data_raw = datum.data;
            if is_empty {
                event_raw = format!("{event_raw}data:{field_data_raw}");
            } else {
                event_raw = format!("{event_raw}\rdata:{field_data_raw}");
            }
            is_empty = false;
        }
        if !datum.id.is_empty() && !datum.id.contains('\u{0000}') {
            let field_data_raw = datum.id;
            if is_empty {
                event_raw = format!("{event_raw}id:{field_data_raw}");
            } else {
                event_raw = format!("{event_raw}\rid:{field_data_raw}");
            }
            is_empty = false;
        }
        if !datum.event.is_empty() {
            let field_data_raw = datum.event;
            if is_empty {
                event_raw = format!("{event_raw}event:{field_data_raw}");
            } else {
                event_raw = format!("{event_raw}\revent:{field_data_raw}");
            }
        }
        format!("{event_raw}\n\n")
    }

    #[tokio::test]
    async fn given_sse_connection_should_read_data() {
        let sse_port = portpicker::pick_unused_port().unwrap();
        sse_server_finite_messages(sse_port).await;
        let mut connection = SseConnection {
            max_attempts: 5,
            delay_between_attempts: Duration::from_secs(2),
            connection_timeout: Duration::from_secs(10),
            bind_address: Url::parse(
                format!("http://localhost:{}/notifications", sse_port).as_str(),
            )
            .unwrap(),
            sleep_between_keepalive_checks: Duration::from_secs(20),
            no_message_timeout: Duration::from_secs(20),
        };

        let data = fetch_data(&mut connection).await;
        assert_eq!(data, vec!["msg 1", "msg 2", "msg 3"])
    }

    #[tokio::test]
    async fn given_sse_connection_when_connecting_to_nonexisting_should_fail() {
        let sse_port = portpicker::pick_unused_port().unwrap();
        let mut connection = SseConnection {
            max_attempts: 5,
            delay_between_attempts: Duration::from_secs(2),
            connection_timeout: Duration::from_secs(10),
            bind_address: Url::parse(
                format!("http://localhost:{}/notifications", sse_port).as_str(),
            )
            .unwrap(),
            sleep_between_keepalive_checks: Duration::from_secs(20),
            no_message_timeout: Duration::from_secs(20),
        };
        let res = connection.connect(None).await;
        assert!(res.is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn given_sse_connection_when_no_data_should_fail() {
        let sse_port = portpicker::pick_unused_port().unwrap();
        sse_server_messages_in_loop(sse_port, 1, 25);
        let mut connection = SseConnection {
            max_attempts: 5,
            delay_between_attempts: Duration::from_secs(2),
            connection_timeout: Duration::from_secs(10),
            bind_address: Url::parse(
                format!("http://localhost:{}/notifications", sse_port).as_str(),
            )
            .unwrap(),
            sleep_between_keepalive_checks: Duration::from_secs(1),
            no_message_timeout: Duration::from_secs(5),
        };
        let start = Instant::now();
        let data = fetch_data_with_timeout(&mut connection, Duration::from_secs(20)).await;
        let elapsed = start.elapsed();
        assert!(data.is_empty());
        assert!(elapsed.as_secs() >= 5); // It should take more then 5 seconds before the inactivity check kicks in
    }

    #[tokio::test]
    async fn given_mock_sse_connection_should_read_data() {
        let data1 = Event {
            data: "data 1".to_string(),
            ..Default::default()
        };
        let data2 = Event {
            data: "data 2".to_string(),
            ..Default::default()
        };
        let mut connection = MockSseConnection {
            data: vec![data1, data2],
            failure_on_connection: None,
            failure_on_message: None,
        };

        let data = fetch_data(&mut connection).await;
        assert_eq!(data, vec!["data 1", "data 2"])
    }

    async fn fetch_data_with_timeout(
        connection: &mut dyn StreamConnector,
        timeout_after: Duration,
    ) -> Vec<String> {
        let mut data = vec![];
        let connection = timeout(Duration::from_secs(5), connection.connect(None)).await;
        if connection.is_err() {
            panic!("Couln't connect to sse endpoint in 5 seconds");
        }
        if let Ok(mut receiver) = connection.unwrap() {
            while let Ok(res) = timeout(timeout_after, receiver.next()).await {
                if let Some(event_res) = res {
                    if let Ok(event) = event_res {
                        data.push(event.data);
                    }
                } else {
                    break;
                }
            }
        }
        data
    }

    async fn fetch_data(connection: &mut dyn StreamConnector) -> Vec<String> {
        fetch_data_with_timeout(connection, Duration::from_secs(120)).await
    }

    fn build_stream(
        interval_in_seconds: u64,
        initial_delay_in_seconds: u64,
    ) -> impl Stream<Item = Result<warp::sse::Event, Infallible>> {
        let (tx, mut rx) = tokio::sync::mpsc::channel(10);
        tokio::spawn(async move {
            sleep(Duration::from_secs(initial_delay_in_seconds)).await;
            let mut i = 0;
            loop {
                let _ = tx.send(i).await;
                i += 1;
                sleep(Duration::from_secs(interval_in_seconds)).await;
            }
        });
        stream! {
            while let Some(z) = rx.recv().await {
                let mut event = warp::sse::Event::default().data("abc".to_string());
                event = event.id(z.to_string());
                yield Ok(event);
            }
        }
    }

    fn sse_server_messages_in_loop(
        port: u16,
        interval_in_seconds: u64,
        initial_delay_in_seconds: u64,
    ) {
        let routes = warp::path("notifications").and(warp::get()).map(move || {
            let event_stream = build_stream(interval_in_seconds, initial_delay_in_seconds);
            // reply using server-sent events
            // keep-alive is omitted intentionally so we can test scenarios in which the sse endpoint gives no traffic
            warp::sse::reply(event_stream)
        });
        tokio::spawn(async move {
            warp::serve(routes).run(([127, 0, 0, 1], port)).await;
        });
    }

    async fn sse_server_finite_messages(sse_port: u16) {
        fn sse_events() -> impl futures_util::Stream<Item = Result<SseEvent, Infallible>> {
            iter(vec![
                Ok(SseEvent::default().data("msg 1")),
                Ok(SseEvent::default().data("msg 2")),
                Ok(SseEvent::default().data("msg 3")),
            ])
        }
        let routes = warp::path("notifications")
            .and(warp::get())
            .map(|| warp::sse::reply(warp::sse::keep_alive().stream(sse_events())));
        tokio::spawn(async move { warp::serve(routes).run(([127, 0, 0, 1], sse_port)).await });
    }
}
