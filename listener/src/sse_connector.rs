use crate::connection_manager::{non_recoverable_error, recoverable_error, ConnectionManagerError};
use anyhow::Error;
use async_trait::async_trait;
use bytes::Bytes;
use eventsource_stream::{Event, EventStream, Eventsource};
use futures::StreamExt;
use reqwest::Client;
use std::{fmt::Debug, time::Duration};
use tokio::sync::mpsc::{channel, Receiver};
use tokio_stream::Stream;
use tracing::debug;
use url::Url;

/// Abstraction over sse connection which hides all the http details and allows mocks for testing.
/// It returns a channel which passes data from stream.
#[async_trait]
pub trait StreamConnector {
    async fn connect(
        &mut self,
        current_event_id: Option<u32>,
    ) -> Result<Receiver<Result<Event, Error>>, ConnectionManagerError>;
}

/// Implementation of [StreamConnector] which connects to an sse http endpoint. Includes retries of initial connection.
/// By design if there is some error after the initial connection it will close the channel and not retry forcing upstream
/// code to handle the disconnect.
pub struct SseConnection {
    pub max_attempts: usize,
    pub delay_between_attempts: Duration,
    pub connection_timeout: Duration,
    pub channel_buffer_size: usize,
    pub bind_address: Url,
}

impl SseConnection {
    async fn internal_connect(
        &mut self,
        url: Url,
    ) -> Result<
        EventStream<impl Stream<Item = Result<Bytes, reqwest::Error>>>,
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
        Ok(sse_response.bytes_stream().eventsource())
    }

    fn push_events_to_channel<E, S>(&self, mut event_stream: S) -> Receiver<Result<Event, Error>>
    where
        E: Debug + Sync + Send + std::error::Error + 'static,
        S: Stream<Item = Result<Event, E>> + Sized + Unpin + Send + 'static,
    {
        let (tx, rx) = channel(self.channel_buffer_size);
        tokio::spawn(async move {
            while let Some(stream_res) = event_stream.next().await {
                let stream_res = stream_res.map_err(|stream_error| Error::from(stream_error));
                let res = tx.send(stream_res).await;
                if res.is_err() {
                    break;
                }
            }
            drop(tx);
        });
        rx
    }
}

#[async_trait]
impl StreamConnector for SseConnection {
    async fn connect(
        &mut self,
        current_event_id: Option<u32>,
    ) -> Result<Receiver<Result<Event, Error>>, ConnectionManagerError> {
        let mut bind_address = self.bind_address.clone();
        if let Some(event_id) = current_event_id {
            let query = format!("start_from={}", event_id);
            bind_address.set_query(Some(&query));
        }
        let mut retry_count = 0;
        let mut last_error = None;
        while retry_count <= self.max_attempts {
            match self.internal_connect(bind_address.clone()).await {
                Ok(event_stream) => return Ok(self.push_events_to_channel(event_stream)),
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
pub struct MockSseConnection {
    data: Vec<Event>,
    failure_on_connection: Option<ConnectionManagerError>,
    failure_on_message: Option<Error>,
}

#[cfg(test)]
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
            failure_on_connection: Some(ConnectionManagerError::NonRecoverableError { error: e }),
            failure_on_message: None,
        }
    }

    pub fn build_failing_on_message() -> Self {
        let e = Error::msg("Some error on message");
        MockSseConnection {
            data: vec![],
            failure_on_connection: None,
            failure_on_message: Some(e),
        }
    }
}

#[cfg(test)]
#[async_trait]
impl StreamConnector for MockSseConnection {
    async fn connect(
        &mut self,
        _current_event_id: Option<u32>,
    ) -> Result<Receiver<Result<Event, Error>>, ConnectionManagerError> {
        if let Some(err) = self.failure_on_connection.take() {
            return Err(err);
        }
        let data = self.data.clone();
        let (tx, rx) = channel(100);
        let maybe_fail_on_message = self.failure_on_message.take();
        tokio::spawn(async move {
            let mut maybe_fail_on_message = maybe_fail_on_message;
            for datum in data {
                if let Some(err) = maybe_fail_on_message.take() {
                    let _ = tx.send(Err(err)).await;
                    break;
                }
                let res = tx.send(Ok(datum)).await;
                if res.is_err() {
                    break;
                }
            }
            drop(tx);
        });
        Ok(rx)
    }
}

#[cfg(test)]
mod tests {
    use crate::sse_connector::{MockSseConnection, SseConnection, StreamConnector};
    use eventsource_stream::Event;
    use futures_util::stream::iter;
    use std::{convert::Infallible, time::Duration};
    use url::Url;
    use warp::{sse::Event as SseEvent, Filter};

    #[tokio::test]
    async fn given_sse_connection_should_read_data() {
        let sse_port = portpicker::pick_unused_port().unwrap();
        spin_up_test_sse_endpoint(sse_port).await;
        let mut connection = SseConnection {
            max_attempts: 5,
            delay_between_attempts: Duration::from_secs(2),
            connection_timeout: Duration::from_secs(10),
            channel_buffer_size: 50,
            bind_address: Url::parse(
                format!("http://localhost:{}/notifications", sse_port).as_str(),
            )
            .unwrap(),
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
            channel_buffer_size: 50,
            bind_address: Url::parse(
                format!("http://localhost:{}/notifications", sse_port).as_str(),
            )
            .unwrap(),
        };
        let res = connection.connect(None).await;
        assert!(res.is_err());
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

    async fn fetch_data(connection: &mut dyn StreamConnector) -> Vec<String> {
        let mut data = vec![];
        if let Ok(mut receiver) = connection.connect(None).await {
            while let Some(event_res) = receiver.recv().await {
                let event = event_res.unwrap();
                data.push(event.data);
            }
        }
        data
    }

    async fn spin_up_test_sse_endpoint(sse_port: u16) {
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
