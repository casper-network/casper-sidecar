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

#[async_trait]
pub trait StreamConnector {
    async fn connect(
        &mut self,
        url: Url,
    ) -> Result<Receiver<Result<Event, Error>>, ConnectionManagerError>;
}

pub struct ConnectToSseUrl {
    pub max_attempts: usize,
    pub delay_between_attempts: Duration,
    pub connection_timeout: Duration,
}

impl ConnectToSseUrl {
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
        let (tx, rx) = channel(100);
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
impl StreamConnector for ConnectToSseUrl {
    async fn connect(
        &mut self,
        url: Url,
    ) -> Result<Receiver<Result<Event, Error>>, ConnectionManagerError> {
        let mut retry_count = 0;
        let mut last_error = None;
        while retry_count <= self.max_attempts {
            match self.internal_connect(url.clone()).await {
                Ok(event_stream) => return Ok(self.push_events_to_channel(event_stream)),
                Err(ConnectionManagerError::NonRecoverableError { error }) => {
                    return Err(ConnectionManagerError::NonRecoverableError { error })
                }
                Err(err) => last_error = Some(err),
            }
            retry_count += 1;
            tokio::time::sleep(self.delay_between_attempts).await;
        }
        Err(couldnt_connect(last_error, url.clone(), self.max_attempts))
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
        _url: Url,
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
