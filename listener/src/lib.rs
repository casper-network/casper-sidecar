mod utils;

use std::{fmt::Debug, time::Duration};

use casper_event_types::SseData;
use casper_types::ProtocolVersion;

use anyhow::Error;
use bytes::Bytes;
use eventsource_stream::{EventStream, Eventsource};
use futures::StreamExt;
use reqwest::Client;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio_stream::Stream;
use tracing::{error, info, warn};

use utils::resolve_address;

const MAIN_FILTER_PATH: &str = "main";
const DEPLOYS_FILTER_PATH: &str = "deploys";
const SIGNATURES_FILTER_PATH: &str = "sigs";

#[derive(Clone)]
pub struct EventListener {
    bind_address: String,
    pub api_version: ProtocolVersion,
    current_event_id: Option<u32>,
    max_retries: u8,
    delay_between_retries: u8,
    allow_partial_connection: bool,
}

pub struct SseEvent {
    pub source: String,
    pub id: Option<u32>,
    pub data: SseData,
}

impl EventListener {
    /// Returns an instance of [EventListener] based on the given `bind_address`.  
    /// `delay_between_retries` should be provided in seconds.
    ///
    /// Will error if:
    /// - `bind_address` cannot be parsed into a network address.
    /// - It is unable to connect to the source.
    /// - The source doesn't send a valid [ApiVersion] as the first event.
    pub async fn new(
        bind_address: String,
        max_retries: u8,
        delay_between_retries: u8,
        allow_partial_connection: bool,
    ) -> Result<Self, Error> {
        resolve_address(&bind_address)?;
        let prefixed_address = format!("http://{}", bind_address);

        let address_with_filter = format!("{}/events/main", prefixed_address);
        let stream = Client::new()
            .get(&address_with_filter)
            .send()
            .await?
            .bytes_stream()
            .eventsource();

        let (api_version, _) = parse_api_version(stream).await?;

        Ok(Self {
            bind_address: prefixed_address,
            api_version,
            current_event_id: None,
            max_retries,
            delay_between_retries,
            allow_partial_connection,
        })
    }

    pub async fn consume_combine_streams(&self) -> UnboundedReceiver<SseEvent> {
        let (sse_tx, sse_rx) = unbounded_channel::<SseEvent>();

        let main_filter_handle = tokio::spawn(connect_with_retry(
            sse_tx.clone(),
            self.bind_address.clone(),
            MAIN_FILTER_PATH.to_string(),
            self.current_event_id,
            self.max_retries,
            self.delay_between_retries,
        ));

        let deploys_filter_handle = tokio::spawn(connect_with_retry(
            sse_tx.clone(),
            self.bind_address.clone(),
            DEPLOYS_FILTER_PATH.to_string(),
            self.current_event_id,
            self.max_retries,
            self.delay_between_retries,
        ));

        let sigs_filter_handle = tokio::spawn(connect_with_retry(
            sse_tx.clone(),
            self.bind_address.clone(),
            SIGNATURES_FILTER_PATH.to_string(),
            self.current_event_id,
            self.max_retries,
            self.delay_between_retries,
        ));

        let allow_partial_connection = self.allow_partial_connection;
        let bind_address = self.bind_address.clone();
        tokio::spawn(async move {
            if allow_partial_connection {
                let _ = tokio::join!(
                    main_filter_handle,
                    deploys_filter_handle,
                    sigs_filter_handle
                );
                warn!("All filters have disconnected");
            } else {
                tokio::select! {
                    result = main_filter_handle => {
                       warn!(?result, "Failed to connect to MAIN filter - allow_partial_connection is set to false - disconnecting from node")
                    },
                    result = deploys_filter_handle => {
                       warn!(?result, "Failed to connect to DEPLOYS filter - allow_partial_connection is set to false - disconnecting from node")
                    },
                    result = sigs_filter_handle => {
                       warn!(?result, "Failed to connect to SIGS filter - allow_partial_connection is set to false - disconnecting from node")
                    }
                }
            }
            // Retries have been exhausted without a successful connection - send Shutdown.
            let _ = sse_tx.send(SseEvent {
                source: bind_address,
                id: None,
                data: SseData::Shutdown,
            });
        });

        sse_rx
    }
}

async fn connect_with_retry(
    event_sender: UnboundedSender<SseEvent>,
    bind_address: String,
    filter: String,
    start_from_id: Option<u32>,
    max_retries: u8,
    delay_between: u8,
) {
    let mut retry_count = 0;
    let mut current_event_id = start_from_id;

    while retry_count <= max_retries {
        if retry_count > 0 {
            info!(
                bind_address,
                filter, "Attempting to reconnect... ({}/{})", retry_count, max_retries
            );
        }

        let mut url = format!("{}/events/{}", bind_address, filter);
        if let Some(event_id) = current_event_id {
            let start_from_query = format!("?start_from={}", event_id);
            url.push_str(&start_from_query);
        }

        let (api_version, mut event_stream) = match connect(url).await {
            Ok(ok_val) => {
                // reset retry counter since the connection was successful
                retry_count = 0;
                ok_val
            }
            Err(_) => {
                // increment retry counter since the connection failed
                retry_count += 1;
                error!(
                    %bind_address,
                    %filter,
                    "Error connecting, retrying in {}s",
                    delay_between
                );
                // wait the configured delay before continuing
                tokio::time::sleep(Duration::from_secs(delay_between as u64)).await;
                continue;
            }
        };

        info!(%bind_address, %filter, %api_version, "Connected to event stream{}", {
            if let Some(event_id) = start_from_id {
                format!(", resuming from id: {}", event_id)
            } else {
                "".to_string()
            }
        });

        while let Some(event) = event_stream.next().await {
            match event {
                Ok(event) => {
                    let event_id: u32 = event.id.parse().expect("Error parsing id into u32");
                    current_event_id = Some(event_id);

                    let cloned_sender = event_sender.clone();

                    match serde_json::from_str::<SseData>(&event.data) {
                        Ok(SseData::Shutdown) | Err(_) => {
                            error!(
                                %bind_address,
                                %filter,
                                "Error connecting, retrying in {}s",
                                delay_between
                            );
                            retry_count += 1;
                            tokio::time::sleep(Duration::from_secs(delay_between as u64)).await;
                            break;
                        }
                        Ok(sse_data) => {
                            let _ = cloned_sender.send(SseEvent {
                                source: bind_address.clone(),
                                id: Some(event_id),
                                data: sse_data,
                            });
                        }
                    }
                }
                Err(event_stream_err) => {
                    warn!(%bind_address, %filter, message = "Error returned from event stream", error=?event_stream_err)
                }
            }
        }
    }
    warn!(%bind_address, %filter, "Error connecting to node - if allow_partial_connection = true, listener will stay connected on other filters");
}

async fn connect(
    url: String,
) -> Result<
    (
        ProtocolVersion,
        EventStream<impl Stream<Item = reqwest::Result<Bytes>> + Sized>,
    ),
    Error,
> {
    let event_stream = Client::new()
        .get(&url)
        .send()
        .await?
        .bytes_stream()
        .eventsource();

    // Parse the first event to see if the connection was successful
    let (api_version, event_stream) = parse_api_version(event_stream).await?;

    Ok((api_version, event_stream))
}

async fn parse_api_version<EvtStr, E>(
    mut stream: EventStream<EvtStr>,
) -> Result<(ProtocolVersion, EventStream<EvtStr>), Error>
where
    E: Debug,
    EvtStr: Stream<Item = Result<Bytes, E>> + Sized + Unpin,
{
    match stream.next().await {
        None => Err(Error::msg("First event was empty")),
        Some(Err(error)) => Err(Error::msg(format!(
            "failed to get first event: {:?}",
            error
        ))),
        Some(Ok(event)) => match serde_json::from_str::<SseData>(&event.data) {
            Ok(SseData::ApiVersion(api_version)) => Ok((api_version, stream)),
            Ok(_) => Err(Error::msg("First event should have been API version")),
            Err(serde_err) => Err(Error::from(serde_err)
                .context("First event could not be deserialized into ApiVersion")),
        },
    }
}
