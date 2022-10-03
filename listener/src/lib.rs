mod utils;

use std::fmt::Debug;
use std::time::Duration;

use casper_event_types::SseData;
use casper_types::ProtocolVersion;

use anyhow::Error;
use bytes::Bytes;
use eventsource_stream::{Event, EventStream, Eventsource};
use reqwest::Client;
use serde::Deserialize;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio_stream::{Stream, StreamExt};
use tracing::{error, info, warn};

use utils::resolve_address;

const CONNECTION_REFUSED: &str = "Connection refused (os error 111)";
const CONNECTION_ERR_MSG: &str = "Connection refused: Please check connection to node.";

#[allow(unused)]
#[derive(Clone)]
pub struct EventListener {
    bind_address: String,
    pub api_version: ProtocolVersion,
    current_event_id: Option<u32>,
    max_retries: u8,
    delay_between_retries: u8,
}

pub struct SseEvent {
    pub source: String,
    pub id: Option<u32>,
    pub data: SseData,
}

/// The version of this node's API server.  This event will always be the first sent to a new
/// client, and will have no associated event ID provided.
#[derive(Deserialize)]
struct ApiVersion {
    #[serde(rename = "ApiVersion")]
    version: ProtocolVersion,
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
    ) -> Result<Self, Error> {
        resolve_address(&bind_address)?;
        let prefixed_address = format!("http://{}", bind_address);

        let address_with_filter = format!("{}/events/main", prefixed_address);
        let stream = Client::new()
            .get(&address_with_filter)
            .send()
            .await
            .map_err(parse_error_for_connection_refused)?
            .bytes_stream()
            .eventsource();

        let (api_version, _) = parse_api_version(stream).await?;

        Ok(Self {
            bind_address: prefixed_address,
            api_version,
            current_event_id: None,
            max_retries,
            delay_between_retries,
        })
    }

    pub async fn stream_events(&self) -> UnboundedReceiver<SseEvent> {
        let (sse_tx, sse_rx) = unbounded_channel::<SseEvent>();

        let mut cloned_self = self.clone();

        tokio::spawn(async move {
            let mut retry_count = 0;
            let mut current_event_id: Option<u32> = None;

            while retry_count <= cloned_self.max_retries {
                if retry_count > 0 {
                    info!(
                        "Attempting to reconnect... ({}/{})",
                        retry_count, cloned_self.max_retries
                    );
                }

                let (api_version, mut event_receiver) = match cloned_self
                    .connect(cloned_self.bind_address.clone(), current_event_id)
                    .await
                {
                    Ok(ok_val) => {
                        retry_count = 0;
                        ok_val
                    }
                    Err(_) => {
                        error!(
                            "Error connecting ({}), retrying in {}s",
                            cloned_self.bind_address, cloned_self.delay_between_retries
                        );
                        retry_count += 1;
                        tokio::time::sleep(Duration::from_secs(
                            cloned_self.delay_between_retries as u64,
                        ))
                        .await;
                        continue;
                    }
                };

                if api_version.ne(&cloned_self.api_version) {
                    warn!(
                        "API version changed from {} to {}",
                        cloned_self.api_version, api_version
                    );
                    cloned_self.api_version = api_version;
                }

                info!(%cloned_self.bind_address, %api_version, "Connected to SSE");

                if let Some(id) = current_event_id {
                    info!("Resuming from event id: {}", id);
                }

                while let Some(event) = event_receiver.recv().await {
                    // todo error handling
                    let event_id: u32 = event.id.parse().expect("Error parsing id into u32");
                    current_event_id = Some(event_id);

                    let cloned_sender = sse_tx.clone();
                    let parsed =
                        serde_json::from_str::<SseData>(&event.data).map_err(|serde_err| {
                            warn!(?event, "Error from stream");
                            if serde_err.to_string() == CONNECTION_REFUSED {
                                warn!("Connection to node lost...");
                            } else {
                                warn!(
                                    "Error parsing SSE: {}, for data:\n{}\n",
                                    serde_err.to_string(),
                                    &event.data
                                );
                            }
                        });

                    match parsed {
                        Ok(SseData::Shutdown) | Err(_) => {
                            error!(
                                "Error connecting, retrying in {}s",
                                cloned_self.delay_between_retries
                            );
                            retry_count += 1;
                            tokio::time::sleep(Duration::from_secs(
                                cloned_self.delay_between_retries as u64,
                            ))
                            .await;
                            break;
                        }
                        Ok(sse_data) => {
                            let _ = cloned_sender.send(SseEvent {
                                source: cloned_self.bind_address.clone(),
                                id: Some(event_id),
                                data: sse_data,
                            });
                        }
                    }
                }
            }

            // Having tried to reconnect and failed we send the Shutdown.
            let _ = sse_tx.send(SseEvent {
                source: cloned_self.bind_address,
                id: None,
                data: SseData::Shutdown,
            });
        });

        sse_rx
    }

    async fn connect(
        &self,
        bind_address: String,
        start_from_id: Option<u32>,
    ) -> Result<(ProtocolVersion, UnboundedReceiver<Event>), Error> {
        let mut main_filter_path = format!("{}/events/main", bind_address);
        let mut deploys_filter_path = format!("{}/events/deploys", bind_address);
        let mut sigs_filter_path = format!("{}/events/sigs", bind_address);

        if let Some(event_id) = start_from_id {
            let start_from_query_string = format!("?start_from={}", event_id);
            main_filter_path.push_str(&start_from_query_string);
            deploys_filter_path.push_str(&start_from_query_string);
            sigs_filter_path.push_str(&start_from_query_string);
        }

        let main_event_stream = Client::new()
            .get(&main_filter_path)
            .send()
            .await
            .map_err(parse_error_for_connection_refused)?
            .bytes_stream()
            .eventsource();

        let deploys_event_stream = Client::new()
            .get(&deploys_filter_path)
            .send()
            .await
            .map_err(parse_error_for_connection_refused)?
            .bytes_stream()
            .eventsource();

        let sigs_event_stream = Client::new()
            .get(&sigs_filter_path)
            .send()
            .await
            .map_err(parse_error_for_connection_refused)?
            .bytes_stream()
            .eventsource();

        // Channel for funnelling all event types into.
        let (aggregate_events_tx, aggregate_events_rx) = unbounded_channel();
        // Clone the aggregate sender for each event type. These will all feed into the aggregate receiver.
        let main_events_tx = aggregate_events_tx.clone();
        let deploy_events_tx = aggregate_events_tx.clone();
        let sigs_event_tx = aggregate_events_tx.clone();

        // Parse the first event to see if the connection was successful
        let (api_version, main_event_stream) = parse_api_version(main_event_stream).await?;

        // For each filtered Stream pass the events along a Sender which all feed into the
        // aggregate Receiver. The first event is (should be) the API Version which is already
        // extracted from the Main filter in the code above, however it can to be discarded
        // from the Deploys and Sigs filter streams.
        tokio::spawn(stream_events_to_channel(
            main_event_stream,
            main_events_tx,
            false,
        ));
        tokio::spawn(stream_events_to_channel(
            deploys_event_stream,
            deploy_events_tx,
            true,
        ));
        tokio::spawn(stream_events_to_channel(
            sigs_event_stream,
            sigs_event_tx,
            true,
        ));

        Ok((api_version, aggregate_events_rx))
    }
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
        Some(Ok(event)) => match serde_json::from_str::<ApiVersion>(&event.data) {
            Ok(api_version) => Ok((api_version.version, stream)),
            Err(serde_err) => {
                return match event.data.as_str() {
                    CONNECTION_REFUSED => Err(Error::msg(CONNECTION_ERR_MSG)),
                    _ => Err(Error::from(serde_err)
                        .context("First event could not be deserialized into ApiVersion")),
                }
            }
        },
    }
}

fn parse_error_for_connection_refused(error: reqwest::Error) -> Error {
    if error.to_string().contains(CONNECTION_REFUSED) {
        Error::msg(CONNECTION_ERR_MSG)
    } else {
        Error::from(error)
    }
}

async fn stream_events_to_channel(
    mut event_stream: EventStream<impl Stream<Item = Result<Bytes, reqwest::Error>> + Unpin>,
    sender: UnboundedSender<Event>,
    discard_first: bool,
) {
    if discard_first {
        let _ = event_stream.next().await;
    }
    while let Some(event) = event_stream.next().await {
        match event {
            Ok(event) => {
                let _ = sender.send(event);
            }
            Err(error) => warn!(%error, "Error receiving events"),
        }
    }
}
