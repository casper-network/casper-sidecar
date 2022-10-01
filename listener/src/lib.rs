mod utils;

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
#[cfg(test)]
use tracing_test::traced_test;

use utils::resolve_address;

#[tokio::test]
#[traced_test]
async fn check() {
    let mut receiver = EventListener::stream_events("http://127.0.0.1:18101".to_string(), 5, 5)
        .await
        .unwrap();

    while let Some(sse) = receiver.recv().await {
        println!("Received SSE: {:?}", sse.id);
    }
}

const CONNECTION_REFUSED: &str = "Connection refused (os error 111)";
const CONNECTION_ERR_MSG: &str = "Connection refused: Please check connection to node.";

#[allow(unused)]
pub struct EventListener {
    bind_address: String,
    api_version: ProtocolVersion,
    current_event_id: Option<u32>,
}

pub struct SseEvent {
    pub source: String,
    pub id: Option<u32>,
    pub data: SseData,
}

#[derive(Deserialize)]
struct ApiVersion {
    #[serde(rename = "ApiVersion")]
    version: ProtocolVersion,
}

impl EventListener {
    pub async fn stream_events(
        bind_address: String,
        max_retries: u8,
        delay_between_retries_secs: u8,
    ) -> Result<UnboundedReceiver<SseEvent>, Error> {
        resolve_address(&bind_address)?;
        let bind_address = format!("http://{}", bind_address);

        let (sse_tx, sse_rx) = unbounded_channel::<SseEvent>();

        tokio::spawn(async move {
            let mut retry_count = 0;
            let mut current_event_id: Option<u32> = None;

            while retry_count <= max_retries {
                if retry_count > 0 {
                    info!(
                        "Attempting to reconnect... ({}/{})",
                        retry_count, max_retries
                    );
                }

                let (api_version, mut event_receiver) =
                    match Self::connect(bind_address.clone(), current_event_id).await {
                        Ok(ok_val) => {
                            retry_count = 0;
                            ok_val
                        }
                        Err(_) => {
                            error!(
                                "Error connecting ({}), retrying in {}s",
                                bind_address, delay_between_retries_secs
                            );
                            retry_count += 1;
                            tokio::time::sleep(Duration::from_secs(
                                delay_between_retries_secs as u64,
                            ))
                            .await;
                            continue;
                        }
                    };

                info!(%bind_address, %api_version, "Connected to SSE");

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
                                delay_between_retries_secs
                            );
                            retry_count += 1;
                            tokio::time::sleep(Duration::from_secs(
                                delay_between_retries_secs as u64,
                            ))
                            .await;
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
            }

            // Having tried to reconnect and failed we send the Shutdown.
            let _ = sse_tx.send(SseEvent {
                source: bind_address,
                id: None,
                data: SseData::Shutdown,
            });
        });

        Ok(sse_rx)
    }

    async fn connect(
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

        let mut main_event_stream = Client::new()
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
        let api_version = match main_event_stream.next().await {
            None => return Err(Error::msg("First event was empty")),
            Some(Err(error)) => {
                return Err(Error::msg(format!("failed to get first event: {}", error)))
            }
            Some(Ok(event)) => match serde_json::from_str::<ApiVersion>(&event.data) {
                Ok(api_version) => api_version.version,
                Err(serde_err) => {
                    return match event.data.as_str() {
                        CONNECTION_REFUSED => Err(Error::msg(CONNECTION_ERR_MSG)),
                        _ => Err(Error::from(serde_err)
                            .context("First event could not be deserialized into ApiVersion")),
                    }
                }
            },
        };

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

fn parse_error_for_connection_refused(error: reqwest::Error) -> Error {
    if error.to_string().contains(CONNECTION_REFUSED) {
        Error::msg(&CONNECTION_ERR_MSG)
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
