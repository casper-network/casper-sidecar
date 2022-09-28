use casper_types::ProtocolVersion;

use anyhow::Error;
use bytes::Bytes;
use eventsource_stream::{Event, EventStream, Eventsource};
use reqwest::Client;
use serde::Deserialize;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio_stream::{Stream, StreamExt};
use tracing::{info, warn};

const CONNECTION_REFUSED: &str = "Connection refused (os error 111)";
const CONNECTION_ERR_MSG: &str = "Connection refused: Please check connection to node.";

pub struct NodeEventSource {
    pub api_version: ProtocolVersion,
    pub event_receiver: UnboundedReceiver<Event>,
}

#[derive(Deserialize)]
struct ApiVersion {
    #[serde(rename = "ApiVersion")]
    version: ProtocolVersion,
}

pub async fn connect_to_event_stream(
    node_ip_address: String,
    sse_port: u16,
    start_from_id: Option<u32>,
) -> Result<NodeEventSource, Error> {
    let url_base = format!("http://{}:{}/events", node_ip_address, sse_port);

    let mut main_filter_path = format!("{}/main", url_base);
    let mut deploys_filter_path = format!("{}/deploys", url_base);
    let mut sigs_filter_path = format!("{}/sigs", url_base);

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

    info!(
        message = "Connected to node",
        api_version = api_version.to_string().as_str(),
        node_ip_address = node_ip_address.as_str()
    );

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

    Ok(NodeEventSource {
        api_version,
        event_receiver: aggregate_events_rx,
    })
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
