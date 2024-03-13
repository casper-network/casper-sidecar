use super::{
    config::Config,
    event_indexer::EventIndex,
    sse_server::{BroadcastChannelMessage, Id, NewSubscriberInfo, ServerSentEvent},
};
use casper_event_types::{sse_data::SseData, Filter, SIDECAR_VERSION};
use casper_types::ProtocolVersion;
use futures::{future, Future, FutureExt};
use tokio::{
    select,
    sync::{
        broadcast,
        mpsc::{self, error::SendError},
        oneshot,
    },
    task,
};
use tracing::{error, info, trace};
use wheelbuf::WheelBuf;
pub type InboundData = (Option<u32>, SseData, Option<Filter>, Option<String>);
pub type OutboundReceiver =
    mpsc::UnboundedReceiver<(Option<EventIndex>, SseData, Option<Filter>, Option<String>)>;
/// Run the HTTP server.
///
/// * `server_with_shutdown` is the actual server as a future which can be gracefully shut down.
/// * `server_shutdown_sender` is the channel by which the server will be notified to shut down.
/// * `data_receiver` will provide the server with local events which should then be sent to all
///   subscribed clients.
/// * `broadcaster` is used by the server to send events to each subscribed client after receiving
///   them via the `data_receiver`.
/// * `new_subscriber_info_receiver` is used to notify the server of the details of a new client
///   having subscribed to the event stream.  It allows the server to populate that client's stream
///   with the requested number of historical events.
pub(super) async fn run(
    config: Config,
    server_with_shutdown: impl Future<Output = ()> + Send + 'static,
    server_shutdown_sender: oneshot::Sender<()>,
    mut data_receiver: OutboundReceiver,
    broadcaster: broadcast::Sender<BroadcastChannelMessage>,
    mut new_subscriber_info_receiver: mpsc::UnboundedReceiver<NewSubscriberInfo>,
) {
    let server_joiner = task::spawn(server_with_shutdown);
    let mut buffer = build_buffer(config);

    // Start handling received messages from the two channels; info on new client subscribers and
    // incoming events announced by node components.
    let event_stream_fut = async {
        let mut latest_protocol_version: Option<ProtocolVersion> = None;
        loop {
            select! {
                maybe_new_subscriber = new_subscriber_info_receiver.recv() => {
                    if let Some(subscriber) = maybe_new_subscriber {
                        register_new_subscriber(subscriber, &buffer, latest_protocol_version).await;
                    }
                }
                maybe_data = data_receiver.recv() => {
                    if handle_incoming_data(maybe_data, &mut latest_protocol_version, &mut buffer, &broadcaster).await.is_err() {
                        break;
                    }
                }
            }
        }
    };
    // Wait for the event stream future to exit, which will only happen if the last `data_sender`
    // paired with `data_receiver` is dropped.  `server_joiner` will never return here.
    let _ = future::select(server_joiner, event_stream_fut.boxed()).await;
    // Kill the event-stream handlers, and shut down the server.
    let _ = broadcaster.send(BroadcastChannelMessage::Shutdown);
    let _ = server_shutdown_sender.send(());
}

fn build_buffer(
    config: Config,
) -> WheelBuf<Vec<(ProtocolVersion, ServerSentEvent)>, (ProtocolVersion, ServerSentEvent)> {
    let zero_version = ProtocolVersion::from_parts(0, 0, 0);
    WheelBuf::new(vec![
        (
            zero_version,
            ServerSentEvent::initial_event(zero_version)
        );
        config.event_stream_buffer_length as usize
    ])
}

async fn send_api_version_from_global_state(
    protocol_version: ProtocolVersion,
    subscriber: &NewSubscriberInfo,
) -> Result<(), SendError<ServerSentEvent>> {
    subscriber
        .initial_events_sender
        .send(ServerSentEvent::initial_event(protocol_version))
}

async fn send_sidecar_version(
    subscriber: &NewSubscriberInfo,
) -> Result<(), SendError<ServerSentEvent>> {
    subscriber
        .initial_events_sender
        .send(ServerSentEvent::sidecar_version_event(*SIDECAR_VERSION))
}

async fn handle_incoming_data(
    maybe_data: Option<InboundData>,
    latest_protocol_version: &mut Option<ProtocolVersion>,
    buffer: &mut WheelBuf<
        Vec<(ProtocolVersion, ServerSentEvent)>,
        (ProtocolVersion, ServerSentEvent),
    >,
    broadcaster: &broadcast::Sender<BroadcastChannelMessage>,
) -> Result<(), ()> {
    match maybe_data {
        Some((maybe_event_index, data, inbound_filter, maybe_json_data)) => {
            // Buffer the data and broadcast it to subscribed clients.
            trace!("Event stream server received {:?}", data);
            let event = ServerSentEvent {
                id: maybe_event_index,
                data: data.clone(),
                json_data: maybe_json_data,
                inbound_filter,
            };
            match data {
                SseData::ApiVersion(v) => *latest_protocol_version = Some(v),
                _ => match latest_protocol_version {
                    None => {
                        error!("Trying to buffer data without an api version observed beforehand");
                    }
                    Some(v) => {
                        buffer.push((*v, event.clone()));
                    }
                },
            };
            let message = BroadcastChannelMessage::ServerSentEvent(event);
            // This can validly fail if there are no connected clients, so don't log
            // the error.
            let _ = broadcaster.send(message);
            Ok(())
        }
        None => {
            // The data sender has been dropped - exit the loop.
            info!("shutting down HTTP server");
            Err(())
        }
    }
}

async fn register_new_subscriber(
    subscriber: NewSubscriberInfo,
    buffer: &WheelBuf<Vec<(ProtocolVersion, ServerSentEvent)>, (ProtocolVersion, ServerSentEvent)>,
    latest_protocol_version: Option<ProtocolVersion>,
) {
    let _ = send_sidecar_version(&subscriber).await;
    let mut observed_events = false;
    // If the client supplied a "start_from" index, provide the buffered events.
    // If they requested more than is buffered, just provide the whole buffer.
    if let Some(start_index) = subscriber.start_from {
        // If the buffer's first event ID is in the range [0, buffer size) or
        // (Id::MAX - buffer size, Id::MAX], then the events in the buffer are
        // considered to have their IDs wrapping round, or that was recently the
        // case.  In this case, we add `buffer.capacity()` to `start_index` and
        // the buffered events' IDs when considering which events to include in
        // the requested initial events, effectively shifting all the IDs past
        // the wrapping transition.
        let mut observed_protocol_version: Option<ProtocolVersion> = None;
        let buffer_size = buffer.capacity() as Id;
        let in_wraparound_zone = buffer
            .iter()
            .next()
            .map(|event| {
                let id = event.1.id.unwrap();
                id > Id::MAX - buffer_size || id < buffer_size
            })
            .unwrap_or_default();
        for tuple in buffer.iter().skip_while(|tuple| {
            if in_wraparound_zone {
                tuple.1.id.unwrap().wrapping_add(buffer_size)
                    < start_index.wrapping_add(buffer_size)
            } else {
                tuple.1.id.unwrap() < start_index
            }
        }) {
            // As per sending `SSE_INITIAL_EVENT`, we don't care if this errors.
            let (protocol, event) = tuple;
            // If one of the stored events belongs to a different api version than the previous one we
            // need to emit an ApiVersion event to the outbound
            if observed_protocol_version.is_none()
                || !observed_protocol_version.unwrap().eq(protocol)
            {
                let _ = subscriber
                    .initial_events_sender
                    .send(ServerSentEvent::initial_event(*protocol));
                observed_protocol_version = Some(*protocol);
            }
            let _ = subscriber.initial_events_sender.send(event.clone());
            observed_events = true;
        }
    }
    send_api_version_if_necessary(observed_events, latest_protocol_version, subscriber).await;
}

async fn send_api_version_if_necessary(
    observed_events: bool,
    latest_protocol_version: Option<ProtocolVersion>,
    subscriber: NewSubscriberInfo,
) {
    if !observed_events {
        match latest_protocol_version {
            None => {}
            Some(v) => {
                let _ = send_api_version_from_global_state(v, &subscriber).await;
            }
        }
    }
}
