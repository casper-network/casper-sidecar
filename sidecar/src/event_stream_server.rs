//! Event stream server
//!
//! The event stream server provides clients with an event-stream returning Server-Sent Events
//! (SSEs) holding JSON-encoded data.
//!
//! The actual server is run in backgrounded tasks.
//!
//! This module currently provides both halves of what is required for an API server:
//! a component implementation that interfaces with other components via being plugged into a
//! reactor, and an external facing http server that manages SSE subscriptions on a single endpoint.
//!
//! This component is passive and receives announcements made by other components while never making
//! a request of other components itself. The handled announcements are serialized to JSON and
//! pushed to subscribers.
//!
//! This component uses a ring buffer for outbound events providing some robustness against
//! unintended subscriber disconnects, if a disconnected subscriber re-subscribes before the buffer
//! has advanced past their last received event.
//!
//! For details about the SSE model and a list of supported SSEs, see:
//! <https://github.com/CasperLabs/ceps/blob/master/text/0009-client-api.md#rpcs>

mod config;
mod event_indexer;
mod http_server;
mod sse_server;
#[cfg(test)]
mod tests;

use std::{fmt::Debug, net::SocketAddr, path::PathBuf};

use serde_json::Value;
use tokio::sync::{
    mpsc::{self, UnboundedSender},
    oneshot,
};
use tracing::{info, warn};
use warp::Filter;

use casper_event_types::sse_data::SseData;

use crate::utils::{resolve_address, ListeningError};
pub use config::Config;
use event_indexer::{EventIndex, EventIndexer};
use sse_server::ChannelsAndFilter;
/// This is used to define the number of events to buffer in the tokio broadcast channel to help
/// slower clients to try to avoid missing events (See
/// <https://docs.rs/tokio/1.4.0/tokio/sync/broadcast/index.html#lagging> for further details).  The
/// resulting broadcast channel size is `ADDITIONAL_PERCENT_FOR_BROADCAST_CHANNEL_SIZE` percent
/// greater than `config.event_stream_buffer_length`.
///
/// We always want the broadcast channel size to be greater than the event stream buffer length so
/// that a new client can retrieve the entire set of buffered events if desired.
const ADDITIONAL_PERCENT_FOR_BROADCAST_CHANNEL_SIZE: u32 = 20;

#[derive(Debug)]
pub(crate) struct EventStreamServer {
    /// Channel sender to pass event-stream data to the event-stream server.
    sse_data_sender: UnboundedSender<(Option<EventIndex>, SseData, Option<serde_json::Value>)>,
    event_indexer: EventIndexer,
    // This is linted as unused because in this implementation it is only printed to the output.
    #[allow(unused)]
    listening_address: SocketAddr,
}

impl EventStreamServer {
    pub(crate) fn new(config: Config, storage_path: PathBuf) -> Result<Self, ListeningError> {
        let required_address = resolve_address(&config.address).map_err(|error| {
            warn!(
                %error,
                address=%config.address,
                "failed to start event stream server, cannot parse address"
            );
            ListeningError::ResolveAddress(error)
        })?;

        let event_indexer = EventIndexer::new(storage_path);
        let (sse_data_sender, sse_data_receiver) = mpsc::unbounded_channel();

        // Event stream channels and filter.
        let broadcast_channel_size = config.event_stream_buffer_length
            * (100 + ADDITIONAL_PERCENT_FOR_BROADCAST_CHANNEL_SIZE)
            / 100;
        let ChannelsAndFilter {
            event_broadcaster,
            new_subscriber_info_receiver,
            sse_filter,
        } = ChannelsAndFilter::new(
            broadcast_channel_size as usize,
            config.max_concurrent_subscribers,
        );

        let (shutdown_sender, shutdown_receiver) = oneshot::channel::<()>();

        let (listening_address, server_with_shutdown) =
            warp::serve(sse_filter.with(warp::cors().allow_any_origin()))
                .try_bind_with_graceful_shutdown(required_address, async {
                    shutdown_receiver.await.ok();
                })
                .map_err(|error| ListeningError::Listen {
                    address: required_address,
                    error: Box::new(error),
                })?;
        info!(address=%listening_address, "started event stream server");

        tokio::spawn(http_server::run(
            config,
            server_with_shutdown,
            shutdown_sender,
            sse_data_receiver,
            event_broadcaster,
            new_subscriber_info_receiver,
        ));

        Ok(EventStreamServer {
            sse_data_sender,
            event_indexer,
            listening_address,
        })
    }

    /// Broadcasts the SSE data to all clients connected to the event stream.
    pub(crate) fn broadcast(&mut self, sse_data: SseData, maybe_json_data: Option<Value>) {
        let event_index = match sse_data {
            SseData::ApiVersion(..) => None,
            _ => Some(self.event_indexer.next_index()),
        };
        let _ = self
            .sse_data_sender
            .send((event_index, sse_data, maybe_json_data));
    }
}

impl Drop for EventStreamServer {
    fn drop(&mut self) {
        self.broadcast(SseData::Shutdown, None);
    }
}
