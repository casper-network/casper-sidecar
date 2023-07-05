use casper_event_types::{sse_data::SseData, Filter};
use reqwest::Url;
use std::{
    fmt::{Display, Formatter},
    net::IpAddr,
};

/// Data on how to connect to a node
#[derive(Clone)]
pub struct NodeConnectionInterface {
    pub ip_address: IpAddr,
    pub sse_port: u16,
    pub rest_port: u16,
}

/// Data fot from sse connection to node which sidecar cares about.
pub struct SseEvent {
    /// Id of the message
    pub id: u32,
    /// Data parsed to a contemporary representation of SseData
    pub data: SseData,
    /// Source from which we got the message
    pub source: Url,
    /// In some cases it is required to emit the data exactly as we got it from the node.
    /// For those situations we store the exact text of the raw payload in this field.
    pub json_data: Option<String>,
    /// Info from which filter we received the message. For some events (Shutdown in particularly) we want to push only to the same outbound as we received them from so we don't duplicate.
    pub inbound_filter: Filter,
}

impl SseEvent {
    pub fn new(
        id: u32,
        data: SseData,
        mut source: Url,
        json_data: Option<String>,
        inbound_filter: Filter,
    ) -> Self {
        // This is to remove the path e.g. /events/main
        // Leaving just the IP and port
        source.set_path("");
        Self {
            id,
            data,
            source,
            json_data,
            inbound_filter,
        }
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
