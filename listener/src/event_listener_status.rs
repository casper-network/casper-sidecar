use metrics::sse::store_node_status;

/// Helper enum determining in what state connection to a node is in.
/// It's used to named different situations in which the connection can be.
pub(super) enum EventListenerStatus {
    /// Event Listener has not yet started to attempt the connection
    Preparing,
    /// Event Listener started establishing relevant sse connections to filters of the node
    Connecting,
    /// Event Listener got data from at least one of the nodes sse connections.
    Connected,
    /// For some reason Event Listener lost connection to the node and is trying to establish it again
    Reconnecting,
    /// If Event Listener reports this state it means that it was unable to establish a connection
    /// with node and there are no more `max_connection_attempts` left. There will be no futhrer
    /// tries to establish the connection.
    ReconnectionsExhausted,
    /// If Event Listener reports this state it means that the node it was trying to connect to has a
    /// version which sidecar can't work with
    IncompatibleVersion,
}

impl EventListenerStatus {
    pub(super) fn log_status(&self, node_address: &str, sse_port: u16) {
        let status = match self {
            EventListenerStatus::Preparing => 0.0,
            EventListenerStatus::Connecting => 1.0,
            EventListenerStatus::Connected => 2.0,
            EventListenerStatus::Reconnecting => 3.0,
            EventListenerStatus::ReconnectionsExhausted => -1.0,
            EventListenerStatus::IncompatibleVersion => -2.0,
        };
        let node_label = format!("{node_address}:{sse_port}");
        store_node_status(node_label.as_str(), status);
    }
}
