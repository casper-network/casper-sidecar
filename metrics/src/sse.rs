use once_cell::sync::Lazy;
use prometheus::{GaugeVec, HistogramOpts, HistogramVec, Opts};

use super::REGISTRY;

const BUCKETS: &[f64; 8] = &[
    5e+2_f64, 1e+3_f64, 2e+3_f64, 5e+3_f64, 5e+4_f64, 5e+5_f64, 5e+6_f64, 5e+7_f64,
];

static NODE_STATUSES: Lazy<GaugeVec> = Lazy::new(|| {
    let counter = GaugeVec::new(
        Opts::new("node_statuses", "Current status of node to which sidecar is connected. Numbers mean: 0 - preparing; 1 - connecting; 2 - connected; 3 - reconnecting; -1 - connections_exhausted -> used up all connection attempts ; -2 - incompatible -> node is in an incompatible version"),
        &["node"]
    )
    .expect("metric can't be created");
    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("cannot register metric");
    counter
});

static RECEIVED_BYTES: Lazy<HistogramVec> = Lazy::new(|| {
    let counter = HistogramVec::new(
        HistogramOpts {
            common_opts: Opts::new(
                "sse_server_received_bytes",
                "Received bytes from SSE inbound per message type",
            ),
            buckets: Vec::from(BUCKETS as &'static [f64]),
        },
        &["message_type"],
    )
    .unwrap();
    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("cannot register metric");
    counter
});

static NUMBER_OF_RECEIVED_CONTRACT_MESSAGES: Lazy<GaugeVec> = Lazy::new(|| {
    let counter = GaugeVec::new(
        Opts::new(
            "sse_server_received_contract_messages",
            "Number of messages received in TransactionProcessed events.",
        ),
        &["label"],
    )
    .expect("cannot sse_server_received_contract_messages metric");
    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("cannot register sse_server_received_contract_messages metric");
    counter
});

pub fn observe_contract_messages(label: &str, number_of_messages: usize) {
    NUMBER_OF_RECEIVED_CONTRACT_MESSAGES
        .with_label_values(&[label])
        .add(number_of_messages as f64);
}

pub fn register_sse_message_size(sse_message_type: &str, payload_size: f64) {
    RECEIVED_BYTES
        .with_label_values(&[sse_message_type])
        .observe(payload_size);
}

pub fn store_node_status(node_label: &str, status: f64) {
    NODE_STATUSES.with_label_values(&[node_label]).set(status);
}
