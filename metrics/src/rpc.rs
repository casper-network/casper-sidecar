use super::REGISTRY;
use once_cell::sync::Lazy;
use prometheus::{HistogramOpts, HistogramVec, IntCounterVec, Opts};

const RESPONSE_SIZE_BUCKETS: &[f64; 8] = &[
    5e+2_f64, 1e+3_f64, 2e+3_f64, 5e+3_f64, 5e+4_f64, 5e+5_f64, 5e+6_f64, 5e+7_f64,
];

static ENDPOINT_CALLS: Lazy<IntCounterVec> = Lazy::new(|| {
    let counter = IntCounterVec::new(
        Opts::new("rpc_server_endpoint_calls", "Endpoint calls"),
        &["endpoint_name"],
    )
    .unwrap();
    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("cannot register metric");
    counter
});

static ENDPOINT_RESPONSES: Lazy<IntCounterVec> = Lazy::new(|| {
    let counter = IntCounterVec::new(
        Opts::new("rpc_server_endpoint_responses", "Endpoint responses"),
        &["endpoint", "status"],
    )
    .unwrap();
    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("cannot register metric");
    counter
});

static ENDPOINT_REQUEST_BYTES: Lazy<HistogramVec> = Lazy::new(|| {
    let counter = HistogramVec::new(
        HistogramOpts {
            common_opts: Opts::new("rpc_server_request_sizes", "Endpoint request sizes"),
            buckets: Vec::from(RESPONSE_SIZE_BUCKETS as &'static [f64]),
        },
        &["endpoint"],
    )
    .unwrap();
    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("cannot register metric");
    counter
});

pub fn inc_method_call(method: &str) {
    ENDPOINT_CALLS.with_label_values(&[method]).inc();
}

pub fn inc_result(method: &str, status: &str) {
    ENDPOINT_RESPONSES
        .with_label_values(&[method, &status])
        .inc();
}

pub fn register_request_size(method: &str, payload_size: f64) {
    ENDPOINT_REQUEST_BYTES
        .with_label_values(&[method])
        .observe(payload_size);
}
