use std::time::Duration;

use super::REGISTRY;
use once_cell::sync::Lazy;
use prometheus::{Histogram, HistogramOpts, HistogramVec, IntCounterVec, IntGauge, Opts};

const RESPONSE_SIZE_BUCKETS: &[f64; 8] = &[
    5e+2_f64, 1e+3_f64, 2e+3_f64, 5e+3_f64, 5e+4_f64, 5e+5_f64, 5e+6_f64, 5e+7_f64,
];

const RESPONSE_TIME_MS_BUCKETS: &[f64; 8] = &[
    1_f64, 5_f64, 10_f64, 30_f64, 50_f64, 100_f64, 200_f64, 300_f64,
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

static RESPONSE_TIMES_MS: Lazy<HistogramVec> = Lazy::new(|| {
    let histogram = HistogramVec::new(
        HistogramOpts {
            common_opts: Opts::new(
                "rpc_server_endpoint_response_times",
                "Time it takes the service to produce a response in milliseconds",
            ),
            buckets: Vec::from(RESPONSE_TIME_MS_BUCKETS as &'static [f64]),
        },
        &["method", "status"],
    )
    .expect("rpc_server_endpoint_response_times metric can't be created");
    REGISTRY
        .register(Box::new(histogram.clone()))
        .expect("cannot register metric");
    histogram
});

static RECONNECT_TIMES_MS: Lazy<Histogram> = Lazy::new(|| {
    let opts = HistogramOpts::new(
        "rpc_server_reconnect_time",
        "Time it takes the service to reconnect to node binary port in milliseconds",
    )
    .buckets(RESPONSE_TIME_MS_BUCKETS.to_vec());
    let histogram =
        Histogram::with_opts(opts).expect("rpc_server_reconnect_time metric can't be created");
    REGISTRY
        .register(Box::new(histogram.clone()))
        .expect("cannot register metric");
    histogram
});

static DISCONNECT_EVENTS: Lazy<IntGauge> = Lazy::new(|| {
    let counter = IntGauge::new(
        "rpc_server_disconnects",
        "Number of TCP disconnects between sidecar and nodes binary port",
    )
    .expect("rpc_server_disconnects metric can't be created");
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

pub fn observe_response_time(method: &str, status: &str, response_time: Duration) {
    let response_time = response_time.as_secs_f64() * 1000.0;
    RESPONSE_TIMES_MS
        .with_label_values(&[method, status])
        .observe(response_time);
}

pub fn observe_reconnect_time(response_time: Duration) {
    let response_time = response_time.as_secs_f64() * 1000.0;
    RECONNECT_TIMES_MS.observe(response_time);
}

pub fn inc_disconnect() {
    DISCONNECT_EVENTS.inc();
}

pub fn register_request_size(method: &str, payload_size: f64) {
    ENDPOINT_REQUEST_BYTES
        .with_label_values(&[method])
        .observe(payload_size);
}
