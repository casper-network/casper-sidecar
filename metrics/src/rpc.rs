use std::{sync::LazyLock, time::Duration};

use super::REGISTRY;
use prometheus::{
    Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, Opts,
};

const RESPONSE_SIZE_BUCKETS: &[f64; 8] = &[
    5e+2_f64, 1e+3_f64, 2e+3_f64, 5e+3_f64, 5e+4_f64, 5e+5_f64, 5e+6_f64, 5e+7_f64,
];

const RESPONSE_TIME_MS_BUCKETS: &[f64; 9] = &[
    1_f64, 5_f64, 10_f64, 30_f64, 50_f64, 100_f64, 300_f64, 1000_f64, 3000_f64,
];

static ENDPOINT_CALLS: LazyLock<IntCounterVec> = LazyLock::new(|| {
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

static BINARY_PORT_CALLS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    let counter = IntCounterVec::new(
        Opts::new(
            "rpc_server_binary_port_calls_total",
            "Total number of binary port RPC calls actually dispatched to the node, \
             labelled by outcome (`success` = a response was received from the node, \
             `failure` = the call did not complete, e.g. timeout or lost connection). \
             Requests served from the binary port cache are not counted.",
        ),
        &["outcome"],
    )
    .unwrap();
    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("cannot register metric");
    counter
});

static TIMEOUT_COUNTERS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    let counter = IntCounterVec::new(
        Opts::new(
            "rpc_server_timeout_counts",
            "Counters for how many of the requests failed due to internal timeout",
        ),
        &["timer"],
    )
    .unwrap();
    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("cannot register metric");
    counter
});

static RESPONSE_TIMES_MS: LazyLock<HistogramVec> = LazyLock::new(|| {
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

static RECONNECT_TIMES_MS: LazyLock<Histogram> = LazyLock::new(|| {
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

static MISMATCHED_IDS: LazyLock<IntGauge> = LazyLock::new(|| {
    let counter = IntGauge::new(
        "rpc_server_mismatched_ids",
        "Number of mismatched ID events observed in responses from binary port",
    )
    .expect("rpc_server_mismatched_ids metric can't be created");
    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("cannot register metric");
    counter
});

static DISCONNECT_EVENTS: LazyLock<IntGauge> = LazyLock::new(|| {
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

static ENDPOINT_REQUEST_BYTES: LazyLock<HistogramVec> = LazyLock::new(|| {
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

static BATCH_LENGTHS: LazyLock<Histogram> = LazyLock::new(|| {
    let histogram = Histogram::with_opts(
        HistogramOpts::new(
            "rpc_server_batch_lengths",
            "Number of entries in JSON-RPC batches",
        )
        .buckets(vec![1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 200.0]),
    )
    .expect("rpc_server_batch_lengths metric can't be created");
    REGISTRY
        .register(Box::new(histogram.clone()))
        .expect("cannot register metric");
    histogram
});

static BATCH_RESPONSE_BYTES: LazyLock<Histogram> = LazyLock::new(|| {
    let histogram = Histogram::with_opts(
        HistogramOpts::new(
            "rpc_server_batch_response_bytes",
            "Serialized JSON-RPC batch response sizes",
        )
        .buckets(RESPONSE_SIZE_BUCKETS.to_vec()),
    )
    .expect("rpc_server_batch_response_bytes metric can't be created");
    REGISTRY
        .register(Box::new(histogram.clone()))
        .expect("cannot register metric");
    histogram
});

static BATCH_COUNT_LIMIT_REJECTIONS: LazyLock<IntCounter> = LazyLock::new(|| {
    let counter = IntCounter::new(
        "rpc_server_batch_count_limit_rejections",
        "JSON-RPC batches rejected for exceeding the item limit",
    )
    .expect("rpc_server_batch_count_limit_rejections metric can't be created");
    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("cannot register metric");
    counter
});

static BATCH_RESPONSE_LIMIT_TRUNCATIONS: LazyLock<IntCounter> = LazyLock::new(|| {
    let counter = IntCounter::new(
        "rpc_server_batch_response_limit_truncations",
        "JSON-RPC batch responses truncated by the soft response-size limit",
    )
    .expect("rpc_server_batch_response_limit_truncations metric can't be created");
    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("cannot register metric");
    counter
});

pub fn inc_method_call(method: &str) {
    ENDPOINT_CALLS.with_label_values(&[method]).inc();
}

/// Records a single binary port RPC call that was actually dispatched to the node
/// (i.e. not served from the cache). `success` is `true` when a response was
/// received, `false` when the call failed to complete.
pub fn inc_binary_port_call(success: bool) {
    let outcome = if success { "success" } else { "failure" };
    BINARY_PORT_CALLS.with_label_values(&[outcome]).inc();
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

pub fn register_request_size(method: &str, payload_size: usize) {
    ENDPOINT_REQUEST_BYTES
        .with_label_values(&[method])
        .observe(payload_size as f64);
}

pub fn register_timeout(timer_name: &str) {
    TIMEOUT_COUNTERS.with_label_values(&[timer_name]).inc();
}

pub fn register_mismatched_id() {
    MISMATCHED_IDS.inc();
}

pub fn observe_batch_length(length: usize) {
    BATCH_LENGTHS.observe(length as f64);
}

pub fn observe_batch_response_bytes(response_bytes: u64) {
    BATCH_RESPONSE_BYTES.observe(response_bytes as f64);
}

pub fn inc_batch_count_limit_rejection() {
    BATCH_COUNT_LIMIT_REJECTIONS.inc();
}

pub fn inc_batch_response_limit_truncation() {
    BATCH_RESPONSE_LIMIT_TRUNCATIONS.inc();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binary_port_call_counter_splits_by_outcome() {
        let ok_before = BINARY_PORT_CALLS.with_label_values(&["success"]).get();
        let err_before = BINARY_PORT_CALLS.with_label_values(&["failure"]).get();

        inc_binary_port_call(true);
        inc_binary_port_call(true);
        inc_binary_port_call(false);

        assert_eq!(
            BINARY_PORT_CALLS.with_label_values(&["success"]).get(),
            ok_before + 2
        );
        assert_eq!(
            BINARY_PORT_CALLS.with_label_values(&["failure"]).get(),
            err_before + 1
        );

        let summary = match crate::metrics_summary() {
            Ok(s) => s,
            Err(e) => panic!("metrics_summary failed: {}", e),
        };
        assert!(summary.contains("rpc_server_binary_port_calls_total"));
    }
}
