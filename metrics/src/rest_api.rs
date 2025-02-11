use super::REGISTRY;
use once_cell::sync::Lazy;
use prometheus::{HistogramOpts, HistogramVec, IntGauge, Opts};
use std::time::Duration;

const RESPONSE_TIME_MS_BUCKETS: &[f64; 8] = &[
    1_f64, 5_f64, 10_f64, 30_f64, 50_f64, 100_f64, 200_f64, 300_f64,
];

static CONNECTED_CLIENTS: Lazy<IntGauge> = Lazy::new(|| {
    let counter = IntGauge::new("rest_api_connected_clients", "Connected Clients")
        .expect("rest_api_connected_clients metric can't be created");
    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("cannot register metric");
    counter
});

static RESPONSE_TIMES_MS: Lazy<HistogramVec> = Lazy::new(|| {
    let counter = HistogramVec::new(
        HistogramOpts {
            common_opts: Opts::new(
                "rest_api_response_times",
                "Time it takes the service to produce a response in milliseconds",
            ),
            buckets: Vec::from(RESPONSE_TIME_MS_BUCKETS as &'static [f64]),
        },
        &["label", "status"],
    )
    .expect("rest_api_response_times metric can't be created");
    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("cannot register metric");
    counter
});

pub fn inc_connected_clients() {
    CONNECTED_CLIENTS.inc();
}

pub fn dec_connected_clients() {
    CONNECTED_CLIENTS.dec();
}

pub fn observe_response_time(label: &str, status: &str, response_time: Duration) {
    let response_time = response_time.as_secs_f64() * 1000.0;
    RESPONSE_TIMES_MS
        .with_label_values(&[label, status])
        .observe(response_time);
}
