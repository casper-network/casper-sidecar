use super::REGISTRY;
use once_cell::sync::Lazy;
#[cfg(feature = "additional-metrics")]
use prometheus::GaugeVec;
use prometheus::{HistogramOpts, HistogramVec, Opts};

const RAW_DATA_SIZE_BUCKETS: &[f64; 8] = &[
    5e+2_f64, 1e+3_f64, 2e+3_f64, 5e+3_f64, 5e+4_f64, 5e+5_f64, 5e+6_f64, 5e+7_f64,
];

static FETCHED_RAW_DATA_SIZE: Lazy<HistogramVec> = Lazy::new(|| {
    let counter = HistogramVec::new(
        HistogramOpts {
            common_opts: Opts::new(
                "db_raw_data_size",
                "Size (in bytes) of raw data fetched from the database.",
            ),
            buckets: Vec::from(RAW_DATA_SIZE_BUCKETS as &'static [f64]),
        },
        &["message_type"],
    )
    .expect("db_raw_data_size metric can't be created");
    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("cannot register metric");
    counter
});

pub fn observe_raw_data_size(message_type: &str, bytes: usize) {
    FETCHED_RAW_DATA_SIZE
        .with_label_values(&[message_type])
        .observe(bytes as f64);
}

#[cfg(feature = "additional-metrics")]
const DB_OPERATION_BUCKETS: &[f64; 8] = &[
    3e+5_f64, 3e+6_f64, 10e+6_f64, 20e+6_f64, 5e+7_f64, 1e+8_f64, 5e+8_f64, 1e+9_f64,
];

#[cfg(feature = "additional-metrics")]
pub static DB_OPERATION_TIMES: Lazy<HistogramVec> = Lazy::new(|| {
    let counter = HistogramVec::new(
        HistogramOpts {
            common_opts: Opts::new(
                "db_operation_times",
                "Times (in nanoseconds) it took to perform a database operation.",
            ),
            buckets: Vec::from(DB_OPERATION_BUCKETS as &'static [f64]),
        },
        &["filter"],
    )
    .expect("metric can't be created");
    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("cannot register metric");
    counter
});
#[cfg(feature = "additional-metrics")]
pub static EVENTS_PROCESSED_PER_SECOND: Lazy<GaugeVec> = Lazy::new(|| {
    let counter = GaugeVec::new(
    Opts::new("events_processed", "Events processed by sidecar. Split by \"module\" which should be either \"inbound\" or \"outbound\". \"Inbound\" means the number of events per second which were read from node endpoints and persisted in DB. \"Outbound\" means number of events pushed to clients."),
    &["module"]
)
.expect("metric can't be created");
    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("cannot register metric");
    counter
});
