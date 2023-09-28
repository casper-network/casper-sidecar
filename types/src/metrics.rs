use once_cell::sync::Lazy;
use prometheus::{GaugeVec, HistogramOpts, HistogramVec, IntCounterVec, Opts, Registry};
#[cfg(feature = "additional-metrics")]
const DB_OPERATION_BUCKETS: &[f64; 8] = &[
    3e+5_f64, 3e+6_f64, 10e+6_f64, 20e+6_f64, 5e+7_f64, 1e+8_f64, 5e+8_f64, 1e+9_f64,
];
const BUCKETS: &[f64; 8] = &[
    5e+2_f64, 1e+3_f64, 2e+3_f64, 5e+3_f64, 5e+4_f64, 5e+5_f64, 5e+6_f64, 5e+7_f64,
];

static REGISTRY: Lazy<Registry> = Lazy::new(Registry::new);
pub static ERROR_COUNTS: Lazy<IntCounterVec> = Lazy::new(|| {
    let counter = IntCounterVec::new(
        Opts::new("error_counts", "Error counts"),
        &["category", "description"],
    )
    .unwrap();
    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("cannot register metric");
    counter
});
pub static RECEIVED_BYTES: Lazy<HistogramVec> = Lazy::new(|| {
    let counter = HistogramVec::new(
        HistogramOpts {
            common_opts: Opts::new("received_bytes", "Received bytes"),
            buckets: Vec::from(BUCKETS as &'static [f64]),
        },
        &["filter"],
    )
    .unwrap();
    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("cannot register metric");
    counter
});
pub static INTERNAL_EVENTS: Lazy<IntCounterVec> = Lazy::new(|| {
    let counter = IntCounterVec::new(
        Opts::new("internal_events", "Count of internal events"),
        &["category", "description"],
    )
    .expect("metric can't be created");
    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("cannot register metric");
    counter
});
pub static NODE_STATUSES: Lazy<GaugeVec> = Lazy::new(|| {
    let counter = GaugeVec::new(
        Opts::new("node_statuses", "Current status of node to which sidecar is connected. Numbers mean: 0 - preparing; 1 - connecting; 2 - connected; 3 - reconnecting; -1 - defunct (used up all connection attempts)"),
        &["node"]
    )
    .expect("metric can't be created");
    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("cannot register metric");
    counter
});

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

pub struct MetricCollectionError {
    reason: String,
}

impl ToString for MetricCollectionError {
    fn to_string(&self) -> String {
        format!("MetricCollectionError: {}", self.reason)
    }
}

impl MetricCollectionError {
    fn new(reason: String) -> Self {
        MetricCollectionError { reason }
    }
}

pub fn metrics_summary() -> Result<String, MetricCollectionError> {
    use prometheus::Encoder;
    let encoder = prometheus::TextEncoder::new();
    let mut buffer = Vec::new();
    if let Err(e) = encoder.encode(&REGISTRY.gather(), &mut buffer) {
        return Err(MetricCollectionError::new(format!(
            "could not encode custom metrics: {}",
            e
        )));
    };
    let mut res = match String::from_utf8(buffer.clone()) {
        Ok(v) => v,
        Err(e) => {
            return Err(MetricCollectionError::new(format!(
                "custom metrics have a non-utf8 character: {}",
                e
            )));
        }
    };
    buffer.clear();

    let mut buffer = Vec::new();
    if let Err(e) = encoder.encode(&prometheus::gather(), &mut buffer) {
        return Err(MetricCollectionError::new(format!(
            "error when encoding default prometheus metrics: {}",
            e
        )));
    };
    let res_custom = match String::from_utf8(buffer.clone()) {
        Ok(v) => v,
        Err(e) => {
            return Err(MetricCollectionError::new(format!(
                "Default and custom metrics have a non-utf8 character: {}",
                e
            )))
        }
    };
    buffer.clear();
    res.push_str(&res_custom);
    Ok(res)
}
