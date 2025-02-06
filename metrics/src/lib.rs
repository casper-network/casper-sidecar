pub mod metrics;
pub use metrics::{metrics_summary, observe_error, MetricCollectionError};
pub mod db;
pub mod rest_api;
pub mod rpc;
pub mod sse;

use metrics::REGISTRY;
