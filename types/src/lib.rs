#![deny(clippy::complexity)]
#![deny(clippy::cognitive_complexity)]
#![deny(clippy::too_many_lines)]

#[cfg_attr(not(test), macro_use)]
extern crate alloc;
mod filter;
pub mod metrics;
pub mod sse_data;
#[cfg(feature = "sse-data-testing")]
mod testing;

pub use filter::Filter;
