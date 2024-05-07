#![deny(clippy::complexity)]
#![deny(clippy::cognitive_complexity)]
#![deny(clippy::too_many_lines)]

#[cfg_attr(not(test), macro_use)]
extern crate alloc;
mod filter;
pub mod legacy_sse_data;
pub mod sse_data;
#[cfg(feature = "sse-data-testing")]
mod testing;

use casper_types::ProtocolVersion;
pub use filter::Filter;
use std::str::FromStr;

use once_cell::sync::Lazy;
pub static SIDECAR_VERSION: Lazy<ProtocolVersion> = Lazy::new(|| {
    let major: u32 = FromStr::from_str(env!("CARGO_PKG_VERSION_MAJOR")).unwrap();
    let minor: u32 = FromStr::from_str(env!("CARGO_PKG_VERSION_MINOR")).unwrap();
    let patch: u32 = FromStr::from_str(env!("CARGO_PKG_VERSION_PATCH")).unwrap();
    ProtocolVersion::from_parts(major, minor, patch)
});
