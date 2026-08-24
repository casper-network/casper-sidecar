#![deny(clippy::complexity)]
#![deny(clippy::cognitive_complexity)]
#![deny(clippy::too_many_lines)]

#[cfg_attr(not(test), macro_use)]
extern crate alloc;
mod filter;
pub mod legacy_sse_data;
pub mod sse_data;
#[cfg(any(feature = "sse-data-testing", test))]
mod testing;

use std::{
    str::FromStr,
    sync::{Arc, LazyLock},
};

use casper_types::{
    Block, BlockHash, FinalitySignature, ProtocolVersion, TransactionHash, execution::ExecutionResult,
};

pub use filter::Filter;

pub static SIDECAR_VERSION: LazyLock<ProtocolVersion> = LazyLock::new(|| {
    let major: u32 = FromStr::from_str(env!("CARGO_PKG_VERSION_MAJOR")).unwrap();
    let minor: u32 = FromStr::from_str(env!("CARGO_PKG_VERSION_MINOR")).unwrap();
    let patch: u32 = FromStr::from_str(env!("CARGO_PKG_VERSION_PATCH")).unwrap();
    ProtocolVersion::from_parts(major, minor, patch)
});

#[derive(Debug, Clone)]
pub enum SidecarEvent {
    ApiVersion(ProtocolVersion),
    BlockAdded {
        block: Arc<Block>,
    },
    FinalitySignature(Box<FinalitySignature>),
    TransactionProcessed {
        transaction_hash: TransactionHash,
        block_hash: BlockHash,
        execution_result: Arc<ExecutionResult>,
    },
}
