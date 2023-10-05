#![deny(clippy::complexity)]
#![deny(clippy::cognitive_complexity)]
#![deny(clippy::too_many_lines)]

#[cfg_attr(not(test), macro_use)]
extern crate alloc;
pub mod block;
pub mod deploy;
mod digest;
mod executable_deploy_item;
mod filter;
pub mod metrics;
pub mod sse_data;
#[cfg(feature = "sse-data-testing")]
mod testing;

pub use crate::executable_deploy_item::ExecutableDeployItem;
pub use block::{json_compatibility::JsonBlock, Block, BlockHash, FinalitySignature};
pub use deploy::{Deploy, DeployHash};
pub use digest::Digest;
pub use filter::Filter;
