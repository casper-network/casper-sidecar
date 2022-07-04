use casper_node::types::{BlockHash, DeployHash};
use casper_types::Timestamp;
use serde::Deserialize;
use crate::DeployProcessed;

#[derive(Deserialize)]
pub enum Network {
    Mainnet,
    Testnet,
    Local,
}

impl Network {
    pub fn as_str(&self) -> &'static str {
        match self {
            Network::Mainnet => "Mainnet",
            Network::Testnet => "Testnet",
            Network::Local => "Local",
        }
    }
}

pub enum DeployAtState {
    Accepted((DeployHash, Timestamp)),
    Processed(DeployProcessed),
    /// A list of all added DeployHashes and the Block Hash they belong to.
    Added((Vec<DeployHash>, BlockHash)),
    Expired(DeployHash)
}
