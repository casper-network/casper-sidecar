use std::sync::Arc;

use serde::Deserialize;

use casper_node::types::{Deploy, DeployHash};

use crate::types::structs::DeployProcessed;

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
    Accepted(Arc<Deploy>),
    Processed(DeployProcessed),
    Expired(DeployHash),
}
