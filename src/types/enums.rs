use super::structs::{BlockAdded, DeployAccepted, DeployProcessed, Fault, Step};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum Event {
    ApiVersion(String),
    BlockAdded(BlockAdded),
    DeployAccepted(DeployAccepted),
    DeployProcessed(DeployProcessed),
    Step(Step),
    Fault(Fault),
}

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
