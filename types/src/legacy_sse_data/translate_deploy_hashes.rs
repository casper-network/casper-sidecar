use casper_types::{BlockBodyV2, DeployHash, TransactionHash};
use mockall::automock;

#[automock]
pub trait DeployHashTranslator {
    fn translate(&self, block_body_v2: &BlockBodyV2) -> Vec<DeployHash>;
}

#[derive(Default)]
pub struct DefaultDeployHashTranslator;

#[derive(Default)]
pub struct TransferDeployHashTranslator;

impl DeployHashTranslator for DefaultDeployHashTranslator {
    fn translate(&self, block_body_v2: &casper_types::BlockBodyV2) -> Vec<DeployHash> {
        block_body_v2
            .standard()
            .filter_map(|el| match el {
                TransactionHash::Deploy(deploy_hash) => Some(*deploy_hash),
                TransactionHash::V1(_) => None,
            })
            .collect()
    }
}

impl DeployHashTranslator for TransferDeployHashTranslator {
    fn translate(&self, block_body_v2: &casper_types::BlockBodyV2) -> Vec<DeployHash> {
        block_body_v2
            .mint()
            .filter_map(|el| match el {
                TransactionHash::Deploy(deploy_hash) => Some(*deploy_hash),
                TransactionHash::V1(_) => None,
            })
            .collect()
    }
}
