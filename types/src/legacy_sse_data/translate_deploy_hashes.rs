use casper_types::{BlockBodyV2, DeployHash, TransactionHash};
use mockall::automock;

#[automock]
pub trait DeployHashTranslator {
    fn translate(&self, block_body_v2: &BlockBodyV2) -> Vec<DeployHash>;
}

#[derive(Default)]
pub struct StandardDeployHashesTranslator;

#[derive(Default)]
pub struct TransferDeployHashesTranslator;

impl DeployHashTranslator for StandardDeployHashesTranslator {
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

impl DeployHashTranslator for TransferDeployHashesTranslator {
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

#[cfg(test)]
mod tests {
    use crate::{legacy_sse_data::fixtures::block_v2, testing::parse_deploy_hash};

    use super::*;

    #[test]
    fn standard_deploy_hashes_translator_uses_standard_deploy_transaction_hashes() {
        let under_test = StandardDeployHashesTranslator;
        let block_v2 = block_v2();
        let block_body = block_v2.body();
        let translated = under_test.translate(block_body);
        assert_eq!(
            translated,
            vec![
                parse_deploy_hash(
                    "e185793e2a6214542ffee6de0ede37d7dd9748b429e4586d73fd2abdd100bd7c"
                ),
                parse_deploy_hash(
                    "c4f7acd014ef88af95ebf338e8dd29b95b161a6a812a6764112bf9d09abc399a"
                )
            ]
        )
    }
    #[test]
    fn transfer_deploy_hashes_translator_uses_mint_deploy_transaction_hashes() {
        let under_test = TransferDeployHashesTranslator;
        let block_v2 = block_v2();
        let block_body = block_v2.body();
        let translated = under_test.translate(block_body);
        assert_eq!(
            translated,
            vec![
                parse_deploy_hash(
                    "19cd7acc75ffe58e6dd5f3f1a6b7c08f8d02bf47928926054d4818e6eb41ca74"
                ),
                parse_deploy_hash(
                    "5e50ebcf0190ef2be4182fe7940f4d68dde8210f42c75ca9478fc1be765c5751"
                )
            ]
        )
    }
}
