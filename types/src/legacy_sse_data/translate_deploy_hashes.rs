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
            .small()
            .chain(block_body_v2.medium())
            .chain(block_body_v2.large())
            .filter_map(|el| match el {
                TransactionHash::Deploy(deploy_hash) => Some(deploy_hash),
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
                TransactionHash::Deploy(deploy_hash) => Some(deploy_hash),
                TransactionHash::V1(_) => None,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use casper_types::{testing::TestRng, EraId};

    use crate::legacy_sse_data::fixtures::*;

    use super::*;

    #[test]
    fn standard_deploy_hashes_translator_uses_standard_deploy_transaction_hashes() {
        let mut test_rng = TestRng::new();
        let under_test = StandardDeployHashesTranslator;
        let (
            transactions,
            standard_deploy_hash,
            _standard_v1_hash,
            _mint_deploy_hash,
            _mint_v1_hash,
            _install_upgrade_v1,
            _auction_v1,
        ) = sample_transactions(&mut test_rng);
        let block_v2 = block_v2_with_transactions(
            &mut test_rng,
            parent_hash(),
            state_root_hash(),
            timestamp(),
            EraId::new(15678276),
            345678987,
            proposer(),
            transactions.iter().collect(),
        );
        let block_body = block_v2.body();
        assert_eq!(block_body.all_transactions().collect::<Vec<_>>().len(), 6);
        let translated = under_test.translate(block_body);
        assert_eq!(translated, vec![standard_deploy_hash,])
    }

    #[test]
    fn transfer_deploy_hashes_translator_uses_mint_deploy_transaction_hashes() {
        let mut test_rng = TestRng::new();
        let under_test = TransferDeployHashesTranslator;
        let (
            transactions,
            _standard_deploy_hash,
            _standard_v1_hash,
            mint_deploy_hash,
            _mint_v1_hash,
            _install_upgrade_v1,
            _auction_v1,
        ) = sample_transactions(&mut test_rng);
        let block_v2 = block_v2_with_transactions(
            &mut test_rng,
            parent_hash(),
            state_root_hash(),
            timestamp(),
            EraId::new(15678276),
            345678987,
            proposer(),
            transactions.iter().collect(),
        );
        let block_body = block_v2.body();
        assert_eq!(block_body.all_transactions().collect::<Vec<_>>().len(), 6);
        let translated = under_test.translate(block_body);
        assert_eq!(translated, vec![mint_deploy_hash,])
    }
}
