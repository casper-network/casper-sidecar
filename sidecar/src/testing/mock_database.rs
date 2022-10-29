use async_trait::async_trait;
use rand::Rng;

use casper_node::types::{BlockHash, FinalitySignature as FinSig};
use casper_types::testing::TestRng;

use crate::types::{
    database::{DatabaseReadError, DatabaseReader, DeployAggregate},
    sse_events::*,
};

#[derive(Clone)]
pub struct MockDatabase;

impl MockDatabase {
    pub(crate) fn new() -> Self {
        MockDatabase
    }
}

#[async_trait]
impl DatabaseReader for MockDatabase {
    async fn get_latest_block(&self) -> Result<BlockAdded, DatabaseReadError> {
        let mut test_rng = TestRng::new();

        let block_added = BlockAdded::random(&mut test_rng);

        Ok(block_added)
    }

    async fn get_block_by_height(&self, height: u64) -> Result<BlockAdded, DatabaseReadError> {
        let mut test_rng = TestRng::new();

        let block_added = BlockAdded::random_with_height(&mut test_rng, height);

        Ok(block_added)
    }

    async fn get_block_by_hash(&self, hash: &str) -> Result<BlockAdded, DatabaseReadError> {
        let mut test_rng = TestRng::new();

        let block_added = BlockAdded::random_with_hash(&mut test_rng, hash.to_string());

        Ok(block_added)
    }

    async fn get_latest_deploy_aggregate(&self) -> Result<DeployAggregate, DatabaseReadError> {
        let mut test_rng = TestRng::new();

        let deploy_aggregate = DeployAggregate::random(&mut test_rng, None);

        Ok(deploy_aggregate)
    }

    async fn get_deploy_aggregate_by_hash(
        &self,
        hash: &str,
    ) -> Result<DeployAggregate, DatabaseReadError> {
        let mut test_rng = TestRng::new();

        let deploy_aggregate = DeployAggregate::random(&mut test_rng, Some(hash.to_string()));

        Ok(deploy_aggregate)
    }

    #[allow(unused)]
    async fn get_deploy_accepted_by_hash(
        &self,
        hash: &str,
    ) -> Result<DeployAccepted, DatabaseReadError> {
        let mut test_rng = TestRng::new();

        let deploy_accepted = DeployAccepted::random(&mut test_rng);

        Ok(deploy_accepted)
    }

    #[allow(unused)]
    async fn get_deploy_processed_by_hash(
        &self,
        hash: &str,
    ) -> Result<DeployProcessed, DatabaseReadError> {
        let mut test_rng = TestRng::new();

        let deploy_processed = DeployProcessed::random(&mut test_rng, None);

        Ok(deploy_processed)
    }

    #[allow(unused)]
    async fn get_deploy_expired_by_hash(&self, hash: &str) -> Result<bool, DatabaseReadError> {
        Ok(true)
    }

    #[allow(unused)]
    async fn get_step_by_era(&self, era: u64) -> Result<Step, DatabaseReadError> {
        let mut test_rng = TestRng::new();

        let step = Step::random(&mut test_rng);

        Ok(step)
    }

    #[allow(unused)]
    async fn get_faults_by_public_key(
        &self,
        public_key: &str,
    ) -> Result<Vec<Fault>, DatabaseReadError> {
        let mut test_rng = TestRng::new();

        let fault_one = Fault::random(&mut test_rng);
        let fault_two = Fault::random(&mut test_rng);
        let fault_three = Fault::random(&mut test_rng);

        let faults = vec![fault_one, fault_two, fault_three];

        Ok(faults)
    }

    #[allow(unused)]
    async fn get_faults_by_era(&self, era: u64) -> Result<Vec<Fault>, DatabaseReadError> {
        let mut test_rng = TestRng::new();

        let fault_one = Fault::random(&mut test_rng);
        let fault_two = Fault::random(&mut test_rng);
        let fault_three = Fault::random(&mut test_rng);

        let faults = vec![fault_one, fault_two, fault_three];

        Ok(faults)
    }

    #[allow(unused)]
    async fn get_finality_signatures_by_block(
        &self,
        block_hash: &str,
    ) -> Result<Vec<FinSig>, DatabaseReadError> {
        let mut test_rng = TestRng::new();

        let finality_signature_one =
            FinSig::random_for_block(BlockHash::random(&mut test_rng), test_rng.gen());
        let finality_signature_two =
            FinSig::random_for_block(BlockHash::random(&mut test_rng), test_rng.gen());
        let finality_signature_three =
            FinSig::random_for_block(BlockHash::random(&mut test_rng), test_rng.gen());

        let finality_signatures = vec![
            finality_signature_one,
            finality_signature_two,
            finality_signature_three,
        ];

        Ok(finality_signatures)
    }
}
