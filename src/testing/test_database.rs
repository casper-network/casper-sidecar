use anyhow::Error;
use async_trait::async_trait;
use casper_node::types::{Block, Deploy};
use casper_types::testing::TestRng;
use casper_types::{ExecutionEffect, ExecutionResult, PublicKey, Timestamp};
use lazy_static::lazy_static;
use rand::Rng;
use std::borrow::BorrowMut;
use std::sync::Mutex;

use crate::sqlite_db::{AggregateDeployInfo, DatabaseReader, DatabaseRequestError};
use crate::types::structs::{DeployProcessed, Fault, Step};

lazy_static! {
    static ref TEST_RNG: Mutex<TestRng> = Mutex::new(TestRng::new());
}

#[derive(Clone)]
pub struct MockDatabase;

#[async_trait]
impl DatabaseReader for MockDatabase {
    async fn get_latest_block(&self) -> Result<Block, DatabaseRequestError> {
        let mut rng = TEST_RNG.lock().unwrap();
        respond_with_ok_or_not_found(Block::random(&mut rng), &mut rng)
    }

    #[allow(unused)]
    async fn get_block_by_height(&self, height: u64) -> Result<Block, DatabaseRequestError> {
        let mut rng = TEST_RNG.lock().unwrap();
        respond_with_ok_or_not_found(Block::random(&mut rng), &mut rng)
    }

    #[allow(unused)]
    async fn get_block_by_hash(&self, hash: &str) -> Result<Block, DatabaseRequestError> {
        let hash_regex = regex::Regex::new("^([0-9A-Fa-f]){64}$")
            .map_err(|err| DatabaseRequestError::Unhandled(Error::from(err)))?;
        if !hash_regex.is_match(hash) {
            return Err(DatabaseRequestError::InvalidParam(Error::msg(format!(
                "Expected hex-encoded block hash (64 chars), received: {} (length: {})",
                hash,
                hash.len()
            ))));
        }

        let mut rng = TEST_RNG.lock().unwrap();
        respond_with_ok_or_not_found(Block::random(&mut rng), &mut rng)
    }

    async fn get_latest_deploy(&self) -> Result<AggregateDeployInfo, DatabaseRequestError> {
        let mut rng = TEST_RNG.lock().unwrap();
        respond_with_ok_or_not_found(
            AggregateDeployInfo {
                deploy_hash: "deploy hash".to_string(),
                accepted: Some("accepted deploy json".to_string()),
                processed: None,
                expired: false,
            },
            &mut rng,
        )
    }

    async fn get_deploy_by_hash(
        &self,
        hash: &str,
    ) -> Result<AggregateDeployInfo, DatabaseRequestError> {
        let mut rng = TEST_RNG.lock().unwrap();
        respond_with_ok_or_not_found(
            AggregateDeployInfo {
                deploy_hash: hash.to_string(),
                accepted: Some("accepted deploy json".to_string()),
                processed: Some("processed deploy json".to_string()),
                expired: false,
            },
            &mut rng,
        )
    }

    #[allow(unused)]
    async fn get_deploy_accepted_by_hash(
        &self,
        hash: &str,
    ) -> Result<Deploy, DatabaseRequestError> {
        let mut rng = TEST_RNG.lock().unwrap();
        respond_with_ok_or_not_found(Deploy::random(&mut rng), &mut rng)
    }

    #[allow(unused)]
    async fn get_deploy_processed_by_hash(
        &self,
        hash: &str,
    ) -> Result<DeployProcessed, DatabaseRequestError> {
        let mut rng = TEST_RNG.lock().unwrap();
        respond_with_ok_or_not_found(
            DeployProcessed {
                deploy_hash: Box::new(Default::default()),
                account: Box::new(PublicKey::System),
                timestamp: Timestamp::random(TEST_RNG.lock().unwrap().borrow_mut()),
                ttl: Default::default(),
                dependencies: vec![],
                block_hash: Box::new(Default::default()),
                execution_result: Box::new(ExecutionResult::Success {
                    effect: ExecutionEffect::default(),
                    transfers: vec![],
                    cost: Default::default(),
                }),
            },
            &mut rng,
        )
    }

    #[allow(unused)]
    async fn get_deploy_expired_by_hash(&self, hash: &str) -> Result<bool, DatabaseRequestError> {
        let mut rng = TEST_RNG.lock().unwrap();
        respond_with_ok_or_not_found(rng.gen_bool(1.0 / 3.0), &mut rng)
    }

    #[allow(unused)]
    async fn get_step_by_era(&self, era_id: u64) -> Result<Step, DatabaseRequestError> {
        let mut rng = TEST_RNG.lock().unwrap();
        respond_with_ok_or_not_found(
            Step {
                era_id: Default::default(),
                execution_effect: Default::default(),
            },
            &mut rng,
        )
    }

    #[allow(unused)]
    async fn get_fault_by_public_key(
        &self,
        public_key: &str,
    ) -> Result<Fault, DatabaseRequestError> {
        let mut rng = TEST_RNG.lock().unwrap();
        respond_with_ok_or_not_found(
            Fault {
                era_id: Default::default(),
                public_key: PublicKey::System,
                timestamp: Timestamp::random(TEST_RNG.lock().unwrap().borrow_mut()),
            },
            &mut rng,
        )
    }
}

#[cfg(test)]
fn respond_with_ok_or_not_found<T>(
    ok_value: T,
    rng: &mut TestRng,
) -> Result<T, DatabaseRequestError> {
    match rng.gen_bool(1.0 / 1.0) {
        true => Ok(ok_value),
        false => Err(DatabaseRequestError::NotFound),
    }
}
