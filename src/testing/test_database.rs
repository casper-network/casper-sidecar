use std::borrow::BorrowMut;
use anyhow::Error;
use async_trait::async_trait;
use casper_node::types::{Block, Deploy};
use casper_types::{ExecutionEffect, ExecutionResult, PublicKey, Timestamp};
use casper_types::testing::TestRng;
use std::sync::Mutex;
use lazy_static::lazy_static;
use rand::Rng;

use crate::types::structs::{DeployProcessed, Fault, Step};
use crate::sqlite_db::{AggregateDeployInfo, DatabaseReader};

lazy_static! {
    static ref TEST_RNG: Mutex<TestRng> = Mutex::new(TestRng::new());
}

#[derive(Clone)]
pub struct MockDatabase;

#[async_trait]
impl DatabaseReader for MockDatabase {
    async fn get_latest_block(&self) -> Result<Block, Error> {
        let mut rng = TEST_RNG.lock().unwrap();
        respond_with_ok_or_err(Block::random(&mut rng), &mut rng)
    }

    #[allow(unused)]
    async fn get_block_by_height(&self, height: u64) -> Result<Block, Error> {
        let mut rng = TEST_RNG.lock().unwrap();
        respond_with_ok_or_err(Block::random(&mut rng), &mut rng)
    }

    #[allow(unused)]
    async fn get_block_by_hash(&self, hash: &str) -> Result<Block, Error> {
        let mut rng = TEST_RNG.lock().unwrap();
        respond_with_ok_or_err(Block::random(&mut rng), &mut rng)
    }

    async fn get_latest_deploy(&self) -> Result<AggregateDeployInfo, Error> {
        let mut rng = TEST_RNG.lock().unwrap();
        respond_with_ok_or_err(AggregateDeployInfo {
            deploy_hash: "deploy hash".to_string(),
            accepted: Some("accepted deploy json".to_string()),
            processed: None,
            expired: false
        }, &mut rng)
    }

    async fn get_deploy_by_hash(&self, hash: &str) -> Result<AggregateDeployInfo, Error> {
        let mut rng = TEST_RNG.lock().unwrap();
        respond_with_ok_or_err(AggregateDeployInfo {
            deploy_hash: hash.to_string(),
            accepted: Some("accepted deploy json".to_string()),
            processed: Some("processed deploy json".to_string()),
            expired: false
        }, &mut rng)
    }

    #[allow(unused)]
    async fn get_deploy_accepted_by_hash(&self, hash: &str) -> Result<Deploy, Error> {
        let mut rng = TEST_RNG.lock().unwrap();
        respond_with_ok_or_err(Deploy::random(&mut rng), &mut rng)
    }

    #[allow(unused)]
    async fn get_deploy_processed_by_hash(&self, hash: &str) -> Result<DeployProcessed, Error> {
        let mut rng = TEST_RNG.lock().unwrap();
        respond_with_ok_or_err(DeployProcessed {
            deploy_hash: Box::new(Default::default()),
            account: Box::new(PublicKey::System),
            timestamp: Timestamp::random(TEST_RNG.lock().unwrap().borrow_mut()),
            ttl: Default::default(),
            dependencies: vec![],
            block_hash: Box::new(Default::default()),
            execution_result: Box::new(ExecutionResult::Success {
                effect: ExecutionEffect::default(),
                transfers: vec![],
                cost: Default::default()
            })
        }, &mut rng)
    }

    #[allow(unused)]
    async fn get_deploy_expired_by_hash(&self, hash: &str) -> Result<bool, Error> {
        let mut rng = TEST_RNG.lock().unwrap();
        respond_with_ok_or_err(rng.gen_bool(1.0 / 3.0), &mut rng)
    }

    #[allow(unused)]
    async fn get_step_by_era(&self, era_id: u64) -> Result<Step, Error> {
        let mut rng = TEST_RNG.lock().unwrap();
        respond_with_ok_or_err(Step {
            era_id: Default::default(),
            execution_effect: Default::default()
        }, &mut rng)
    }

    #[allow(unused)]
    async fn get_fault_by_public_key(&self, public_key: &str) -> Result<Fault, Error> {
        let mut rng = TEST_RNG.lock().unwrap();
        respond_with_ok_or_err(Fault {
            era_id: Default::default(),
            public_key: PublicKey::System,
            timestamp: Timestamp::random(TEST_RNG.lock().unwrap().borrow_mut())
        }, &mut rng)
    }
}

#[cfg(test)]
fn respond_with_ok_or_err<T>(ok_value: T, rng: &mut TestRng) -> Result<T, Error> {
    match rng.gen_range(0..=2) {
        0 => Ok(ok_value),
        1 => Err(Error::msg("Query returned no rows")),
        _ => Err(Error::msg("Invalid request parameter"))
    }
}

// todo create a StorageError enum to share between prod and test code