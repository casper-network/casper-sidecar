use sea_query::{Query, SqliteQueryBuilder};
use sqlx::Row;

use casper_types::testing::TestRng;
use casper_types::{AsymmetricType, EraId};

use super::SqliteDatabase;
use crate::{
    sql::tables,
    types::{
        database::{DatabaseReader, DatabaseWriter},
        sse_events::*,
    },
};

const MAX_CONNECTIONS: u32 = 100;

#[tokio::test]
async fn should_save_and_retrieve_a_u32max_id() {
    let sqlite_db = SqliteDatabase::new_in_memory(MAX_CONNECTIONS)
        .await
        .expect("Error opening database in memory");

    let sql = tables::event_log::create_insert_stmt(1, "source", u32::MAX)
        .expect("Error creating event_log insert SQL")
        .to_string(SqliteQueryBuilder);

    let _ = sqlite_db.fetch_one(&sql).await;

    let sql = Query::select()
        .column(tables::event_log::EventLog::EventId)
        .from(tables::event_log::EventLog::Table)
        .limit(1)
        .to_string(SqliteQueryBuilder);

    let event_id_u32max = sqlite_db
        .fetch_one(&sql)
        .await
        .try_get::<u32, usize>(0)
        .expect("Error getting event_id (=u32::MAX) from row");

    assert_eq!(event_id_u32max, u32::MAX);
}

#[tokio::test]
async fn should_save_and_retrieve_a_step_with_u64_max_era() {
    let mut test_rng = TestRng::new();

    let sqlite_db = SqliteDatabase::new_in_memory(MAX_CONNECTIONS)
        .await
        .expect("Error opening database in memory");
    let mut step = Step::random(&mut test_rng);
    step.era_id = EraId::new(u64::MAX);

    sqlite_db
        .save_step(step.clone(), 1, "127.0.0.1".to_string())
        .await
        .expect("Error saving Step with u64::MAX era id");

    let retrieved_step = sqlite_db
        .get_step_by_era(u64::MAX)
        .await
        .expect("Error retrieving Step with u64::MAX era id");

    assert_eq!(retrieved_step.era_id.value(), u64::MAX)
}

#[tokio::test]
async fn should_save_and_retrieve_block_added() {
    let mut test_rng = TestRng::new();

    let sqlite_db = SqliteDatabase::new_in_memory(MAX_CONNECTIONS)
        .await
        .expect("Error opening database in memory");
    let block_added = BlockAdded::random(&mut test_rng);

    sqlite_db
        .save_block_added(block_added.clone(), 1, "127.0.0.1".to_string())
        .await
        .expect("Error saving block_added");

    sqlite_db
        .get_latest_block()
        .await
        .expect("Error getting latest block_added");

    sqlite_db
        .get_block_by_hash(&block_added.hex_encoded_hash())
        .await
        .expect("Error getting block_added by hash");

    sqlite_db
        .get_block_by_height(block_added.get_height())
        .await
        .expect("Error getting block_added by height");
}

#[tokio::test]
async fn should_save_and_retrieve_deploy_accepted() {
    let mut test_rng = TestRng::new();

    let sqlite_db = SqliteDatabase::new_in_memory(MAX_CONNECTIONS)
        .await
        .expect("Error opening database in memory");
    let deploy_accepted = DeployAccepted::random(&mut test_rng);

    sqlite_db
        .save_deploy_accepted(deploy_accepted.clone(), 1, "127.0.0.1".to_string())
        .await
        .expect("Error saving deploy_accepted");

    sqlite_db
        .get_deploy_accepted_by_hash(&deploy_accepted.hex_encoded_hash())
        .await
        .expect("Error getting deploy_accepted by hash");
}

#[tokio::test]
async fn should_save_and_retrieve_deploy_processed() {
    let mut test_rng = TestRng::new();

    let sqlite_db = SqliteDatabase::new_in_memory(MAX_CONNECTIONS)
        .await
        .expect("Error opening database in memory");
    let deploy_processed = DeployProcessed::random(&mut test_rng, None);

    sqlite_db
        .save_deploy_processed(deploy_processed.clone(), 1, "127.0.0.1".to_string())
        .await
        .expect("Error saving deploy_processed");

    sqlite_db
        .get_deploy_processed_by_hash(&deploy_processed.hex_encoded_hash())
        .await
        .expect("Error getting deploy_processed by hash");
}

#[tokio::test]
async fn should_save_and_retrieve_deploy_expired() {
    let mut test_rng = TestRng::new();

    let sqlite_db = SqliteDatabase::new_in_memory(MAX_CONNECTIONS)
        .await
        .expect("Error opening database in memory");
    let deploy_expired = DeployExpired::random(&mut test_rng, None);

    sqlite_db
        .save_deploy_expired(deploy_expired.clone(), 1, "127.0.0.1".to_string())
        .await
        .expect("Error saving deploy_expired");

    sqlite_db
        .get_deploy_expired_by_hash(&deploy_expired.hex_encoded_hash())
        .await
        .expect("Error getting deploy_expired by hash");
}

#[tokio::test]
async fn should_retrieve_deploy_aggregate_of_accepted() {
    let mut test_rng = TestRng::new();

    let sqlite_db = SqliteDatabase::new_in_memory(MAX_CONNECTIONS)
        .await
        .expect("Error opening database in memory");
    let deploy_accepted = DeployAccepted::random(&mut test_rng);

    sqlite_db
        .save_deploy_accepted(deploy_accepted.clone(), 1, "127.0.0.1".to_string())
        .await
        .expect("Error saving deploy_accepted");

    sqlite_db
        .get_latest_deploy_aggregate()
        .await
        .expect("Error getting latest deploy aggregate");

    sqlite_db
        .get_deploy_aggregate_by_hash(&deploy_accepted.hex_encoded_hash())
        .await
        .expect("Error getting deploy aggregate by hash");
}

#[tokio::test]
async fn should_retrieve_deploy_aggregate_of_processed() {
    let mut test_rng = TestRng::new();

    let sqlite_db = SqliteDatabase::new_in_memory(MAX_CONNECTIONS)
        .await
        .expect("Error opening database in memory");
    let deploy_accepted = DeployAccepted::random(&mut test_rng);
    let deploy_processed =
        DeployProcessed::random(&mut test_rng, Some(deploy_accepted.deploy_hash()));

    sqlite_db
        .save_deploy_accepted(deploy_accepted.clone(), 1, "127.0.0.1".to_string())
        .await
        .expect("Error saving deploy_accepted");

    sqlite_db
        .save_deploy_processed(deploy_processed, 2, "127.0.0.1".to_string())
        .await
        .expect("Error saving deploy_processed");

    sqlite_db
        .get_latest_deploy_aggregate()
        .await
        .expect("Error getting latest deploy aggregate");

    sqlite_db
        .get_deploy_aggregate_by_hash(&deploy_accepted.hex_encoded_hash())
        .await
        .expect("Error getting deploy aggregate by hash");
}

#[tokio::test]
async fn should_retrieve_deploy_aggregate_of_expired() {
    let mut test_rng = TestRng::new();

    let sqlite_db = SqliteDatabase::new_in_memory(MAX_CONNECTIONS)
        .await
        .expect("Error opening database in memory");
    let deploy_accepted = DeployAccepted::random(&mut test_rng);
    let deploy_expired = DeployExpired::random(&mut test_rng, Some(deploy_accepted.deploy_hash()));

    sqlite_db
        .save_deploy_accepted(deploy_accepted.clone(), 1, "127.0.0.1".to_string())
        .await
        .expect("Error saving deploy_accepted");

    sqlite_db
        .save_deploy_expired(deploy_expired, 2, "127.0.0.1".to_string())
        .await
        .expect("Error saving deploy_expired");

    sqlite_db
        .get_latest_deploy_aggregate()
        .await
        .expect("Error getting latest deploy aggregate");

    sqlite_db
        .get_deploy_aggregate_by_hash(&deploy_accepted.hex_encoded_hash())
        .await
        .expect("Error getting deploy aggregate by hash");
}

#[tokio::test]
async fn should_save_and_retrieve_fault() {
    let mut test_rng = TestRng::new();

    let sqlite_db = SqliteDatabase::new_in_memory(MAX_CONNECTIONS)
        .await
        .expect("Error opening database in memory");
    let fault = Fault::random(&mut test_rng);

    sqlite_db
        .save_fault(fault.clone(), 1, "127.0.0.1".to_string())
        .await
        .expect("Error saving fault");

    sqlite_db
        .get_faults_by_era(fault.era_id.value())
        .await
        .expect("Error getting faults by era");

    sqlite_db
        .get_faults_by_public_key(&fault.public_key.to_hex())
        .await
        .expect("Error getting faults by public key");
}

#[tokio::test]
async fn should_save_and_retrieve_fault_with_a_u64max() {
    let mut test_rng = TestRng::new();

    let sqlite_db = SqliteDatabase::new_in_memory(MAX_CONNECTIONS)
        .await
        .expect("Error opening database in memory");
    let mut fault = Fault::random(&mut test_rng);
    fault.era_id = EraId::new(u64::MAX);

    sqlite_db
        .save_fault(fault.clone(), 1, "127.0.0.1".to_string())
        .await
        .expect("Error saving fault");

    sqlite_db
        .get_faults_by_era(fault.era_id.value())
        .await
        .expect("Error getting faults by era");

    sqlite_db
        .get_faults_by_public_key(&fault.public_key.to_hex())
        .await
        .expect("Error getting faults by public key");
}

#[tokio::test]
async fn should_save_and_retrieve_finality_signature() {
    let mut test_rng = TestRng::new();

    let sqlite_db = SqliteDatabase::new_in_memory(MAX_CONNECTIONS)
        .await
        .expect("Error opening database in memory");
    let finality_signature = FinalitySignature::random(&mut test_rng);

    sqlite_db
        .save_finality_signature(finality_signature.clone(), 1, "127.0.0.1".to_string())
        .await
        .expect("Error saving finality_signature");

    sqlite_db
        .get_finality_signatures_by_block(&finality_signature.hex_encoded_block_hash())
        .await
        .expect("Error getting finality signatures by block_hash");
}

#[tokio::test]
async fn should_save_and_retrieve_step() {
    let mut test_rng = TestRng::new();

    let sqlite_db = SqliteDatabase::new_in_memory(MAX_CONNECTIONS)
        .await
        .expect("Error opening database in memory");
    let step = Step::random(&mut test_rng);

    sqlite_db
        .save_step(step.clone(), 1, "127.0.0.1".to_string())
        .await
        .expect("Error saving step");

    sqlite_db
        .get_step_by_era(step.era_id.value())
        .await
        .expect("Error getting step by era");
}
