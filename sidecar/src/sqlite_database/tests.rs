use std::collections::HashMap;

use casper_types::Timestamp;
use rand::Rng;
use sea_query::{Expr, Query, SqliteQueryBuilder};
use serde::Deserialize;
use sqlx::Row;

use casper_types::{testing::TestRng, AsymmetricType, EraId};

use super::SqliteDatabase;
use crate::types::database::{
    DeployAggregate, DeployAggregateFilter, DeployAggregateSortColumn, SortOrder,
};
use crate::{
    sql::tables::{self, event_type::EventTypeId},
    types::{
        database::{DatabaseReader, DatabaseWriteError, DatabaseWriter},
        sse_events::*,
    },
};

const MAX_CONNECTIONS: u32 = 100;

#[tokio::test]
async fn should_save_and_retrieve_a_u32max_id() {
    let sqlite_db = SqliteDatabase::new_in_memory(MAX_CONNECTIONS)
        .await
        .expect("Error opening database in memory");

    let sql = tables::event_log::create_insert_stmt(1, "source", u32::MAX, "event key")
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

    let aggregate = sqlite_db
        .get_deploy_aggregate_by_hash(&deploy_accepted.hex_encoded_hash())
        .await
        .expect("Error getting deploy aggregate by hash");
    assert_eq!(aggregate.deploy_hash, deploy_accepted.hex_encoded_hash());
    assert!(aggregate.block_timestamp.is_none());
}

#[tokio::test]
async fn should_retrieve_deploy_aggregate_with_block_data() {
    let mut test_rng = TestRng::new();

    let sqlite_db = SqliteDatabase::new_in_memory(MAX_CONNECTIONS)
        .await
        .expect("Error opening database in memory");
    let deploy_accepted = DeployAccepted::random(&mut test_rng);
    sqlite_db
        .save_deploy_accepted(deploy_accepted.clone(), 1, "127.0.0.1".to_string())
        .await
        .expect("Error saving deploy_accepted");
    let block_added = get_block_with_deploy_hash(
        String::from("0fc35e50bc21f88761ac0c8468db06040b3d0d1ea82c5e545370908cdc75b2a4"),
        deploy_accepted.hex_encoded_hash(),
        None,
        None,
    );

    sqlite_db
        .save_block_added(block_added, 2, "127.0.0.1".to_string())
        .await
        .expect("Error saving block_added");

    let deploy_processed =
        DeployProcessed::random(&mut test_rng, Some(deploy_accepted.deploy_hash()));

    sqlite_db
        .save_deploy_processed(deploy_processed, 3, "127.0.0.1".to_string())
        .await
        .expect("Error saving deploy_processed");

    let aggregate = sqlite_db
        .get_deploy_aggregate_by_hash(&deploy_accepted.hex_encoded_hash())
        .await
        .expect("Error getting deploy aggregate by hash");
    assert_eq!(aggregate.deploy_hash, deploy_accepted.hex_encoded_hash());
    assert_eq!(
        aggregate.block_timestamp.unwrap(),
        Timestamp::from(1673864937472)
    );
}

#[tokio::test]
/// Scenario:
/// * 4 DeployAccepted
/// ** first is processed and in a block
/// ** second is processed ant not in block
/// ** third is expired,
/// ** fourth is not expired and not processed
/// Expected: listing them should show data for all of them, only first one should have block_timestamp
async fn should_list_aggregates() {
    let sqlite_db = SqliteDatabase::new_in_memory(MAX_CONNECTIONS)
        .await
        .expect("Error opening database in memory");
    let mut test_rng = TestRng::new();
    let (deploy_hash_1, deploy_hash_2, deploy_hash_3, deploy_hash_4, _) =
        setup_four_deploy_accepted_scenario(&sqlite_db, &mut test_rng).await;

    let (data, count) = sqlite_db
        .list_deploy_aggregate(DeployAggregateFilter::paginate(0, 10))
        .await
        .expect("Error doing list_deploy_aggregate");

    assert_eq!(count, 4);
    assert_eq!(data.len(), 4);
    let deploys_map: HashMap<String, DeployAggregate> = data
        .into_iter()
        .map(|data| (data.deploy_hash.clone(), data))
        .collect();
    let retrieved_1 = deploys_map.get(&deploy_hash_1).unwrap();
    assert_eq!(
        retrieved_1.block_timestamp.unwrap(),
        Timestamp::from(1673864937472)
    );
    let retrieved_2 = deploys_map.get(&deploy_hash_2).unwrap();
    assert!(retrieved_2.block_timestamp.is_none());
    let retrieved_3 = deploys_map.get(&deploy_hash_3).unwrap();
    assert!(retrieved_3.block_timestamp.is_none());
    let retrieved_4 = deploys_map.get(&deploy_hash_4).unwrap();
    assert!(retrieved_4.block_timestamp.is_none());
}

#[tokio::test]
/// Scenario:
/// * 4 DeployAccepted
/// ** first is processed and in block_1
/// ** second is processed and not in blocd
/// ** third is expired,
/// ** fourth is not expired and not processed
/// Expected: listing them with exclude_expired and exclude_not_processed should return only first two
async fn should_list_aggregates_while_excluding() {
    let sqlite_db = SqliteDatabase::new_in_memory(MAX_CONNECTIONS)
        .await
        .expect("Error opening database in memory");
    let mut test_rng = TestRng::new();
    let (deploy_hash_1, deploy_hash_2, _, _, _) =
        setup_four_deploy_accepted_scenario(&sqlite_db, &mut test_rng).await;
    let mut filter = DeployAggregateFilter::paginate(0, 10);
    filter.exclude_expired = true;
    filter.exclude_not_processed = true;
    let (data, count) = sqlite_db
        .list_deploy_aggregate(filter)
        .await
        .expect("Error doing list_deploy_aggregate");

    assert_eq!(count, 2);
    assert_eq!(data.len(), 2);
    let deploys_map: HashMap<String, DeployAggregate> = data
        .into_iter()
        .map(|data| (data.deploy_hash.clone(), data))
        .collect();
    let retrieved_1 = deploys_map.get(&deploy_hash_1).unwrap();
    assert_eq!(
        retrieved_1.block_timestamp.unwrap(),
        Timestamp::from(1673864937472)
    );
    let retrieved_2 = deploys_map.get(&deploy_hash_2).unwrap();
    assert!(retrieved_2.block_timestamp.is_none());
}

#[tokio::test]
/// Scenario:
/// * 5 DeployAccepted
/// ** first is processed and in block_1 (block from 2023)
/// ** second is processed and in block_2 (block from 2022)
/// ** third is expired,
/// ** fourth is not expired and not processed
/// ** fifth is processed and not in block_3 (block from 2021)
/// Expected: listing them with sorting ASC and paging should return deploy accepted in order fifth, second, first
async fn should_list_aggregates_should_sort_and_paginate() {
    let mut test_rng = TestRng::new();
    let sqlite_db = SqliteDatabase::new_in_memory(MAX_CONNECTIONS)
        .await
        .expect("Error opening database in memory");
    let (deploy_hash_1, deploy_hash_2, _, _, _) =
        setup_four_deploy_accepted_scenario(&sqlite_db, &mut test_rng).await;
    let deploy_accepted_5 = DeployAccepted::random(&mut test_rng);
    sqlite_db
        .save_deploy_accepted(deploy_accepted_5.clone(), 9, "127.0.0.1".to_string())
        .await
        .expect("Error saving deploy_accepted");
    let deploy_processed_5 =
        DeployProcessed::random(&mut test_rng, Some(deploy_accepted_5.deploy_hash()));
    sqlite_db
        .save_deploy_processed(deploy_processed_5, 10, "127.0.0.1".to_string())
        .await
        .expect("Error saving deploy_processed");
    let block_added_2 = get_block_with_deploy_hash(
        String::from("5ffa56dbb87f983a0fa917d29f75254fe8f6eec3c2066756046d2c563b95a591"),
        deploy_hash_2.clone(),
        Some(String::from("2022-01-16T10:28:57.472Z")),
        Some(2),
    );
    sqlite_db
        .save_block_added(block_added_2.clone(), 11, "127.0.0.1".to_string())
        .await
        .expect("Error saving block_added");
    let block_added_3 = get_block_with_deploy_hash(
        String::from("ae8533b66af5b39b82a639996cd9346fa1f7dab4e976d5fde8755f661f13fc47"),
        deploy_accepted_5.hex_encoded_hash(),
        Some(String::from("2021-01-16T10:28:57.472Z")),
        Some(3),
    );
    sqlite_db
        .save_block_added(block_added_3.clone(), 12, "127.0.0.1".to_string())
        .await
        .expect("Error saving block_added");
    let mut filter = DeployAggregateFilter::paginate(0, 3);
    filter.sort_column = Some(DeployAggregateSortColumn::BlockTimestamp);
    filter.sort_order = Some(SortOrder::Asc);

    let (data, count) = sqlite_db
        .list_deploy_aggregate(filter)
        .await
        .expect("Error doing list_deploy_aggregate");

    assert_eq!(count, 5);
    assert_eq!(data.len(), 3);
    assert_eq!(
        data.get(0).unwrap().deploy_hash,
        deploy_accepted_5.hex_encoded_hash()
    );
    assert_eq!(data.get(1).unwrap().deploy_hash, deploy_hash_2);
    assert_eq!(data.get(2).unwrap().deploy_hash, deploy_hash_1);
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
        .expect("Error saving fault with a u64::MAX era id");

    let faults = sqlite_db
        .get_faults_by_era(u64::MAX)
        .await
        .expect("Error getting faults by era with u64::MAX era id");

    assert_eq!(faults[0].era_id.value(), u64::MAX);

    let faults = sqlite_db
        .get_faults_by_public_key(&fault.public_key.to_hex())
        .await
        .expect("Error getting faults by public key with u64::MAX era id");

    assert_eq!(faults[0].era_id.value(), u64::MAX);
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
async fn should_disallow_duplicate_event_id_from_source() {
    let mut test_rng = TestRng::new();

    let event_id = test_rng.gen::<u32>();

    let sqlite_db = SqliteDatabase::new_in_memory(MAX_CONNECTIONS)
        .await
        .expect("Error opening database in memory");

    let block_added = BlockAdded::random(&mut test_rng);

    assert!(sqlite_db
        .save_block_added(block_added.clone(), event_id, "127.0.0.1".to_string())
        .await
        .is_ok());
    let res = sqlite_db
        .save_block_added(block_added, event_id, "127.0.0.1".to_string())
        .await;
    assert!(matches!(res, Err(DatabaseWriteError::UniqueConstraint(_))));
    // This check is to ensure that the UNIQUE constraint being broken is from the event_log table rather than from the raw event table.
    if let Err(DatabaseWriteError::UniqueConstraint(uc_err)) = res {
        assert_eq!(uc_err.table, "event_log")
    }
}

#[tokio::test]
async fn should_disallow_insert_of_existing_block_added() {
    let mut test_rng = TestRng::new();

    let sqlite_db = SqliteDatabase::new_in_memory(MAX_CONNECTIONS)
        .await
        .expect("Error opening database in memory");

    let block_added = BlockAdded::random(&mut test_rng);

    assert!(sqlite_db
        .save_block_added(block_added.clone(), 1, "127.0.0.1".to_string())
        .await
        .is_ok());

    let db_err = sqlite_db
        .save_block_added(block_added, 2, "127.0.0.1".to_string())
        .await
        .unwrap_err();

    assert!(matches!(db_err, DatabaseWriteError::UniqueConstraint(_)));

    // This check is to ensure that the UNIQUE constraint error is originating from the raw event table rather than the event_log
    if let DatabaseWriteError::UniqueConstraint(uc_err) = db_err {
        assert_eq!(uc_err.table, "BlockAdded")
    }
}

#[tokio::test]
async fn should_disallow_insert_of_existing_deploy_accepted() {
    let mut test_rng = TestRng::new();

    let sqlite_db = SqliteDatabase::new_in_memory(MAX_CONNECTIONS)
        .await
        .expect("Error opening database in memory");

    let deploy_accepted = DeployAccepted::random(&mut test_rng);

    assert!(sqlite_db
        .save_deploy_accepted(deploy_accepted.clone(), 1, "127.0.0.1".to_string())
        .await
        .is_ok());

    let db_err = sqlite_db
        .save_deploy_accepted(deploy_accepted, 2, "127.0.0.1".to_string())
        .await
        .unwrap_err();

    assert!(matches!(db_err, DatabaseWriteError::UniqueConstraint(_)));

    // This check is to ensure that the UNIQUE constraint error is originating from the raw event table rather than the event_log
    if let DatabaseWriteError::UniqueConstraint(uc_err) = db_err {
        assert_eq!(uc_err.table, "DeployAccepted")
    }
}

#[tokio::test]
async fn should_disallow_insert_of_existing_deploy_expired() {
    let mut test_rng = TestRng::new();

    let sqlite_db = SqliteDatabase::new_in_memory(MAX_CONNECTIONS)
        .await
        .expect("Error opening database in memory");

    let deploy_expired = DeployExpired::random(&mut test_rng, None);

    assert!(sqlite_db
        .save_deploy_expired(deploy_expired.clone(), 1, "127.0.0.1".to_string())
        .await
        .is_ok());

    let db_err = sqlite_db
        .save_deploy_expired(deploy_expired, 2, "127.0.0.1".to_string())
        .await
        .unwrap_err();

    assert!(matches!(db_err, DatabaseWriteError::UniqueConstraint(_)));

    // This check is to ensure that the UNIQUE constraint error is originating from the raw event table rather than the event_log
    if let DatabaseWriteError::UniqueConstraint(uc_err) = db_err {
        assert_eq!(uc_err.table, "DeployExpired")
    }
}

#[tokio::test]
async fn should_disallow_insert_of_existing_deploy_processed() {
    let mut test_rng = TestRng::new();

    let sqlite_db = SqliteDatabase::new_in_memory(MAX_CONNECTIONS)
        .await
        .expect("Error opening database in memory");

    let deploy_processed = DeployProcessed::random(&mut test_rng, None);

    assert!(sqlite_db
        .save_deploy_processed(deploy_processed.clone(), 1, "127.0.0.1".to_string())
        .await
        .is_ok());

    let db_err = sqlite_db
        .save_deploy_processed(deploy_processed, 2, "127.0.0.1".to_string())
        .await
        .unwrap_err();

    assert!(matches!(db_err, DatabaseWriteError::UniqueConstraint(_)));

    // This check is to ensure that the UNIQUE constraint error is originating from the raw event table rather than the event_log
    if let DatabaseWriteError::UniqueConstraint(uc_err) = db_err {
        assert_eq!(uc_err.table, "DeployProcessed")
    }
}

#[tokio::test]
async fn should_disallow_insert_of_existing_fault() {
    let mut test_rng = TestRng::new();

    let sqlite_db = SqliteDatabase::new_in_memory(MAX_CONNECTIONS)
        .await
        .expect("Error opening database in memory");

    let fault = Fault::random(&mut test_rng);

    assert!(sqlite_db
        .save_fault(fault.clone(), 1, "127.0.0.1".to_string())
        .await
        .is_ok());

    let db_err = sqlite_db
        .save_fault(fault, 2, "127.0.0.1".to_string())
        .await
        .unwrap_err();

    assert!(matches!(db_err, DatabaseWriteError::UniqueConstraint(_)));

    // This check is to ensure that the UNIQUE constraint error is originating from the raw event table rather than the event_log
    if let DatabaseWriteError::UniqueConstraint(uc_err) = db_err {
        assert_eq!(uc_err.table, "Fault")
    }
}

#[tokio::test]
async fn should_disallow_insert_of_existing_finality_signature() {
    let mut test_rng = TestRng::new();

    let sqlite_db = SqliteDatabase::new_in_memory(MAX_CONNECTIONS)
        .await
        .expect("Error opening database in memory");

    let finality_signature = FinalitySignature::random(&mut test_rng);

    assert!(sqlite_db
        .save_finality_signature(finality_signature.clone(), 1, "127.0.0.1".to_string())
        .await
        .is_ok());

    let db_err = sqlite_db
        .save_finality_signature(finality_signature, 2, "127.0.0.1".to_string())
        .await
        .unwrap_err();

    assert!(matches!(db_err, DatabaseWriteError::UniqueConstraint(_)));

    // This check is to ensure that the UNIQUE constraint error is originating from the raw event table rather than the event_log
    if let DatabaseWriteError::UniqueConstraint(uc_err) = db_err {
        assert_eq!(uc_err.table, "FinalitySignature")
    }
}

#[tokio::test]
async fn should_disallow_insert_of_existing_step() {
    let mut test_rng = TestRng::new();

    let sqlite_db = SqliteDatabase::new_in_memory(MAX_CONNECTIONS)
        .await
        .expect("Error opening database in memory");

    let step = Step::random(&mut test_rng);

    assert!(sqlite_db
        .save_step(step.clone(), 1, "127.0.0.1".to_string())
        .await
        .is_ok());

    let db_err = sqlite_db
        .save_step(step, 2, "127.0.0.1".to_string())
        .await
        .unwrap_err();

    assert!(matches!(db_err, DatabaseWriteError::UniqueConstraint(_)));

    // This check is to ensure that the UNIQUE constraint error is originating from the raw event table rather than the event_log
    if let DatabaseWriteError::UniqueConstraint(uc_err) = db_err {
        assert_eq!(uc_err.table, "Step")
    }
}

#[tokio::test]
async fn should_save_block_added_with_correct_event_type_id() {
    let mut test_rng = TestRng::new();

    let sqlite_db = SqliteDatabase::new_in_memory(MAX_CONNECTIONS)
        .await
        .expect("Error opening database in memory");

    let block_added = BlockAdded::random(&mut test_rng);

    assert!(sqlite_db
        .save_block_added(block_added, 1, "127.0.0.1".to_string())
        .await
        .is_ok());

    let sql = Query::select()
        .column(tables::event_log::EventLog::EventTypeId)
        .from(tables::event_log::EventLog::Table)
        .limit(1)
        .to_string(SqliteQueryBuilder);

    let event_type_id = sqlite_db
        .fetch_one(&sql)
        .await
        .try_get::<u8, usize>(0)
        .expect("Error getting event_type_id from row");

    assert_eq!(event_type_id, EventTypeId::BlockAdded as u8)
}

#[tokio::test]
async fn should_save_deploy_accepted_with_correct_event_type_id() {
    let mut test_rng = TestRng::new();

    let sqlite_db = SqliteDatabase::new_in_memory(MAX_CONNECTIONS)
        .await
        .expect("Error opening database in memory");

    let deploy_accepted = DeployAccepted::random(&mut test_rng);

    assert!(sqlite_db
        .save_deploy_accepted(deploy_accepted, 1, "127.0.0.1".to_string())
        .await
        .is_ok());

    let sql = Query::select()
        .column(tables::event_log::EventLog::EventTypeId)
        .from(tables::event_log::EventLog::Table)
        .limit(1)
        .to_string(SqliteQueryBuilder);

    let event_type_id = sqlite_db
        .fetch_one(&sql)
        .await
        .try_get::<u8, usize>(0)
        .expect("Error getting event_type_id from row");

    assert_eq!(event_type_id, EventTypeId::DeployAccepted as u8)
}

#[tokio::test]
async fn should_save_deploy_processed_with_correct_event_type_id() {
    let mut test_rng = TestRng::new();

    let sqlite_db = SqliteDatabase::new_in_memory(MAX_CONNECTIONS)
        .await
        .expect("Error opening database in memory");

    let deploy_processed = DeployProcessed::random(&mut test_rng, None);

    assert!(sqlite_db
        .save_deploy_processed(deploy_processed, 1, "127.0.0.1".to_string())
        .await
        .is_ok());

    let sql = Query::select()
        .column(tables::event_log::EventLog::EventTypeId)
        .from(tables::event_log::EventLog::Table)
        .limit(1)
        .to_string(SqliteQueryBuilder);

    let event_type_id = sqlite_db
        .fetch_one(&sql)
        .await
        .try_get::<u8, usize>(0)
        .expect("Error getting event_type_id from row");

    assert_eq!(event_type_id, EventTypeId::DeployProcessed as u8)
}

#[tokio::test]
async fn should_save_deploy_expired_with_correct_event_type_id() {
    let mut test_rng = TestRng::new();

    let sqlite_db = SqliteDatabase::new_in_memory(MAX_CONNECTIONS)
        .await
        .expect("Error opening database in memory");

    let deploy_expired = DeployExpired::random(&mut test_rng, None);

    assert!(sqlite_db
        .save_deploy_expired(deploy_expired, 1, "127.0.0.1".to_string())
        .await
        .is_ok());

    let sql = Query::select()
        .column(tables::event_log::EventLog::EventTypeId)
        .from(tables::event_log::EventLog::Table)
        .limit(1)
        .to_string(SqliteQueryBuilder);

    let event_type_id = sqlite_db
        .fetch_one(&sql)
        .await
        .try_get::<u8, usize>(0)
        .expect("Error getting event_type_id from row");

    assert_eq!(event_type_id, EventTypeId::DeployExpired as u8)
}

#[tokio::test]
async fn should_save_fault_with_correct_event_type_id() {
    let mut test_rng = TestRng::new();

    let sqlite_db = SqliteDatabase::new_in_memory(MAX_CONNECTIONS)
        .await
        .expect("Error opening database in memory");

    let fault = Fault::random(&mut test_rng);

    assert!(sqlite_db
        .save_fault(fault, 1, "127.0.0.1".to_string())
        .await
        .is_ok());

    let sql = Query::select()
        .column(tables::event_log::EventLog::EventTypeId)
        .from(tables::event_log::EventLog::Table)
        .limit(1)
        .to_string(SqliteQueryBuilder);

    let event_type_id = sqlite_db
        .fetch_one(&sql)
        .await
        .try_get::<u8, usize>(0)
        .expect("Error getting event_type_id from row");

    assert_eq!(event_type_id, EventTypeId::Fault as u8)
}

#[tokio::test]
async fn should_save_finality_signature_with_correct_event_type_id() {
    let mut test_rng = TestRng::new();

    let sqlite_db = SqliteDatabase::new_in_memory(MAX_CONNECTIONS)
        .await
        .expect("Error opening database in memory");

    let finality_signature = FinalitySignature::random(&mut test_rng);

    assert!(sqlite_db
        .save_finality_signature(finality_signature, 1, "127.0.0.1".to_string())
        .await
        .is_ok());

    let sql = Query::select()
        .column(tables::event_log::EventLog::EventTypeId)
        .from(tables::event_log::EventLog::Table)
        .limit(1)
        .to_string(SqliteQueryBuilder);

    let event_type_id = sqlite_db
        .fetch_one(&sql)
        .await
        .try_get::<u8, usize>(0)
        .expect("Error getting event_type_id from row");

    assert_eq!(event_type_id, EventTypeId::FinalitySignature as u8)
}

#[tokio::test]
async fn should_save_step_with_correct_event_type_id() {
    let mut test_rng = TestRng::new();

    let sqlite_db = SqliteDatabase::new_in_memory(MAX_CONNECTIONS)
        .await
        .expect("Error opening database in memory");

    let step = Step::random(&mut test_rng);

    assert!(sqlite_db
        .save_step(step, 1, "127.0.0.1".to_string())
        .await
        .is_ok());

    let sql = Query::select()
        .column(tables::event_log::EventLog::EventTypeId)
        .from(tables::event_log::EventLog::Table)
        .limit(1)
        .to_string(SqliteQueryBuilder);

    let event_type_id = sqlite_db
        .fetch_one(&sql)
        .await
        .try_get::<u8, usize>(0)
        .expect("Error getting event_type_id from row");

    assert_eq!(event_type_id, EventTypeId::Step as u8)
}

#[tokio::test]
async fn should_save_and_retrieve_a_shutdown() {
    let sqlite_db = SqliteDatabase::new_in_memory(MAX_CONNECTIONS)
        .await
        .expect("Error opening database in memory");
    assert!(sqlite_db.save_shutdown(15, "xyz".to_string()).await.is_ok());

    let sql = Query::select()
        .expr(Expr::asterisk())
        .from(tables::shutdown::Shutdown::Table)
        .to_string(SqliteQueryBuilder);
    let row = sqlite_db.fetch_one(&sql).await;

    assert_eq!(
        row.get::<String, &str>("event_source_address"),
        "xyz".to_string()
    );
}

#[tokio::test]
async fn get_number_of_events_should_return_0() {
    let sqlite_db = SqliteDatabase::new_in_memory(MAX_CONNECTIONS)
        .await
        .expect("Error opening database in memory");

    assert_eq!(sqlite_db.get_number_of_events().await.unwrap(), 0);
}

#[tokio::test]
async fn get_number_of_events_should_return_1_when_event_stored() {
    let mut test_rng = TestRng::new();
    let sqlite_db = SqliteDatabase::new_in_memory(MAX_CONNECTIONS)
        .await
        .expect("Error opening database in memory");
    let fault = Fault::random(&mut test_rng);

    assert!(sqlite_db
        .save_fault(fault, 1, "127.0.0.1".to_string())
        .await
        .is_ok());
    assert_eq!(sqlite_db.get_number_of_events().await.unwrap(), 1);
}

fn get_block_with_deploy_hash(
    block_hash: String,
    deploy_hash: String,
    maybe_timestamp_string: Option<String>,
    maybe_height: Option<u32>,
) -> BlockAdded {
    let height = maybe_height.unwrap_or(1);
    let timestamp_string =
        maybe_timestamp_string.unwrap_or_else(|| String::from("2023-01-16T10:28:57.472Z"));
    let raw_json = format!("{{\"block_hash\":\"{}\",\"block\":{{\"hash\":\"{}\",\"header\":{{\"parent_hash\":\"7c00fc463c173b08e17abf1445914e9716d3a84cf5eaf7b50c011eb578f599cb\",\"state_root_hash\":\"57cc370c119d81631909ada9613976cbfa70bd8793799df765cfda0d8515c5e8\",\"body_hash\":\"3c138e9f9ceab35fb4152d468525e27c62f16d8e1901341567b478ef4cfd8b02\",\"random_bit\":false,\"accumulated_seed\":\"01f7166c6bfbacf428217e547dab9b4ee1a889ac0040e79a1c0005e94ff7094c\",\"era_end\":null,\"timestamp\":\"{}\",\"era_id\":7745,\"height\":{},\"protocol_version\":\"1.4.12\"}},\"body\":{{\"proposer\":\"011b19ef983c039a2a335f2f35199bf8cad5ba2c583bd709748feb76f24ffb1bab\",\"deploy_hashes\":[\"{}\"],\"transfer_hashes\":[]}},\"proofs\":[]}}}}", block_hash, block_hash, timestamp_string, height, deploy_hash);
    deserialize_data::<BlockAdded>(&raw_json)
}

fn deserialize_data<'de, T: Deserialize<'de>>(data: &'de str) -> T {
    serde_json::from_str::<T>(data).expect("Error while deserializing")
}

async fn setup_four_deploy_accepted_scenario(
    sqlite_db: &SqliteDatabase,
    test_rng: &mut TestRng,
) -> (String, String, String, String, String) {
    let deploy_accepted = DeployAccepted::random(test_rng);
    let deploy_accepted_2 = DeployAccepted::random(test_rng);
    let deploy_accepted_3 = DeployAccepted::random(test_rng);
    let deploy_accepted_4 = DeployAccepted::random(test_rng);
    sqlite_db
        .save_deploy_accepted(deploy_accepted.clone(), 1, "127.0.0.1".to_string())
        .await
        .expect("Error saving deploy_accepted");
    sqlite_db
        .save_deploy_accepted(deploy_accepted_2.clone(), 2, "127.0.0.1".to_string())
        .await
        .expect("Error saving deploy_accepted");
    sqlite_db
        .save_deploy_accepted(deploy_accepted_3.clone(), 3, "127.0.0.1".to_string())
        .await
        .expect("Error saving deploy_accepted");
    sqlite_db
        .save_deploy_accepted(deploy_accepted_4.clone(), 4, "127.0.0.1".to_string())
        .await
        .expect("Error saving deploy_accepted");
    let block_added = get_block_with_deploy_hash(
        String::from("0fc35e50bc21f88761ac0c8468db06040b3d0d1ea82c5e545370908cdc75b2a4"),
        deploy_accepted.hex_encoded_hash(),
        None,
        None,
    );
    sqlite_db
        .save_block_added(block_added.clone(), 5, "127.0.0.1".to_string())
        .await
        .expect("Error saving block_added");

    let deploy_processed = DeployProcessed::random(test_rng, Some(deploy_accepted.deploy_hash()));

    sqlite_db
        .save_deploy_processed(deploy_processed, 6, "127.0.0.1".to_string())
        .await
        .expect("Error saving deploy_processed");

    let deploy_processed_2 =
        DeployProcessed::random(test_rng, Some(deploy_accepted_2.deploy_hash()));

    sqlite_db
        .save_deploy_processed(deploy_processed_2, 7, "127.0.0.1".to_string())
        .await
        .expect("Error saving deploy_processed");

    let deploy_expired = DeployExpired::random(test_rng, None);

    sqlite_db
        .save_deploy_expired(deploy_expired.clone(), 8, "127.0.0.1".to_string())
        .await
        .expect("Error saving deploy_expired");

    (
        deploy_accepted.hex_encoded_hash(),
        deploy_accepted_2.hex_encoded_hash(),
        deploy_accepted_3.hex_encoded_hash(),
        deploy_accepted_4.hex_encoded_hash(),
        block_added.hex_encoded_hash(),
    )
}
