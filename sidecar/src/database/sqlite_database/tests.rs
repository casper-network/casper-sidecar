use sea_query::{Asterisk, Expr, Query, SqliteQueryBuilder};
use sqlx::Row;

use casper_types::testing::TestRng;

use super::SqliteDatabase;
use crate::{
    sql::tables::{self, event_type::EventTypeId},
    types::{database::DatabaseWriter, sse_events::*},
};

const MAX_CONNECTIONS: u32 = 100;

#[cfg(test)]
async fn build_database() -> SqliteDatabase {
    SqliteDatabase::new_in_memory(MAX_CONNECTIONS)
        .await
        .expect("Error opening database in memory")
}

#[tokio::test]
async fn should_save_and_retrieve_a_u32max_id() {
    let sqlite_db = build_database().await;
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
    let sqlite_db = build_database().await;
    crate::database::tests::should_save_and_retrieve_block_added(sqlite_db).await;
}

#[tokio::test]
async fn should_save_and_retrieve_deploy_accepted() {
    let sqlite_db = build_database().await;
    crate::database::tests::should_save_and_retrieve_deploy_accepted(sqlite_db).await;
}

#[tokio::test]
async fn should_save_and_retrieve_deploy_processed() {
    let sqlite_db = build_database().await;
    crate::database::tests::should_save_and_retrieve_deploy_processed(sqlite_db).await;
}

#[tokio::test]
async fn should_save_and_retrieve_deploy_expired() {
    let sqlite_db = build_database().await;
    crate::database::tests::should_save_and_retrieve_deploy_expired(sqlite_db).await;
}

#[tokio::test]
async fn should_retrieve_deploy_aggregate_of_accepted() {
    let sqlite_db = build_database().await;
    crate::database::tests::should_retrieve_deploy_aggregate_of_accepted(sqlite_db).await;
}

#[tokio::test]
async fn should_retrieve_deploy_aggregate_of_processed() {
    let sqlite_db = build_database().await;
    crate::database::tests::should_retrieve_deploy_aggregate_of_processed(sqlite_db).await;
}

#[tokio::test]
async fn should_retrieve_deploy_aggregate_of_expired() {
    let sqlite_db = build_database().await;
    crate::database::tests::should_retrieve_deploy_aggregate_of_expired(sqlite_db).await;
}

#[tokio::test]
async fn should_save_and_retrieve_fault() {
    let sqlite_db = build_database().await;
    crate::database::tests::should_save_and_retrieve_fault(sqlite_db).await;
}

#[tokio::test]
async fn should_save_and_retrieve_fault_with_a_u64max() {
    let sqlite_db = build_database().await;
    crate::database::tests::should_save_and_retrieve_fault_with_a_u64max(sqlite_db).await;
}

#[tokio::test]
async fn should_save_and_retrieve_finality_signature() {
    let sqlite_db = build_database().await;
    crate::database::tests::should_save_and_retrieve_finality_signature(sqlite_db).await;
}

#[tokio::test]
async fn should_save_and_retrieve_step() {
    let sqlite_db = build_database().await;
    crate::database::tests::should_save_and_retrieve_step(sqlite_db).await;
}

#[tokio::test]
async fn should_save_and_retrieve_a_step_with_u64_max_era() {
    let sqlite_db = build_database().await;
    crate::database::tests::should_save_and_retrieve_a_step_with_u64_max_era(sqlite_db).await;
}

#[tokio::test]
async fn should_disallow_duplicate_event_id_from_source() {
    let sqlite_db = build_database().await;
    crate::database::tests::should_disallow_duplicate_event_id_from_source(sqlite_db).await;
}

#[tokio::test]
async fn should_disallow_insert_of_existing_block_added() {
    let sqlite_db = build_database().await;
    crate::database::tests::should_disallow_insert_of_existing_block_added(sqlite_db).await;
}

#[tokio::test]
async fn should_disallow_insert_of_existing_deploy_accepted() {
    let sqlite_db = build_database().await;
    crate::database::tests::should_disallow_insert_of_existing_deploy_accepted(sqlite_db).await;
}

#[tokio::test]
async fn should_disallow_insert_of_existing_deploy_expired() {
    let sqlite_db = build_database().await;
    crate::database::tests::should_disallow_insert_of_existing_deploy_expired(sqlite_db).await;
}

#[tokio::test]
async fn should_disallow_insert_of_existing_deploy_processed() {
    let sqlite_db = build_database().await;
    crate::database::tests::should_disallow_insert_of_existing_deploy_processed(sqlite_db).await;
}

#[tokio::test]
async fn should_disallow_insert_of_existing_fault() {
    let sqlite_db = build_database().await;
    crate::database::tests::should_disallow_insert_of_existing_fault(sqlite_db).await;
}

#[tokio::test]
async fn should_disallow_insert_of_existing_finality_signature() {
    let sqlite_db = build_database().await;
    crate::database::tests::should_disallow_insert_of_existing_finality_signature(sqlite_db).await;
}

#[tokio::test]
async fn should_disallow_insert_of_existing_step() {
    let sqlite_db = build_database().await;
    crate::database::tests::should_disallow_insert_of_existing_step(sqlite_db).await;
}

#[tokio::test]
async fn should_save_block_added_with_correct_event_type_id() {
    let mut test_rng = TestRng::new();

    let sqlite_db = build_database().await;

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
        .try_get::<i16, usize>(0)
        .expect("Error getting event_type_id from row");

    assert_eq!(event_type_id, EventTypeId::BlockAdded as i16)
}

#[tokio::test]
async fn should_save_deploy_accepted_with_correct_event_type_id() {
    let mut test_rng = TestRng::new();

    let sqlite_db = build_database().await;

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
        .try_get::<i16, usize>(0)
        .expect("Error getting event_type_id from row");

    assert_eq!(event_type_id, EventTypeId::DeployAccepted as i16)
}

#[tokio::test]
async fn should_save_deploy_processed_with_correct_event_type_id() {
    let mut test_rng = TestRng::new();

    let sqlite_db = build_database().await;

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
        .try_get::<i16, usize>(0)
        .expect("Error getting event_type_id from row");

    assert_eq!(event_type_id, EventTypeId::DeployProcessed as i16)
}

#[tokio::test]
async fn should_save_deploy_expired_with_correct_event_type_id() {
    let mut test_rng = TestRng::new();

    let sqlite_db = build_database().await;

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
        .try_get::<i16, usize>(0)
        .expect("Error getting event_type_id from row");

    assert_eq!(event_type_id, EventTypeId::DeployExpired as i16)
}

#[tokio::test]
async fn should_save_fault_with_correct_event_type_id() {
    let mut test_rng = TestRng::new();

    let sqlite_db = build_database().await;

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
        .try_get::<i16, usize>(0)
        .expect("Error getting event_type_id from row");

    assert_eq!(event_type_id, EventTypeId::Fault as i16)
}

#[tokio::test]
async fn should_save_finality_signature_with_correct_event_type_id() {
    let mut test_rng = TestRng::new();

    let sqlite_db = build_database().await;

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
        .try_get::<i16, usize>(0)
        .expect("Error getting event_type_id from row");

    assert_eq!(event_type_id, EventTypeId::FinalitySignature as i16)
}

#[tokio::test]
async fn should_save_step_with_correct_event_type_id() {
    let mut test_rng = TestRng::new();

    let sqlite_db = build_database().await;

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
        .try_get::<i16, usize>(0)
        .expect("Error getting event_type_id from row");

    assert_eq!(event_type_id, EventTypeId::Step as i16)
}

#[tokio::test]
async fn should_save_and_retrieve_a_shutdown() {
    let sqlite_db = build_database().await;
    assert!(sqlite_db.save_shutdown(15, "xyz".to_string()).await.is_ok());

    let sql = Query::select()
        .expr(Expr::col(Asterisk))
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
    let sqlite_db = build_database().await;
    crate::database::tests::get_number_of_events_should_return_0(sqlite_db).await;
}

#[tokio::test]
async fn get_number_of_events_should_return_1_when_event_stored() {
    let sqlite_db = build_database().await;
    crate::database::tests::get_number_of_events_should_return_1_when_event_stored(sqlite_db).await;
}
