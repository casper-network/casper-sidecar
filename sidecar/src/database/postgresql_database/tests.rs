use crate::{
    sql::tables::{self, event_type::EventTypeId},
    types::{database::DatabaseWriter, sse_events::*},
    utils::build_postgres_database,
};
use casper_types::testing::TestRng;
use sea_query::{Asterisk, Expr, PostgresQueryBuilder, Query, SqliteQueryBuilder};
use sqlx::Row;

#[tokio::test]
async fn should_save_and_retrieve_a_u32max_id() {
    let context = build_postgres_database().await.unwrap();
    let sqlite_db = &context.db;
    let sql = tables::event_log::create_insert_stmt(1, "source", u32::MAX, "event key")
        .expect("Error creating event_log insert SQL")
        .to_string(PostgresQueryBuilder);
    let _ = sqlite_db.fetch_one(&sql).await;
    let sql = Query::select()
        .column(tables::event_log::EventLog::EventId)
        .from(tables::event_log::EventLog::Table)
        .limit(1)
        .to_string(PostgresQueryBuilder);

    let event_id_u32max = sqlite_db
        .fetch_one(&sql)
        .await
        .try_get::<i64, usize>(0)
        .expect("Error getting event_id (=u32::MAX) from row");

    assert_eq!(event_id_u32max, u32::MAX as i64);
}

#[tokio::test]
async fn should_save_and_retrieve_block_added() {
    let test_context = build_postgres_database().await.unwrap();
    crate::database::tests::should_save_and_retrieve_block_added(test_context.db.clone()).await;
}

#[tokio::test]
async fn should_save_and_retrieve_deploy_accepted() {
    let test_context = build_postgres_database().await.unwrap();
    crate::database::tests::should_save_and_retrieve_deploy_accepted(test_context.db.clone()).await;
}

#[tokio::test]
async fn should_save_and_retrieve_deploy_processed() {
    let test_context = build_postgres_database().await.unwrap();
    crate::database::tests::should_save_and_retrieve_deploy_processed(test_context.db.clone())
        .await;
}

#[tokio::test]
async fn should_save_and_retrieve_deploy_expired() {
    let test_context = build_postgres_database().await.unwrap();
    crate::database::tests::should_save_and_retrieve_deploy_expired(test_context.db.clone()).await;
}

#[tokio::test]
async fn should_retrieve_deploy_aggregate_of_accepted() {
    let test_context = build_postgres_database().await.unwrap();
    crate::database::tests::should_retrieve_deploy_aggregate_of_accepted(test_context.db.clone())
        .await;
}

#[tokio::test]
async fn should_retrieve_deploy_aggregate_of_processed() {
    let test_context = build_postgres_database().await.unwrap();
    crate::database::tests::should_retrieve_deploy_aggregate_of_processed(test_context.db.clone())
        .await;
}

#[tokio::test]
async fn should_retrieve_deploy_aggregate_of_expired() {
    let test_context = build_postgres_database().await.unwrap();
    crate::database::tests::should_retrieve_deploy_aggregate_of_expired(test_context.db.clone())
        .await;
}

#[tokio::test]
async fn should_save_and_retrieve_fault() {
    let test_context = build_postgres_database().await.unwrap();
    crate::database::tests::should_save_and_retrieve_fault(test_context.db.clone()).await;
}

#[tokio::test]
async fn should_save_and_retrieve_fault_with_a_u64max() {
    let test_context = build_postgres_database().await.unwrap();
    crate::database::tests::should_save_and_retrieve_fault_with_a_u64max(test_context.db.clone())
        .await;
}

#[tokio::test]
async fn should_save_and_retrieve_finality_signature() {
    let test_context = build_postgres_database().await.unwrap();
    crate::database::tests::should_save_and_retrieve_finality_signature(test_context.db.clone())
        .await;
}

#[tokio::test]
async fn should_save_and_retrieve_step() {
    let test_context = build_postgres_database().await.unwrap();
    crate::database::tests::should_save_and_retrieve_step(test_context.db.clone()).await;
}

#[tokio::test]
async fn should_save_and_retrieve_a_step_with_u64_max_era() {
    let test_context = build_postgres_database().await.unwrap();
    crate::database::tests::should_save_and_retrieve_a_step_with_u64_max_era(
        test_context.db.clone(),
    )
    .await;
}

#[tokio::test]
async fn should_disallow_duplicate_event_id_from_source() {
    let test_context = build_postgres_database().await.unwrap();
    crate::database::tests::should_disallow_duplicate_event_id_from_source(test_context.db.clone())
        .await;
}

#[tokio::test]
async fn should_disallow_insert_of_existing_block_added() {
    let test_context = build_postgres_database().await.unwrap();
    crate::database::tests::should_disallow_insert_of_existing_block_added(test_context.db.clone())
        .await;
}

#[tokio::test]
async fn should_disallow_insert_of_existing_deploy_accepted() {
    let test_context = build_postgres_database().await.unwrap();
    crate::database::tests::should_disallow_insert_of_existing_deploy_accepted(
        test_context.db.clone(),
    )
    .await;
}

#[tokio::test]
async fn should_disallow_insert_of_existing_deploy_expired() {
    let test_context = build_postgres_database().await.unwrap();
    crate::database::tests::should_disallow_insert_of_existing_deploy_expired(
        test_context.db.clone(),
    )
    .await;
}

#[tokio::test]
async fn should_disallow_insert_of_existing_deploy_processed() {
    let test_context = build_postgres_database().await.unwrap();
    crate::database::tests::should_disallow_insert_of_existing_deploy_processed(
        test_context.db.clone(),
    )
    .await;
}

#[tokio::test]
async fn should_disallow_insert_of_existing_fault() {
    let test_context = build_postgres_database().await.unwrap();
    crate::database::tests::should_disallow_insert_of_existing_fault(test_context.db.clone()).await;
}

#[tokio::test]
async fn should_disallow_insert_of_existing_finality_signature() {
    let test_context = build_postgres_database().await.unwrap();
    crate::database::tests::should_disallow_insert_of_existing_finality_signature(
        test_context.db.clone(),
    )
    .await;
}

#[tokio::test]
async fn should_disallow_insert_of_existing_step() {
    let test_context = build_postgres_database().await.unwrap();
    crate::database::tests::should_disallow_insert_of_existing_step(test_context.db.clone()).await;
}

#[tokio::test]
async fn should_save_block_added_with_correct_event_type_id() {
    let mut test_rng = TestRng::new();

    let test_context = build_postgres_database().await.unwrap();
    let sqlite_db = &test_context.db;

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

    let test_context = build_postgres_database().await.unwrap();
    let sqlite_db = &test_context.db;

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

    let test_context = build_postgres_database().await.unwrap();
    let sqlite_db = &test_context.db;

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

    let test_context = build_postgres_database().await.unwrap();
    let sqlite_db = &test_context.db;

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

    let test_context = build_postgres_database().await.unwrap();
    let sqlite_db = &test_context.db;

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

    let test_context = build_postgres_database().await.unwrap();
    let sqlite_db = &test_context.db;

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

    let test_context = build_postgres_database().await.unwrap();
    let sqlite_db = &test_context.db;

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
    let test_context = build_postgres_database().await.unwrap();
    let sqlite_db = &test_context.db;
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
    let test_context = build_postgres_database().await.unwrap();
    crate::database::tests::get_number_of_events_should_return_0(test_context.db.clone()).await;
}

#[tokio::test]
async fn get_number_of_events_should_return_1_when_event_stored() {
    let test_context = build_postgres_database().await.unwrap();
    crate::database::tests::get_number_of_events_should_return_1_when_event_stored(
        test_context.db.clone(),
    )
    .await;
}
