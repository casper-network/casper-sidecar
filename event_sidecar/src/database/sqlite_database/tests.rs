use sea_query::{Query, SqliteQueryBuilder};
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
    let sql = tables::event_log::create_insert_stmt(
        1,
        "source",
        u32::MAX,
        "event key",
        "some_version",
        "network-1",
    )
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
async fn should_save_and_retrieve_transaction_accepted() {
    let sqlite_db = build_database().await;
    crate::database::tests::should_save_and_retrieve_transaction_accepted(sqlite_db).await;
}

#[tokio::test]
async fn should_save_and_retrieve_transaction_processed() {
    let sqlite_db = build_database().await;
    crate::database::tests::should_save_and_retrieve_transaction_processed(sqlite_db).await;
}

#[tokio::test]
async fn should_save_and_retrieve_transaction_expired() {
    let sqlite_db = build_database().await;
    crate::database::tests::should_save_and_retrieve_transaction_expired(sqlite_db).await;
}

#[tokio::test]
async fn should_retrieve_transaction_aggregate_of_accepted() {
    let sqlite_db = build_database().await;
    crate::database::tests::should_retrieve_transaction_aggregate_of_accepted(sqlite_db).await;
}

#[tokio::test]
async fn should_retrieve_transaction_aggregate_of_processed() {
    let sqlite_db = build_database().await;
    crate::database::tests::should_retrieve_transaction_aggregate_of_processed(sqlite_db).await;
}

#[tokio::test]
async fn should_retrieve_transaction_aggregate_of_expired() {
    let sqlite_db = build_database().await;
    crate::database::tests::should_retrieve_transaction_aggregate_of_expired(sqlite_db).await;
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
async fn should_disallow_insert_of_existing_transaction_accepted() {
    let sqlite_db = build_database().await;
    crate::database::tests::should_disallow_insert_of_existing_transaction_accepted(sqlite_db)
        .await;
}

#[tokio::test]
async fn should_disallow_insert_of_existing_transaction_expired() {
    let sqlite_db = build_database().await;
    crate::database::tests::should_disallow_insert_of_existing_transaction_expired(sqlite_db).await;
}

#[tokio::test]
async fn should_disallow_insert_of_existing_transaction_processed() {
    let sqlite_db = build_database().await;
    crate::database::tests::should_disallow_insert_of_existing_transaction_processed(sqlite_db)
        .await;
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

    let db = build_database().await;

    let block_added = BlockAdded::random(&mut test_rng);

    assert!(db
        .save_block_added(
            block_added,
            1,
            "127.0.0.1".to_string(),
            "1.1.1".to_string(),
            "network-1".to_string()
        )
        .await
        .is_ok());

    verify_event_log_entry(
        db,
        "127.0.0.1",
        EventTypeId::BlockAdded as i16,
        "1.1.1",
        "network-1",
    )
    .await;
}

#[tokio::test]
async fn should_save_transaction_accepted_with_correct_event_type_id() {
    let mut test_rng = TestRng::new();

    let db = build_database().await;

    let transaction_accepted = TransactionAccepted::random(&mut test_rng);

    assert!(db
        .save_transaction_accepted(
            transaction_accepted,
            1,
            "127.0.0.1".to_string(),
            "1.5.5".to_string(),
            "network-1".to_string(),
        )
        .await
        .is_ok());

    verify_event_log_entry(
        db,
        "127.0.0.1",
        EventTypeId::TransactionAccepted as i16,
        "1.5.5",
        "network-1",
    )
    .await;
}

#[tokio::test]
async fn should_save_transaction_processed_with_correct_event_type_id() {
    let mut test_rng = TestRng::new();

    let db = build_database().await;

    let transaction_processed = TransactionProcessed::random(&mut test_rng, None);

    assert!(db
        .save_transaction_processed(
            transaction_processed,
            1,
            "127.0.0.1".to_string(),
            "1.1.1".to_string(),
            "network-1".to_string(),
        )
        .await
        .is_ok());

    verify_event_log_entry(
        db,
        "127.0.0.1",
        EventTypeId::TransactionProcessed as i16,
        "1.1.1",
        "network-1",
    )
    .await;
}

#[tokio::test]
async fn should_save_transaction_expired_with_correct_event_type_id() {
    let mut test_rng = TestRng::new();

    let db = build_database().await;

    let transaction_expired = TransactionExpired::random(&mut test_rng, None);

    assert!(db
        .save_transaction_expired(
            transaction_expired,
            1,
            "127.0.0.1".to_string(),
            "1.1.1".to_string(),
            "network-1".to_string(),
        )
        .await
        .is_ok());

    verify_event_log_entry(
        db,
        "127.0.0.1",
        EventTypeId::TransactionExpired as i16,
        "1.1.1",
        "network-1",
    )
    .await;
}

#[tokio::test]
async fn should_save_fault_with_correct_event_type_id() {
    let mut test_rng = TestRng::new();

    let db = build_database().await;

    let fault = Fault::random(&mut test_rng);

    assert!(db
        .save_fault(
            fault,
            1,
            "127.0.0.1".to_string(),
            "1.1.1".to_string(),
            "network-1".to_string()
        )
        .await
        .is_ok());

    verify_event_log_entry(
        db,
        "127.0.0.1",
        EventTypeId::Fault as i16,
        "1.1.1",
        "network-1",
    )
    .await;
}

#[tokio::test]
async fn should_save_finality_signature_with_correct_event_type_id() {
    let mut test_rng = TestRng::new();

    let db = build_database().await;

    let finality_signature = FinalitySignature::random(&mut test_rng);

    assert!(db
        .save_finality_signature(
            finality_signature,
            1,
            "127.0.0.1".to_string(),
            "1.1.1".to_string(),
            "network-1".to_string(),
        )
        .await
        .is_ok());

    verify_event_log_entry(
        db,
        "127.0.0.1",
        EventTypeId::FinalitySignature as i16,
        "1.1.1",
        "network-1",
    )
    .await;
}

#[tokio::test]
async fn should_save_step_with_correct_event_type_id() {
    let mut test_rng = TestRng::new();

    let db = build_database().await;

    let step = Step::random(&mut test_rng);

    assert!(db
        .save_step(
            step,
            1,
            "127.0.0.1".to_string(),
            "1.1.1".to_string(),
            "network-1".to_string(),
        )
        .await
        .is_ok());

    verify_event_log_entry(
        db,
        "127.0.0.1",
        EventTypeId::Step as i16,
        "1.1.1",
        "network-1",
    )
    .await;
}

#[tokio::test]
async fn should_save_and_retrieve_a_shutdown() {
    let db = build_database().await;
    assert!(db
        .save_shutdown(
            15,
            "xyz".to_string(),
            "1.1.1".to_string(),
            "network-1".to_string(),
        )
        .await
        .is_ok());

    verify_event_log_entry(
        db,
        "xyz",
        EventTypeId::Shutdown as i16,
        "1.1.1",
        "network-1",
    )
    .await;
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

async fn verify_event_log_entry(
    db: SqliteDatabase,
    expected_event_source_address: &str,
    expected_event_type_id: i16,
    expected_api_version: &str,
    expected_network_name: &str,
) {
    let sql = crate::database::tests::fetch_event_log_data_query().to_string(SqliteQueryBuilder);
    let row = db.fetch_one(&sql).await;
    let event_type_id = row
        .try_get::<i16, usize>(0)
        .expect("Error getting event_type_id from row");
    let api_version = row
        .try_get::<String, usize>(1)
        .expect("Error getting api_version from row");
    let network_name = row
        .try_get::<String, usize>(2)
        .expect("Error getting network_name from row");
    let event_source_address = row.get::<String, usize>(3);
    assert_eq!(event_type_id, expected_event_type_id);
    assert_eq!(api_version, expected_api_version.to_string());
    assert_eq!(network_name, expected_network_name.to_string());
    assert_eq!(
        event_source_address,
        expected_event_source_address.to_string()
    );
}
