use super::PostgreSqlDatabase;
use crate::testing::testing_config::get_port;
use crate::{
    sql::tables::{self, event_type::EventTypeId},
    types::{database::DatabaseWriter, sse_events::*},
};
use anyhow::Error;
use casper_types::testing::TestRng;
use pg_embed::pg_enums::PgAuthMethod;
use pg_embed::postgres::PgSettings;
use pg_embed::{
    pg_fetch::{PgFetchSettings, PG_V13},
    postgres::PgEmbed,
};
use sea_query::{Asterisk, Expr, PostgresQueryBuilder, Query, SqliteQueryBuilder};
use sqlx::Row;
use std::path::PathBuf;
use std::time::Duration;
use tempfile::{tempdir, TempDir};

struct TestContext {
    pg: PgEmbed,
    _temp_dir: TempDir,
    db: PostgreSqlDatabase,
}

impl Drop for TestContext {
    fn drop(&mut self) {
        let _ = self.pg.stop_db_sync();
    }
}

async fn spin_up_postgres(
    pg_settings: PgSettings,
    temp_dir: TempDir,
) -> Result<(PgEmbed, TempDir), Error> {
    let fetch_settings = PgFetchSettings {
        version: PG_V13,
        ..Default::default()
    };
    let pg_res = PgEmbed::new(pg_settings, fetch_settings).await;
    if let Err(e) = pg_res {
        drop(temp_dir);
        return Err(anyhow::Error::from(e));
    }
    let mut pg = pg_res.unwrap();
    let res = pg.setup().await;
    if let Err(e) = res {
        let _ = pg.stop_db().await;
        drop(temp_dir);
        return Err(anyhow::Error::from(e));
    }
    let res = pg.start_db().await;
    if let Err(e) = res {
        let _ = pg.stop_db().await;
        drop(temp_dir);
        return Err(anyhow::Error::from(e));
    }
    Ok((pg, temp_dir))
}

async fn start_embedded_postgres() -> (PgEmbed, TempDir) {
    let port = get_port();
    let temp_storage_path = tempdir().expect("Should have created a temporary storage directory");
    let path = temp_storage_path.path().to_string_lossy().to_string();
    let pg_settings = PgSettings {
        database_dir: PathBuf::from(path),
        port,
        user: "postgres".to_string(),
        password: "docker".to_string(),
        auth_method: PgAuthMethod::Plain,
        persistent: false,
        timeout: Some(Duration::from_secs(120)),
        migration_dir: None,
    };
    spin_up_postgres(pg_settings, temp_storage_path)
        .await
        .unwrap()
}

async fn build_database(test_name: &str) -> Result<TestContext, Error> {
    println!("{} Running test", test_name);
    let (pg, temp_dir) = start_embedded_postgres().await;
    println!("{} Embedded postgres started: ", test_name);
    let database_name = "event_sidecar";
    pg.create_database(database_name).await?;
    println!("{} Database created: ", test_name);
    let pg_db_uri: String = pg.full_db_uri(database_name);
    let db = PostgreSqlDatabase::new_from_postgres_uri(pg_db_uri)
        .await
        .unwrap();
    println!("{} Database connection created ", test_name);
    let res = Ok(TestContext {
        pg,
        _temp_dir: temp_dir,
        db,
    });
    println!("{} Back to test", test_name);
    res
}

#[tokio::test]
async fn should_save_and_retrieve_a_u32max_id() {
    let context = build_database("should_save_and_retrieve_a_u32max_id")
        .await
        .unwrap();
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
    let test_context = build_database("should_save_and_retrieve_block_added")
        .await
        .unwrap();
    crate::database::tests::should_save_and_retrieve_block_added(test_context.db.clone()).await;
}

#[tokio::test]
async fn should_save_and_retrieve_deploy_accepted() {
    let test_context = build_database("should_save_and_retrieve_deploy_accepted")
        .await
        .unwrap();
    crate::database::tests::should_save_and_retrieve_deploy_accepted(test_context.db.clone()).await;
}

#[tokio::test]
async fn should_save_and_retrieve_deploy_processed() {
    let test_context = build_database("should_save_and_retrieve_deploy_processed")
        .await
        .unwrap();
    crate::database::tests::should_save_and_retrieve_deploy_processed(test_context.db.clone())
        .await;
}

#[tokio::test]
async fn should_save_and_retrieve_deploy_expired() {
    let test_context = build_database("should_save_and_retrieve_deploy_expired")
        .await
        .unwrap();
    crate::database::tests::should_save_and_retrieve_deploy_expired(test_context.db.clone()).await;
}

#[tokio::test]
async fn should_retrieve_deploy_aggregate_of_accepted() {
    let test_context = build_database("should_retrieve_deploy_aggregate_of_accepted")
        .await
        .unwrap();
    crate::database::tests::should_retrieve_deploy_aggregate_of_accepted(test_context.db.clone())
        .await;
}

#[tokio::test]
async fn should_retrieve_deploy_aggregate_of_processed() {
    let test_context = build_database("should_retrieve_deploy_aggregate_of_processed")
        .await
        .unwrap();
    crate::database::tests::should_retrieve_deploy_aggregate_of_processed(test_context.db.clone())
        .await;
}

#[tokio::test]
async fn should_retrieve_deploy_aggregate_of_expired() {
    let test_context = build_database("should_retrieve_deploy_aggregate_of_expired")
        .await
        .unwrap();
    crate::database::tests::should_retrieve_deploy_aggregate_of_expired(test_context.db.clone())
        .await;
}

#[tokio::test]
async fn should_save_and_retrieve_fault() {
    let test_context = build_database("should_save_and_retrieve_fault")
        .await
        .unwrap();
    crate::database::tests::should_save_and_retrieve_fault(test_context.db.clone()).await;
}

#[tokio::test]
async fn should_save_and_retrieve_fault_with_a_u64max() {
    let test_context = build_database("should_save_and_retrieve_fault_with_a_u64max")
        .await
        .unwrap();
    crate::database::tests::should_save_and_retrieve_fault_with_a_u64max(test_context.db.clone())
        .await;
}

#[tokio::test]
async fn should_save_and_retrieve_finality_signature() {
    let test_context = build_database("should_save_and_retrieve_finality_signature")
        .await
        .unwrap();
    crate::database::tests::should_save_and_retrieve_finality_signature(test_context.db.clone())
        .await;
}

#[tokio::test]
async fn should_save_and_retrieve_step() {
    let test_context = build_database("should_save_and_retrieve_step")
        .await
        .unwrap();
    crate::database::tests::should_save_and_retrieve_step(test_context.db.clone()).await;
}

#[tokio::test]
async fn should_save_and_retrieve_a_step_with_u64_max_era() {
    let test_context = build_database("should_save_and_retrieve_a_step_with_u64_max_era")
        .await
        .unwrap();
    crate::database::tests::should_save_and_retrieve_a_step_with_u64_max_era(
        test_context.db.clone(),
    )
    .await;
}

#[tokio::test]
async fn should_disallow_duplicate_event_id_from_source() {
    let test_context = build_database("should_disallow_duplicate_event_id_from_source")
        .await
        .unwrap();
    crate::database::tests::should_disallow_duplicate_event_id_from_source(test_context.db.clone())
        .await;
}

#[tokio::test]
async fn should_disallow_insert_of_existing_block_added() {
    let test_context = build_database("should_disallow_insert_of_existing_block_added")
        .await
        .unwrap();
    crate::database::tests::should_disallow_insert_of_existing_block_added(test_context.db.clone())
        .await;
}

#[tokio::test]
async fn should_disallow_insert_of_existing_deploy_accepted() {
    let test_context = build_database("should_disallow_insert_of_existing_deploy_accepted")
        .await
        .unwrap();
    crate::database::tests::should_disallow_insert_of_existing_deploy_accepted(
        test_context.db.clone(),
    )
    .await;
}

#[tokio::test]
async fn should_disallow_insert_of_existing_deploy_expired() {
    let test_context = build_database("should_disallow_insert_of_existing_deploy_expired")
        .await
        .unwrap();
    crate::database::tests::should_disallow_insert_of_existing_deploy_expired(
        test_context.db.clone(),
    )
    .await;
}

#[tokio::test]
async fn should_disallow_insert_of_existing_deploy_processed() {
    let test_context = build_database("should_disallow_insert_of_existing_deploy_processed")
        .await
        .unwrap();
    crate::database::tests::should_disallow_insert_of_existing_deploy_processed(
        test_context.db.clone(),
    )
    .await;
}

#[tokio::test]
async fn should_disallow_insert_of_existing_fault() {
    let test_context = build_database("should_disallow_insert_of_existing_fault")
        .await
        .unwrap();
    crate::database::tests::should_disallow_insert_of_existing_fault(test_context.db.clone()).await;
}

#[tokio::test]
async fn should_disallow_insert_of_existing_finality_signature() {
    let test_context = build_database("should_disallow_insert_of_existing_finality_signature")
        .await
        .unwrap();
    crate::database::tests::should_disallow_insert_of_existing_finality_signature(
        test_context.db.clone(),
    )
    .await;
}

#[tokio::test]
async fn should_disallow_insert_of_existing_step() {
    let test_context = build_database("should_disallow_insert_of_existing_step")
        .await
        .unwrap();
    crate::database::tests::should_disallow_insert_of_existing_step(test_context.db.clone()).await;
}

#[tokio::test]
async fn should_save_block_added_with_correct_event_type_id() {
    let mut test_rng = TestRng::new();

    let test_context = build_database("should_save_block_added_with_correct_event_type_id")
        .await
        .unwrap();
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

    let test_context = build_database("should_save_deploy_accepted_with_correct_event_type_id")
        .await
        .unwrap();
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

    let test_context = build_database("should_save_deploy_processed_with_correct_event_type_id")
        .await
        .unwrap();
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

    let test_context = build_database("should_save_deploy_expired_with_correct_event_type_id")
        .await
        .unwrap();
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

    let test_context = build_database("should_save_fault_with_correct_event_type_id")
        .await
        .unwrap();
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

    let test_context = build_database("should_save_finality_signature_with_correct_event_type_id")
        .await
        .unwrap();
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

    let test_context = build_database("should_save_step_with_correct_event_type_id")
        .await
        .unwrap();
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
    let test_context = build_database("should_save_and_retrieve_a_shutdown")
        .await
        .unwrap();
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
    let test_context = build_database("get_number_of_events_should_return_0")
        .await
        .unwrap();
    crate::database::tests::get_number_of_events_should_return_0(
        "postgres",
        test_context.db.clone(),
    )
    .await;
}

#[tokio::test]
async fn get_number_of_events_should_return_1_when_event_stored() {
    let test_context = build_database("get_number_of_events_should_return_1_when_event_stored")
        .await
        .unwrap();
    crate::database::tests::get_number_of_events_should_return_1_when_event_stored(
        test_context.db.clone(),
    )
    .await;
}
