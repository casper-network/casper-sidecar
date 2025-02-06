use crate::database::{migration_manager::MigrationManager, sqlite_database::SqliteDatabase};
use crate::types::database::{
    DatabaseReader, DatabaseWriteError, Migration, MigrationScriptExecutor, StatementWrapper,
    TransactionWrapper,
};
use async_trait::async_trait;
use itertools::Itertools;
use sqlx::{Executor, Row};
use std::sync::Arc;

const MAX_CONNECTIONS: u32 = 100;

#[tokio::test]
async fn should_have_version_none_if_no_migrations_applied() {
    let sqlite_db = SqliteDatabase::new_in_memory_no_migrations(MAX_CONNECTIONS)
        .expect("Error opening database in memory");

    let apply_res = MigrationManager::apply_migrations(sqlite_db.clone(), vec![]).await;

    assert!(apply_res.is_ok());
    let version_res = sqlite_db.get_newest_migration_version().await;
    assert!(version_res.is_ok());
    assert!(version_res.unwrap().is_none());
}

#[tokio::test]
async fn should_store_failed_version_if_migration_was_erroneous() {
    let sqlite_db = SqliteDatabase::new_in_memory_no_migrations(MAX_CONNECTIONS)
        .expect("Error opening database in memory");

    let apply_res =
        MigrationManager::apply_migrations(sqlite_db.clone(), vec![build_failed_migration(1)])
            .await;

    assert!(apply_res.is_err());
    let version_res = sqlite_db.get_newest_migration_version().await;
    assert!(version_res.is_ok());
    assert!(version_res.unwrap() == Some((1, false)));
}

#[tokio::test]
async fn should_apply_migration() {
    let sqlite_db = SqliteDatabase::new_in_memory_no_migrations(MAX_CONNECTIONS)
        .expect("Error opening database in memory");

    let apply_res =
        MigrationManager::apply_migrations(sqlite_db.clone(), vec![build_ok_migration(1)]).await;

    assert!(apply_res.is_ok());
    let res = sqlite_db
        .connection_pool
        .fetch_all("SELECT * FROM x;")
        .await
        .unwrap();
    assert!(res.len() == 4);
    let got: Vec<u32> = res
        .iter()
        .map(|r| r.get_unchecked::<u32, &str>("a"))
        .collect();
    assert!(got == vec![10, 20, 30, 40]);
}

#[tokio::test]
async fn should_apply_migrations() {
    let sqlite_db = SqliteDatabase::new_in_memory_no_migrations(MAX_CONNECTIONS)
        .expect("Error opening database in memory");

    let apply_res = MigrationManager::apply_migrations(
        sqlite_db.clone(),
        vec![build_ok_migration(1), build_different_ok_migration(2)],
    )
    .await;

    assert!(apply_res.is_ok());
    let res_1 = sqlite_db
        .connection_pool
        .fetch_all("SELECT * FROM x;")
        .await
        .unwrap();
    let res_2 = sqlite_db
        .connection_pool
        .fetch_all("SELECT * FROM y;")
        .await
        .unwrap();
    let got_1: Vec<u32> = res_1
        .iter()
        .map(|r| r.get_unchecked::<u32, &str>("a"))
        .collect();
    let got_2: Vec<u32> = res_2
        .iter()
        .map(|r| r.get_unchecked::<u32, &str>("a"))
        .collect();
    assert!(got_1 == vec![10, 20, 30, 40]);
    assert!(got_2 == vec![7, 5, 125]);
    let version_res = sqlite_db.get_newest_migration_version().await;
    assert!(version_res.unwrap() == Some((2, true)));
}

#[tokio::test]
async fn given_ok_and_failing_migrations_first_should_be_applied_second_only_stored_in_migrations_table(
) {
    let sqlite_db = SqliteDatabase::new_in_memory_no_migrations(MAX_CONNECTIONS)
        .expect("Error opening database in memory");

    let apply_res = MigrationManager::apply_migrations(
        sqlite_db.clone(),
        vec![build_ok_migration(1), build_failed_migration(2)],
    )
    .await;

    assert_eq!(sqlite_db.get_number_of_tables().await, 2); //"Migrations" and "x" table should be created, "abcdef" should be reverted.
    assert!(apply_res.is_err());
    let version_res = sqlite_db.get_newest_migration_version().await;
    assert!(version_res.is_ok());
    assert!(version_res.unwrap() == Some((2, false)));
}

#[tokio::test]
async fn should_fail_if_migration_has_no_version() {
    let sqlite_db = SqliteDatabase::new_in_memory_no_migrations(MAX_CONNECTIONS)
        .expect("Error opening database in memory");
    let migration = build_no_version_migration();

    let apply_res = MigrationManager::apply_migrations(sqlite_db.clone(), vec![migration]).await;

    assert!(apply_res.is_err());
}

#[tokio::test]
async fn should_fail_if_two_migrations_have_the_same_version() {
    let sqlite_db = SqliteDatabase::new_in_memory_no_migrations(MAX_CONNECTIONS)
        .expect("Error opening database in memory");

    let apply_res = MigrationManager::apply_migrations(
        sqlite_db.clone(),
        vec![build_ok_migration(1), build_different_ok_migration(1)],
    )
    .await;

    assert!(apply_res.is_err());
}

#[tokio::test]
async fn given_shuffled_migrations_should_sort_by_version() {
    let sqlite_db = SqliteDatabase::new_in_memory_no_migrations(MAX_CONNECTIONS)
        .expect("Error opening database in memory");

    let apply_res = MigrationManager::apply_migrations(
        sqlite_db.clone(),
        vec![
            build_update_migration(3),
            build_ok_migration(1),
            build_different_ok_migration(2),
        ],
    )
    .await;

    assert!(apply_res.is_ok());
    let res = sqlite_db
        .connection_pool
        .fetch_all("SELECT * FROM x;")
        .await
        .unwrap();
    let got: Vec<u32> = res
        .iter()
        .map(|r| r.get_unchecked::<u32, &str>("a"))
        .collect();
    assert!(got == vec![555, 555, 555, 555]);
}

#[tokio::test]
async fn should_execute_script() {
    let sqlite_db = SqliteDatabase::new_in_memory_no_migrations(MAX_CONNECTIONS)
        .expect("Error opening database in memory");

    let apply_res =
        MigrationManager::apply_migrations(sqlite_db.clone(), vec![build_migration_with_script(1)])
            .await;

    assert!(apply_res.is_ok());
    let res = sqlite_db
        .connection_pool
        .fetch_all("SELECT * FROM y;")
        .await
        .unwrap();
    let got: Vec<u32> = res
        .iter()
        .map(|r| r.get_unchecked::<u32, &str>("a"))
        .collect();
    assert!(got == vec![1, 2, 7]);
    assert!(sqlite_db.get_newest_migration_version().await.unwrap() == Some((1, true)))
}

#[tokio::test]
async fn given_failed_migration_should_not_execute_next_migration_() {
    let sqlite_db = SqliteDatabase::new_in_memory_no_migrations(MAX_CONNECTIONS)
        .expect("Error opening database in memory");

    let apply_res = MigrationManager::apply_migrations(
        sqlite_db.clone(),
        vec![build_migration_with_failing_script(1)],
    )
    .await;
    assert!(apply_res.is_err());
    let apply_res =
        MigrationManager::apply_migrations(sqlite_db.clone(), vec![build_ok_migration(2)]).await;
    assert!(apply_res.is_err());
    assert_eq!(sqlite_db.get_number_of_tables().await, 1); //Only Migrations table should be present, build_ok_migration should not be executed at all
    assert!(sqlite_db.get_newest_migration_version().await.unwrap() == Some((1, false)))
}

#[tokio::test]
async fn should_store_failed_version_if_script_fails() {
    let sqlite_db = SqliteDatabase::new_in_memory_no_migrations(MAX_CONNECTIONS)
        .expect("Error opening database in memory");

    let apply_res = MigrationManager::apply_migrations(
        sqlite_db.clone(),
        vec![build_migration_with_failing_script(1)],
    )
    .await;

    assert_eq!(sqlite_db.get_number_of_tables().await, 1); //Only Migrations table should be present, "y" table should be reverted
    assert!(apply_res.is_err());
    assert!(sqlite_db.get_newest_migration_version().await.unwrap() == Some((1, false)))
}

fn build_ok_migration(version: u32) -> Migration {
    Migration {
        version: Some(version),
        statement_producers: |_| {
            Ok(vec![
                StatementWrapper::Raw(create_table_stmt("x")),
                StatementWrapper::Raw(String::from("INSERT INTO x VALUES(10), (20), (30), (40);")),
            ])
        },
        script_executor: None,
    }
}

fn build_update_migration(version: u32) -> Migration {
    Migration {
        version: Some(version),
        statement_producers: |_| {
            Ok(vec![StatementWrapper::Raw(String::from(
                "UPDATE x SET a = 555;",
            ))])
        },
        script_executor: None,
    }
}

fn build_no_version_migration() -> Migration {
    Migration {
        version: None,
        statement_producers: |_| {
            Ok(vec![
                StatementWrapper::Raw(create_table_stmt("x")),
                StatementWrapper::Raw(String::from("INSERT INTO x VALUES(10), (20), (30), (40);")),
            ])
        },
        script_executor: None,
    }
}

fn build_different_ok_migration(version: u32) -> Migration {
    Migration {
        version: Some(version),
        statement_producers: |_| {
            Ok(vec![
                StatementWrapper::Raw(create_table_stmt("y")),
                StatementWrapper::Raw(String::from("INSERT INTO y VALUES(7), (5), (125);")),
            ])
        },
        script_executor: None,
    }
}

fn build_migration_with_script(version: u32) -> Migration {
    let script = InsertValuesInTableScript {
        table_name: "y".to_string(),
        values: vec!["1".to_string(), "2".to_string(), "7".to_string()],
    };
    Migration {
        version: Some(version),
        statement_producers: |_| Ok(vec![StatementWrapper::Raw(create_table_stmt("y"))]),
        script_executor: Some(Arc::new(script)),
    }
}

fn build_migration_with_failing_script(version: u32) -> Migration {
    let script = FailingScript {};
    Migration {
        version: Some(version),
        statement_producers: |_| Ok(vec![StatementWrapper::Raw(create_table_stmt("y"))]),
        script_executor: Some(Arc::new(script)),
    }
}

fn create_table_stmt(table_name: &str) -> String {
    format!("CREATE TABLE {table_name}(a int);")
}

fn build_failed_migration(version: u32) -> Migration {
    Migration {
        version: Some(version),
        statement_producers: |_| {
            Ok(vec![
                StatementWrapper::Raw(String::from("CREATE TABLE abcdef(a int);")),
                StatementWrapper::Raw(String::from("CREATE TAB();")),
            ])
        },
        script_executor: None,
    }
}

struct InsertValuesInTableScript {
    table_name: String,
    values: Vec<String>,
}

#[async_trait]
impl MigrationScriptExecutor for InsertValuesInTableScript {
    async fn execute(
        &self,
        transaction: Arc<dyn TransactionWrapper>,
    ) -> Result<(), DatabaseWriteError> {
        let values_sql_part = self.values.iter().map(|el| format!("({})", el)).join(",");
        transaction
            .execute(format!("INSERT INTO {} VALUES {}", self.table_name, values_sql_part).as_str())
            .await
    }
}

struct FailingScript {}

#[async_trait]
impl MigrationScriptExecutor for FailingScript {
    async fn execute(
        &self,
        transaction: Arc<dyn TransactionWrapper>,
    ) -> Result<(), DatabaseWriteError> {
        transaction.execute("CREATE TAB").await
    }
}
