use super::SqliteDatabase;
use crate::database::types::DDLConfiguration;
use sea_query::SqliteQueryBuilder;
#[cfg(test)]
use sqlx::sqlite::SqliteRow;
use sqlx::{sqlite::SqliteQueryResult, Sqlite};

database_writer_implementation!(
    SqliteDatabase,
    Sqlite,
    SqliteQueryResult,
    SqliteQueryBuilder,
    DDLConfiguration {
        db_supports_unsigned: true,
    }
);

#[cfg(test)]
impl SqliteDatabase {
    pub(super) async fn fetch_one(&self, sql: &str) -> SqliteRow {
        self.connection_pool
            .fetch_one(sql)
            .await
            .expect("Error executing provided SQL")
    }
}
