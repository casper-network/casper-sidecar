use super::PostgreSqlDatabase;
use crate::{database::types::DDLConfiguration, database_writer_implementation};
use sea_query::PostgresQueryBuilder;
use sqlx::{postgres::PgQueryResult, Postgres};

database_writer_implementation!(
    PostgreSqlDatabase,
    Postgres,
    PgQueryResult,
    PostgresQueryBuilder,
    DDLConfiguration {
        is_big_integer_id: true,
        db_supports_unsigned: false,
    }
);
