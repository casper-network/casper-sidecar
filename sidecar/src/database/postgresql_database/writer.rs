use super::PostgreSqlDatabase;
use crate::{database::types::DatabaseSpecificConfiguration, database_writer_implementation};

use sea_query::PostgresQueryBuilder;
use sqlx::{postgres::PgQueryResult, Postgres};

impl PostgreSqlDatabase {}

database_writer_implementation!(
    PostgreSqlDatabase,
    Postgres,
    PgQueryResult,
    PostgresQueryBuilder,
    DatabaseSpecificConfiguration {
        is_big_integer_id: true,
        db_supports_unsigned: false,
    }
);
