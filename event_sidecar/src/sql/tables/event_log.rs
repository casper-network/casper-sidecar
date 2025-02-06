use sea_query::{
    error::Result as SqResult, Asterisk, ColumnDef, Expr, ForeignKey, ForeignKeyAction, Iden,
    Index, InsertStatement, Query, SelectStatement, Table, TableCreateStatement,
};

use super::event_type::EventType;

#[allow(clippy::enum_variant_names)]
#[derive(Iden)]
pub enum EventLog {
    Table,
    EventLogId,
    EventTypeId,
    EventSourceAddress,
    EventId,
    EventKey,
    InsertedTimestamp,
    EmittedTimestamp,
    ApiVersion,
    NetworkName,
}

#[allow(clippy::too_many_lines)]
pub fn create_table_stmt() -> TableCreateStatement {
    Table::create()
        .table(EventLog::Table)
        .if_not_exists()
        .col(
            ColumnDef::new(EventLog::EventLogId)
                .big_integer()
                .auto_increment()
                .not_null()
                .primary_key(),
        )
        .col(
            ColumnDef::new(EventLog::EventTypeId)
                .tiny_unsigned()
                .not_null(),
        )
        .col(
            ColumnDef::new(EventLog::EventSourceAddress)
                .string()
                .not_null(),
        )
        //EventId is big_integer to accomodate postgresql -> in postgres there is no notion of unsigned columns, so we need to extend
        // the data type to fit u32::MAX
        .col(ColumnDef::new(EventLog::EventId).big_integer().not_null())
        .col(ColumnDef::new(EventLog::EventKey).string().not_null())
        .col(
            ColumnDef::new(EventLog::InsertedTimestamp)
                .timestamp()
                .not_null()
                // This can be replaced with better syntax when https://github.com/SeaQL/sea-query/pull/428 merges.
                .extra("DEFAULT CURRENT_TIMESTAMP".to_string()),
        )
        .col(
            ColumnDef::new(EventLog::EmittedTimestamp)
                .timestamp()
                .not_null()
                // This can be replaced with better syntax when https://github.com/SeaQL/sea-query/pull/428 merges.
                .extra("DEFAULT CURRENT_TIMESTAMP".to_string()),
        )
        .col(ColumnDef::new(EventLog::ApiVersion).string().not_null())
        .col(ColumnDef::new(EventLog::NetworkName).string().not_null())
        .foreign_key(
            ForeignKey::create()
                .name("FK_event_type_id")
                .from(EventLog::Table, EventLog::EventTypeId)
                .to(EventType::Table, EventType::EventTypeId)
                .on_delete(ForeignKeyAction::Restrict)
                .on_update(ForeignKeyAction::Restrict),
        )
        .index(
            Index::create()
                .unique()
                .name("UDX_event_log")
                .col(EventLog::EventSourceAddress)
                .col(EventLog::EventId)
                .col(EventLog::EventTypeId)
                .col(EventLog::EventKey),
        )
        .to_owned()
}

pub fn create_insert_stmt(
    event_type_id: u8,
    event_source_address: &str,
    event_id: u32,
    event_key: &str,
    api_version: &str,
    network_name: &str,
) -> SqResult<InsertStatement> {
    let insert_stmt = Query::insert()
        .into_table(EventLog::Table)
        .columns([
            EventLog::EventTypeId,
            EventLog::EventSourceAddress,
            EventLog::EventId,
            EventLog::EventKey,
            EventLog::ApiVersion,
            EventLog::NetworkName,
        ])
        .values(vec![
            event_type_id.into(),
            event_source_address.into(),
            event_id.into(),
            event_key.into(),
            api_version.into(),
            network_name.into(),
        ])
        .map(|stmt| stmt.returning_col(EventLog::EventLogId).to_owned())?;

    Ok(insert_stmt)
}

pub fn count() -> SelectStatement {
    Query::select()
        .expr(Expr::col(Asterisk).count())
        .from(EventLog::Table)
        .to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_query::{PostgresQueryBuilder, SqliteQueryBuilder};

    #[test]
    fn should_prepare_create_stmt_for_sqlite() {
        let expected_sql = r#"CREATE TABLE IF NOT EXISTS "event_log" ( "event_log_id" integer NOT NULL PRIMARY KEY AUTOINCREMENT, "event_type_id" integer NOT NULL, "event_source_address" text NOT NULL, "event_id" bigint NOT NULL, "event_key" text NOT NULL, "inserted_timestamp" text NOT NULL DEFAULT CURRENT_TIMESTAMP, "emitted_timestamp" text NOT NULL DEFAULT CURRENT_TIMESTAMP, "api_version" text NOT NULL, "network_name" text NOT NULL, CONSTRAINT "UDX_event_log" UNIQUE ("event_source_address", "event_id", "event_type_id", "event_key"), FOREIGN KEY ("event_type_id") REFERENCES "event_type" ("event_type_id") ON DELETE RESTRICT ON UPDATE RESTRICT )"#;
        let stmt = create_table_stmt().to_string(SqliteQueryBuilder);
        assert_eq!(stmt.to_string(), expected_sql);
    }

    #[test]
    fn should_prepare_create_stmt_for_postgres() {
        let expected_sql = r#"CREATE TABLE IF NOT EXISTS "event_log" ( "event_log_id" bigserial NOT NULL PRIMARY KEY, "event_type_id" smallint NOT NULL, "event_source_address" varchar NOT NULL, "event_id" bigint NOT NULL, "event_key" varchar NOT NULL, "inserted_timestamp" timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP, "emitted_timestamp" timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP, "api_version" varchar NOT NULL, "network_name" varchar NOT NULL, CONSTRAINT "UDX_event_log" UNIQUE ("event_source_address", "event_id", "event_type_id", "event_key"), CONSTRAINT "FK_event_type_id" FOREIGN KEY ("event_type_id") REFERENCES "event_type" ("event_type_id") ON DELETE RESTRICT ON UPDATE RESTRICT )"#;
        let stmt = create_table_stmt().to_string(PostgresQueryBuilder);
        assert_eq!(stmt.to_string(), expected_sql,);
    }
}
