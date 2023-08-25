use sea_query::{
    error::Result as SqResult, ColumnDef, Expr, ForeignKey, ForeignKeyAction, Iden, Index,
    InsertStatement, Query, SelectStatement, Table, TableCreateStatement,
};

use super::event_type::EventType;

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
}

#[allow(clippy::too_many_lines)]
pub fn create_table_stmt() -> TableCreateStatement {
    Table::create()
        .table(EventLog::Table)
        .if_not_exists()
        .col(
            ColumnDef::new(EventLog::EventLogId)
                .big_unsigned()
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
        .col(ColumnDef::new(EventLog::EventId).unsigned().not_null())
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
) -> SqResult<InsertStatement> {
    let insert_stmt = Query::insert()
        .into_table(EventLog::Table)
        .columns([
            EventLog::EventTypeId,
            EventLog::EventSourceAddress,
            EventLog::EventId,
            EventLog::EventKey,
        ])
        .values(vec![
            event_type_id.into(),
            event_source_address.into(),
            event_id.into(),
            event_key.into(),
        ])
        .map(|stmt| stmt.returning_col(EventLog::EventLogId).to_owned())?;

    Ok(insert_stmt)
}

pub fn count() -> SelectStatement {
    Query::select()
        .expr(Expr::asterisk().count())
        .from(EventLog::Table)
        .to_owned()
}
