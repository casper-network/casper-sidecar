use sea_query::{
    error::Result as SqResult, ColumnDef, ForeignKey, ForeignKeyAction, Iden, Index,
    InsertStatement, Query, Table, TableCreateStatement,
};

use super::event_type::EventType;

#[derive(Iden)]
pub(super) enum EventLog {
    Table,
    EventLogId,
    EventTypeId,
    EventSourceAddress,
    EventId,
    InsertedTimestamp,
    EmittedTimestamp,
}

pub fn create_table_stmt() -> TableCreateStatement {
    Table::create()
        .table(EventLog::Table)
        .if_not_exists()
        .col(
            ColumnDef::new(EventLog::EventLogId)
                .integer()
                .auto_increment()
                .not_null()
                .primary_key(),
        )
        .col(ColumnDef::new(EventLog::EventTypeId).integer().not_null())
        .col(
            ColumnDef::new(EventLog::EventSourceAddress)
                .string()
                .not_null(),
        )
        .col(ColumnDef::new(EventLog::EventId).integer().not_null())
        .col(
            ColumnDef::new(EventLog::InsertedTimestamp)
                .timestamp()
                .not_null()
                // This can be replaced with better syntax when https://github.com/SeaQL/sea-query/pull/428 merges.
                .extra("DEFAULT CURRENT_TIMESTAMP".to_string()),
        )
        .col(ColumnDef::new(EventLog::EmittedTimestamp).timestamp())
        .foreign_key(
            ForeignKey::create()
                .name("FK_event_type_id")
                .from(EventLog::Table, EventLog::EventTypeId)
                .to(EventType::Table, EventType::EventTypeId)
                .on_delete(ForeignKeyAction::Restrict)
                .on_update(ForeignKeyAction::Restrict),
        )
        // todo is this the correct naming for the index?
        .index(
            Index::create()
                .unique()
                .name("UDX_event_log")
                .col(EventLog::EventSourceAddress)
                .col(EventLog::EventId)
                .col(EventLog::InsertedTimestamp),
        )
        .to_owned()
}

pub fn create_insert_stmt(
    event_type_id: u8,
    event_source_address: &str,
    event_id: u64,
) -> SqResult<InsertStatement> {
    let insert_stmt = Query::insert()
        .into_table(EventLog::Table)
        .columns([
            EventLog::EventTypeId,
            EventLog::EventSourceAddress,
            EventLog::EventId,
        ])
        .values(vec![
            event_type_id.into(),
            event_source_address.into(),
            event_id.into(),
        ])
        .map(|stmt| stmt.returning_col(EventLog::EventLogId).to_owned())?;

    Ok(insert_stmt)
}
