use sea_query::{
    error::Result as SqResult, ColumnDef, Expr, ForeignKey, ForeignKeyAction, Iden,
    InsertStatement, Query, SelectStatement, Table, TableCreateStatement,
};

use super::event_type::EventType;

#[derive(Iden)]
pub(super) enum EventLog {
    Table,
    EventLogId,
    EventTypeId,
    EventSourceId,
    EventId,
    Timestamp,
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
        .col(ColumnDef::new(EventLog::EventSourceId).integer().not_null())
        .col(ColumnDef::new(EventLog::EventId).integer().not_null())
        .col(
            ColumnDef::new(EventLog::Timestamp)
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
        .to_owned()
}

pub fn create_insert_stmt(
    event_type_id: u8,
    event_source_id: u8,
    event_id: u64,
) -> SqResult<InsertStatement> {
    Query::insert()
        .into_table(EventLog::Table)
        .columns([
            EventLog::EventTypeId,
            EventLog::EventSourceId,
            EventLog::EventId,
        ])
        .values(vec![
            event_type_id.into(),
            event_source_id.into(),
            event_id.into(),
        ])
        .map(|stmt| stmt.to_owned())
}

pub fn create_get_max_id_stmt() -> SelectStatement {
    Query::select()
        .column(EventLog::EventLogId)
        .from(EventLog::Table)
        .expr(Expr::col(EventLog::EventLogId).max())
        .to_owned()
}
