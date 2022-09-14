use super::event_source::EventSource;
use sea_query::{
    error::Result as SqResult, ColumnDef, ForeignKey, ForeignKeyAction, Iden, InsertStatement,
    Query, Table, TableCreateStatement,
};

#[derive(Iden)]
pub(super) enum EventSourceOfEvent {
    Table,
    EventId,
    EventSourceId,
}

pub fn create_table_stmt() -> TableCreateStatement {
    Table::create()
        .table(EventSourceOfEvent::Table)
        .if_not_exists()
        .col(
            ColumnDef::new(EventSourceOfEvent::EventId)
                .integer()
                .not_null(),
        )
        .col(
            ColumnDef::new(EventSourceOfEvent::EventSourceId)
                .string()
                .not_null(),
        )
        .foreign_key(
            ForeignKey::create()
                .name("FK_event_source_id")
                .from(EventSourceOfEvent::Table, EventSourceOfEvent::EventSourceId)
                .to(EventSource::Table, EventSource::EventSourceId)
                .on_delete(ForeignKeyAction::Restrict)
                .on_update(ForeignKeyAction::Restrict),
        )
        .to_owned()
}

#[allow(unused)]
pub fn create_insert_stmt(event_id: u64, event_source_id: u8) -> SqResult<InsertStatement> {
    Query::insert()
        .into_table(EventSourceOfEvent::Table)
        .columns([
            EventSourceOfEvent::EventId,
            EventSourceOfEvent::EventSourceId,
        ])
        .values(vec![event_id.into(), event_source_id.into()])
        .map(|stmt| stmt.to_owned())
}
