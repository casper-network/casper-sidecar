use sea_query::{
    error::Result as SqResult, ColumnDef, Expr, ForeignKey, ForeignKeyAction, Iden,
    InsertStatement, Query, QueryStatementBuilder, SimpleExpr, Table, TableCreateStatement,
};

use super::{event_source::EventSource, event_type::EventType};

#[derive(Iden)]
pub(super) enum EventLog {
    Table,
    EventLogId,
    EventTypeId,
    EventSourceId,
    EventId,
    InsertedTimestamp,
    EmittedTimestamp,
}

// todo add constraint on [ EventSourceId + EventId + Timestamp] being UNIQUE

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
            EventLog::EventSourceId,
            EventLog::EventId,
        ])
        .exprs([
            Expr::val(event_type_id).into_simple_expr(),
            SimpleExpr::SubQuery(Box::new(
                Query::select()
                    .column(EventSource::EventSourceId)
                    .from(EventSource::Table)
                    .and_where(Expr::col(EventSource::EventSourceAddress).eq(event_source_address))
                    .to_owned()
                    .into_sub_query_statement(),
            )),
            Expr::val(event_id).into_simple_expr(),
        ])
        .map(|stmt| stmt.returning_col(EventLog::EventLogId).to_owned())?;

    Ok(insert_stmt)
}
