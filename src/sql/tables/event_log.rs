use crate::sql::tables::event_source::EventSource;
use crate::sql::tables::event_type::EventTypeId;
use crate::SqliteDb;
use sea_query::{
    error::Result as SqResult, ColumnDef, ColumnRef, Expr, ForeignKey, ForeignKeyAction, Iden,
    InsertStatement, Query, QueryStatementBuilder, SelectStatement, SimpleExpr, SqliteQueryBuilder,
    Table, TableCreateStatement,
};
use std::io::Error;
use std::path::Path;
use tracing::error;

use super::event_type::EventType;

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

// todo add constraint on [ EventSourceId + EventId ] being UNIQUE

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

//Query::insert()
//     .into_table(Glyph::Table)
//     .columns([Glyph::Aspect, Glyph::Image])
//     .exprs([Expr::val(1).into(), SimpleExpr::SubQuery(Box::new( // Or, `exprs_panic`
//         Query::select()
//         .column(Glyph::Aspect)
//         .from(Glyph::Table)
//         .to_owned()
//         .into_sub_query_statement()))])

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

    // format!(
    //     r#"INSERT INTO "event_log" ("event_type_id", "event_source_id", "event_id") VALUES ({}, (SELECT "event_source_id" FROM "event_source" WHERE "event_source_address" = "{}"), {}) RETURNING "event_log_id""#,
    //     event_type_id, event_source_address, event_id
    // )
}

pub fn create_get_latest_deploy_event_log_id() -> SelectStatement {
    Query::select()
        .column(EventLog::EventLogId)
        .and_where(Expr::col(EventLog::EventTypeId).is_in(vec![
            EventTypeId::DeployAccepted as u8,
            EventTypeId::DeployProcessed as u8,
            EventTypeId::DeployExpired as u8,
        ]))
        .limit(1)
        .to_owned()
}
