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
}

#[allow(clippy::too_many_lines)]
pub fn create_table_stmt(is_big_integer_id: bool) -> TableCreateStatement {
    let mut binding = ColumnDef::new(EventLog::EventLogId);
    let mut event_log_id_col_definition = binding.auto_increment().not_null().primary_key();
    if is_big_integer_id {
        event_log_id_col_definition = event_log_id_col_definition.big_integer();
    } else {
        event_log_id_col_definition = event_log_id_col_definition.integer();
    }
    Table::create()
        .table(EventLog::Table)
        .if_not_exists()
        .col(event_log_id_col_definition)
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
        .expr(Expr::col(Asterisk).count())
        .from(EventLog::Table)
        .to_owned()
}
