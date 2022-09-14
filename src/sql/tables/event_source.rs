use sea_query::{
    error::Result as SqResult, ColumnDef, Expr, Iden, InsertStatement, Query,
    SelectStatement, Table, TableCreateStatement,
};

#[derive(Iden)]
pub(super) enum EventSource {
    Table,
    EventSourceId,
    EventSourceAddress,
}

pub fn create_table_stmt() -> TableCreateStatement {
    Table::create()
        .table(EventSource::Table)
        .if_not_exists()
        .col(
            ColumnDef::new(EventSource::EventSourceId)
                .integer()
                .auto_increment()
                .not_null()
                .primary_key(),
        )
        .col(
            ColumnDef::new(EventSource::EventSourceAddress)
                .string()
                .not_null(),
        )
        .to_owned()
}

pub fn create_insert_stmt(event_source_address: String) -> SqResult<InsertStatement> {
    Query::insert()
        .into_table(EventSource::Table)
        .columns([EventSource::EventSourceAddress])
        .values(vec![event_source_address.into()])
        .map(|stmt| stmt.to_owned())
}

pub fn create_select_id_by_address_stmt(address: String) -> SelectStatement {
    Query::select()
        .columns([EventSource::EventSourceId, EventSource::EventSourceAddress])
        .expr(Expr::col(EventSource::EventSourceAddress).eq(address))
        .from(EventSource::Table)
        .to_owned()
}
