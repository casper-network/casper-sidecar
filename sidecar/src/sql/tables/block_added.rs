use sea_query::{
    error::Result as SqResult, ColumnDef, Expr, ForeignKey, ForeignKeyAction, Iden, Index,
    InsertStatement, Query, SelectStatement, Table, TableCreateStatement,
};

use super::event_log::EventLog;

#[derive(Iden)]
enum BlockAdded {
    #[iden = "BlockAdded"]
    Table,
    Height,
    BlockHash,
    Raw,
    EventLogId,
}

pub fn create_table_stmt() -> TableCreateStatement {
    Table::create()
        .table(BlockAdded::Table)
        .if_not_exists()
        .col(
            ColumnDef::new(BlockAdded::Height)
                .big_unsigned()
                .not_null()
                .unique_key(),
        )
        .col(ColumnDef::new(BlockAdded::BlockHash).string().not_null())
        .col(ColumnDef::new(BlockAdded::Raw).text().not_null())
        .col(
            ColumnDef::new(BlockAdded::EventLogId)
                .big_unsigned()
                .not_null(),
        )
        .index(
            Index::create()
                .primary()
                .name("PDX_BlockAdded")
                .col(BlockAdded::BlockHash)
                .col(BlockAdded::Height),
        )
        .foreign_key(
            ForeignKey::create()
                .name("FK_event_log_id")
                .from(BlockAdded::Table, BlockAdded::EventLogId)
                .to(EventLog::Table, EventLog::EventLogId)
                .on_delete(ForeignKeyAction::Restrict)
                .on_update(ForeignKeyAction::Restrict),
        )
        .to_owned()
}

pub fn create_insert_stmt(
    height: u64,
    block_hash: String,
    raw: String,
    event_log_id: u64,
) -> SqResult<InsertStatement> {
    Query::insert()
        .into_table(BlockAdded::Table)
        .columns([
            BlockAdded::Height,
            BlockAdded::BlockHash,
            BlockAdded::Raw,
            BlockAdded::EventLogId,
        ])
        .values(vec![
            height.into(),
            block_hash.into(),
            raw.into(),
            event_log_id.into(),
        ])
        .map(|stmt| stmt.to_owned())
}

pub fn create_get_by_hash_stmt(block_hash: String) -> SelectStatement {
    Query::select()
        .column(BlockAdded::Raw)
        .from(BlockAdded::Table)
        .and_where(Expr::col(BlockAdded::BlockHash).eq(block_hash))
        .to_owned()
}

pub fn create_get_by_height_stmt(height: u64) -> SelectStatement {
    Query::select()
        .column(BlockAdded::Raw)
        .from(BlockAdded::Table)
        .and_where(Expr::col(BlockAdded::Height).eq(height))
        .to_owned()
}

pub fn create_get_latest_stmt() -> SelectStatement {
    let select_max = Query::select()
        .expr(Expr::col(BlockAdded::Height).max())
        .from(BlockAdded::Table)
        .to_owned();
    Query::select()
        .column(BlockAdded::Raw)
        .from(BlockAdded::Table)
        .and_where(Expr::col(BlockAdded::Height).in_subquery(select_max))
        .to_owned()
}
