use sea_query::{
    error::Result as SqResult, ColumnDef, Expr, ForeignKey, ForeignKeyAction, Iden, Index,
    InsertStatement, Query, SelectStatement, Table, TableCreateStatement,
};

use super::event_log::EventLog;

#[derive(Iden)]
enum FinalitySignature {
    #[iden = "FinalitySignature"]
    Table,
    BlockHash,
    PublicKey,
    Raw,
    EventLogId,
}

pub fn create_table_stmt() -> TableCreateStatement {
    Table::create()
        .table(FinalitySignature::Table)
        .if_not_exists()
        .col(
            ColumnDef::new(FinalitySignature::BlockHash)
                .string()
                .not_null(),
        )
        .col(
            ColumnDef::new(FinalitySignature::PublicKey)
                .string()
                .not_null(),
        )
        .col(ColumnDef::new(FinalitySignature::Raw).text().not_null())
        .col(
            ColumnDef::new(FinalitySignature::EventLogId)
                .big_unsigned()
                .not_null(),
        )
        .index(
            Index::create()
                .name("PDX_FinalitySignature")
                .col(FinalitySignature::BlockHash)
                .col(FinalitySignature::PublicKey)
                .primary(),
        )
        .foreign_key(
            ForeignKey::create()
                .name("FK_event_log_id")
                .from(FinalitySignature::Table, FinalitySignature::EventLogId)
                .to(EventLog::Table, EventLog::EventLogId)
                .on_delete(ForeignKeyAction::Restrict)
                .on_update(ForeignKeyAction::Restrict),
        )
        .to_owned()
}

pub fn create_insert_stmt(
    block_hash: String,
    public_key: String,
    raw: String,
    event_log_id: u64,
) -> SqResult<InsertStatement> {
    Query::insert()
        .into_table(FinalitySignature::Table)
        .columns([
            FinalitySignature::BlockHash,
            FinalitySignature::PublicKey,
            FinalitySignature::Raw,
            FinalitySignature::EventLogId,
        ])
        .values(vec![
            block_hash.into(),
            public_key.into(),
            raw.into(),
            event_log_id.into(),
        ])
        .map(|stmt| stmt.to_owned())
}

pub fn create_get_finality_signatures_by_block_stmt(block_hash: String) -> SelectStatement {
    Query::select()
        .column(FinalitySignature::Raw)
        .column(EventLog::ApiVersion)
        .column(EventLog::NetworkName)
        .from(FinalitySignature::Table)
        .left_join(
            EventLog::Table,
            Expr::col((EventLog::Table, EventLog::EventLogId))
                .equals((FinalitySignature::Table, FinalitySignature::EventLogId)),
        )
        .and_where(Expr::col(FinalitySignature::BlockHash).eq(block_hash))
        .to_owned()
}
