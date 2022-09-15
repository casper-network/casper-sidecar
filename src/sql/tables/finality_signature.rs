use sea_query::{
    error::Result as SqResult, BlobSize, ColumnDef, Expr, ForeignKey, ForeignKeyAction, Iden,
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
        .col(
            ColumnDef::new(FinalitySignature::Raw)
                .blob(BlobSize::Tiny)
                .not_null(),
        )
        .col(
            ColumnDef::new(FinalitySignature::EventLogId)
                .integer()
                .not_null(),
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
        .from(FinalitySignature::Table)
        .and_where(Expr::col(FinalitySignature::BlockHash).eq(block_hash))
        .to_owned()
}
