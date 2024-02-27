use sea_query::{
    error::Result as SqResult, ColumnDef, Expr, ForeignKey, ForeignKeyAction, Iden, Index,
    InsertStatement, Query, SelectStatement, Table, TableCreateStatement,
};

use super::{event_log::EventLog, transaction_type::TransactionType};

#[derive(Iden)]
pub enum TransactionProcessed {
    #[iden = "TransactionProcessed"]
    Table,
    TransactionHash,
    TransactionTypeId,
    Raw,
    EventLogId,
}

pub fn create_table_stmt() -> TableCreateStatement {
    Table::create()
        .table(TransactionProcessed::Table)
        .if_not_exists()
        .col(
            ColumnDef::new(TransactionProcessed::TransactionHash)
                .string()
                .not_null(),
        )
        .col(
            ColumnDef::new(TransactionProcessed::TransactionTypeId)
                .tiny_unsigned()
                .not_null(),
        )
        .col(ColumnDef::new(TransactionProcessed::Raw).text().not_null())
        .col(
            ColumnDef::new(TransactionProcessed::EventLogId)
                .big_unsigned()
                .not_null(),
        )
        .index(
            &mut primary_key(),
        )
        .foreign_key(
            &mut event_log_fk(),
        )
        .foreign_key(
            &mut transaction_type_fk(),
        )
        .to_owned()
}

fn transaction_type_fk() -> sea_query::ForeignKeyCreateStatement {
    ForeignKey::create()
        .name("FK_transaction_type_id")
        .from(
            TransactionProcessed::Table,
            TransactionProcessed::TransactionTypeId,
        )
        .to(TransactionType::Table, TransactionType::TransactionTypeId)
        .on_delete(ForeignKeyAction::Restrict)
        .on_update(ForeignKeyAction::Restrict)
        .to_owned()
}

fn event_log_fk() -> sea_query::ForeignKeyCreateStatement {
    ForeignKey::create()
        .name("FK_event_log_id")
        .from(
            TransactionProcessed::Table,
            TransactionProcessed::EventLogId,
        )
        .to(EventLog::Table, EventLog::EventLogId)
        .on_delete(ForeignKeyAction::Restrict)
        .on_update(ForeignKeyAction::Restrict)
        .to_owned()
}

fn primary_key() -> sea_query::IndexCreateStatement {
    Index::create()
        .name("PDX_TransactionProcessed")
        .col(TransactionProcessed::TransactionHash)
        .col(TransactionProcessed::TransactionTypeId)
        .primary()
        .to_owned()
}

pub fn create_insert_stmt(
    transaction_type: u8,
    transaction_hash: String,
    raw: String,
    event_log_id: u64,
) -> SqResult<InsertStatement> {
    Query::insert()
        .into_table(TransactionProcessed::Table)
        .columns([
            TransactionProcessed::TransactionTypeId,
            TransactionProcessed::TransactionHash,
            TransactionProcessed::Raw,
            TransactionProcessed::EventLogId,
        ])
        .values(vec![
            transaction_type.into(),
            transaction_hash.into(),
            raw.into(),
            event_log_id.into(),
        ])
        .map(|stmt| stmt.to_owned())
}

pub fn create_get_by_hash_stmt(transaction_type: u8, transaction_hash: String) -> SelectStatement {
    Query::select()
        .column(TransactionProcessed::Raw)
        .from(TransactionProcessed::Table)
        .and_where(Expr::col(TransactionProcessed::TransactionTypeId).eq(transaction_type))
        .and_where(Expr::col(TransactionProcessed::TransactionHash).eq(transaction_hash))
        .to_owned()
}
