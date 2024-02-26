use sea_query::{
    error::Result as SqResult, ColumnDef, Expr, ForeignKey, ForeignKeyAction, Iden, Index,
    InsertStatement, Query, SelectStatement, Table, TableCreateStatement,
};

use super::{event_log::EventLog, transaction_type::TransactionType};

#[derive(Iden)]
pub(super) enum TransactionAccepted {
    #[iden = "TransactionAccepted"]
    Table,
    TransactionHash,
    TransactionTypeId,
    Raw,
    EventLogId,
}

pub fn create_table_stmt() -> TableCreateStatement {
    Table::create()
        .table(TransactionAccepted::Table)
        .if_not_exists()
        .col(
            ColumnDef::new(TransactionAccepted::TransactionHash)
                .string()
                .not_null(),
        )
        .col(
            ColumnDef::new(TransactionAccepted::TransactionTypeId)
                .tiny_unsigned()
                .not_null(),
        )
        .col(ColumnDef::new(TransactionAccepted::Raw).text().not_null())
        .col(
            ColumnDef::new(TransactionAccepted::EventLogId)
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
            TransactionAccepted::Table,
            TransactionAccepted::TransactionTypeId,
        )
        .to(TransactionType::Table, TransactionType::TransactionTypeId)
        .on_delete(ForeignKeyAction::Restrict)
        .on_update(ForeignKeyAction::Restrict)
        .to_owned()
}

fn event_log_fk() -> sea_query::ForeignKeyCreateStatement {
    ForeignKey::create()
        .name("FK_event_log_id")
        .from(TransactionAccepted::Table, TransactionAccepted::EventLogId)
        .to(EventLog::Table, EventLog::EventLogId)
        .on_delete(ForeignKeyAction::Restrict)
        .on_update(ForeignKeyAction::Restrict)
        .to_owned()
}

fn primary_key() -> sea_query::IndexCreateStatement {
    Index::create()
        .name("PDX_TransactionAccepted")
        .col(TransactionAccepted::TransactionTypeId)
        .col(TransactionAccepted::TransactionHash)
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
        .into_table(TransactionAccepted::Table)
        .columns([
            TransactionAccepted::TransactionTypeId,
            TransactionAccepted::TransactionHash,
            TransactionAccepted::Raw,
            TransactionAccepted::EventLogId,
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
        .column(TransactionAccepted::Raw)
        .from(TransactionAccepted::Table)
        .and_where(Expr::col(TransactionAccepted::TransactionTypeId).eq(transaction_type))
        .and_where(Expr::col(TransactionAccepted::TransactionHash).eq(transaction_hash))
        .to_owned()
}
