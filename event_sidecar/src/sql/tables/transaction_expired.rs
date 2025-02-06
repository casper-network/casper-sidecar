use sea_query::{
    error::Result as SqResult, ColumnDef, Expr, ForeignKey, ForeignKeyAction, Iden, Index,
    InsertStatement, Query, SelectStatement, Table, TableCreateStatement,
};

use super::{event_log::EventLog, transaction_type::TransactionType};

#[derive(Iden)]
pub(super) enum TransactionExpired {
    #[iden = "TransactionExpired"]
    Table,
    TransactionTypeId,
    TransactionHash,
    Raw,
    EventLogId,
}

pub fn create_table_stmt() -> TableCreateStatement {
    Table::create()
        .table(TransactionExpired::Table)
        .if_not_exists()
        .col(
            ColumnDef::new(TransactionExpired::TransactionHash)
                .string()
                .not_null(),
        )
        .col(
            ColumnDef::new(TransactionExpired::TransactionTypeId)
                .tiny_unsigned()
                .not_null(),
        )
        .col(ColumnDef::new(TransactionExpired::Raw).text().not_null())
        .col(
            ColumnDef::new(TransactionExpired::EventLogId)
                .big_unsigned()
                .not_null(),
        )
        .index(&mut primary_key())
        .foreign_key(&mut event_log_fk())
        .foreign_key(&mut transaction_type_fk())
        .to_owned()
}

fn transaction_type_fk() -> sea_query::ForeignKeyCreateStatement {
    ForeignKey::create()
        .name("FK_transaction_type_id")
        .from(
            TransactionExpired::Table,
            TransactionExpired::TransactionTypeId,
        )
        .to(TransactionType::Table, TransactionType::TransactionTypeId)
        .on_delete(ForeignKeyAction::Restrict)
        .on_update(ForeignKeyAction::Restrict)
        .to_owned()
}

fn event_log_fk() -> sea_query::ForeignKeyCreateStatement {
    ForeignKey::create()
        .name("FK_event_log_id")
        .from(TransactionExpired::Table, TransactionExpired::EventLogId)
        .to(EventLog::Table, EventLog::EventLogId)
        .on_delete(ForeignKeyAction::Restrict)
        .on_update(ForeignKeyAction::Restrict)
        .to_owned()
}

fn primary_key() -> sea_query::IndexCreateStatement {
    Index::create()
        .primary()
        .name("PDX_TransactionExpired")
        .col(TransactionExpired::TransactionHash)
        .to_owned()
}

pub fn create_insert_stmt(
    transaction_type: u8,
    transaction_hash: String,
    event_log_id: u64,
    raw: String,
) -> SqResult<InsertStatement> {
    Query::insert()
        .into_table(TransactionExpired::Table)
        .columns([
            TransactionExpired::TransactionTypeId,
            TransactionExpired::TransactionHash,
            TransactionExpired::EventLogId,
            TransactionExpired::Raw,
        ])
        .values(vec![
            transaction_type.into(),
            transaction_hash.into(),
            event_log_id.into(),
            raw.into(),
        ])
        .map(|stmt| stmt.to_owned())
}

pub fn create_get_by_hash_stmt(transaction_type: u8, transaction_hash: String) -> SelectStatement {
    Query::select()
        .column(TransactionExpired::Raw)
        .column(EventLog::ApiVersion)
        .column(EventLog::NetworkName)
        .from(TransactionExpired::Table)
        .left_join(
            EventLog::Table,
            Expr::col((EventLog::Table, EventLog::EventLogId))
                .equals((TransactionExpired::Table, TransactionExpired::EventLogId)),
        )
        .and_where(Expr::col(TransactionExpired::TransactionTypeId).eq(transaction_type))
        .and_where(Expr::col(TransactionExpired::TransactionHash).eq(transaction_hash))
        .to_owned()
}
