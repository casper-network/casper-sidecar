use sea_query::{
    error::Result as SqResult, ColumnDef, ForeignKey, ForeignKeyAction, Iden, Index,
    InsertStatement, Query, Table, TableCreateStatement,
};

use super::{event_log::EventLog, transaction_type::TransactionType};

#[derive(Iden)]
pub(super) enum TransactionEvent {
    Table,
    EventLogId,
    TransactionTypeId,
    TransactionHash,
}

pub fn create_table_stmt() -> TableCreateStatement {
    Table::create()
        .table(TransactionEvent::Table)
        .if_not_exists()
        .col(
            ColumnDef::new(TransactionEvent::EventLogId)
                .big_unsigned()
                .not_null(),
        )
        .col(
            ColumnDef::new(TransactionEvent::TransactionHash)
                .string()
                .not_null(),
        )
        .col(
            ColumnDef::new(TransactionEvent::TransactionTypeId)
                .tiny_unsigned()
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
        .from(TransactionEvent::Table, TransactionEvent::TransactionTypeId)
        .to(TransactionType::Table, TransactionType::TransactionTypeId)
        .on_delete(ForeignKeyAction::Restrict)
        .on_update(ForeignKeyAction::Restrict)
        .to_owned()
}

fn event_log_fk() -> sea_query::ForeignKeyCreateStatement {
    ForeignKey::create()
        .name("FK_event_log_id")
        .from(TransactionEvent::Table, TransactionEvent::EventLogId)
        .to(EventLog::Table, EventLog::EventLogId)
        .on_delete(ForeignKeyAction::Restrict)
        .on_update(ForeignKeyAction::Restrict)
        .to_owned()
}

fn primary_key() -> sea_query::IndexCreateStatement {
    Index::create()
        .primary()
        .name("PDX_TransactionEvent")
        .col(TransactionEvent::TransactionHash)
        .col(TransactionEvent::TransactionTypeId)
        .col(TransactionEvent::EventLogId)
        .to_owned()
}

pub fn create_insert_stmt(
    event_log_id: u64,
    transaction_type: u8,
    transaction_hash: String,
) -> SqResult<InsertStatement> {
    let insert_stmt = Query::insert()
        .into_table(TransactionEvent::Table)
        .columns([
            TransactionEvent::TransactionTypeId,
            TransactionEvent::EventLogId,
            TransactionEvent::TransactionHash,
        ])
        .values(vec![
            transaction_type.into(),
            event_log_id.into(),
            transaction_hash.into(),
        ])?
        .to_owned();

    Ok(insert_stmt)
}
