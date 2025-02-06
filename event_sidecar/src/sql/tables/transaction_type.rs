use sea_query::{
    error::Result as SqResult, ColumnDef, Iden, InsertStatement, OnConflict, Query, Table,
    TableCreateStatement,
};

#[derive(Clone)]
pub enum TransactionTypeId {
    Deploy = 0,
    Version1 = 1,
}

#[allow(clippy::enum_variant_names)]
#[derive(Iden)]
pub(super) enum TransactionType {
    #[iden = "TransactionType"]
    Table,
    TransactionTypeId,
    TransactionTypeName,
}

pub fn create_table_stmt() -> TableCreateStatement {
    Table::create()
        .table(TransactionType::Table)
        .if_not_exists()
        .col(
            ColumnDef::new(TransactionType::TransactionTypeId)
                .integer()
                .not_null()
                .primary_key(),
        )
        .col(
            ColumnDef::new(TransactionType::TransactionTypeName)
                .string()
                .not_null()
                .unique_key(),
        )
        .to_owned()
}

pub fn create_initialise_stmt() -> SqResult<InsertStatement> {
    Ok(Query::insert()
        .into_table(TransactionType::Table)
        .columns([
            TransactionType::TransactionTypeId,
            TransactionType::TransactionTypeName,
        ])
        .values(vec![
            (TransactionTypeId::Deploy as u8).into(),
            "Deploy".into(),
        ])?
        .values(vec![
            (TransactionTypeId::Version1 as u8).into(),
            "Version1".into(),
        ])?
        .on_conflict(
            OnConflict::column(TransactionType::TransactionTypeId)
                .do_nothing()
                .to_owned(),
        )
        .to_owned())
}
