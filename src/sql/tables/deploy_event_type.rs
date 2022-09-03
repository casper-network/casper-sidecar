use sea_query::{
    error::Result as SqResult, ColumnDef, Expr, Iden, InsertStatement, Order, Query,
    SelectStatement, Table, TableCreateStatement,
};

#[derive(Iden)]
pub(super) enum DeployEventType {
    Table,
    DeployEventTypeId,
    DeployEventTypeName,
}

pub enum DeployEventTypeId {
    DeployAccepted = 1,
    DeployExpired = 2,
    DeployProcessed = 3,
}

pub fn create_table_stmt() -> TableCreateStatement {
    Table::create()
        .table(DeployEventType::Table)
        .if_not_exists()
        .col(
            ColumnDef::new(DeployEventType::DeployEventTypeId)
                .integer()
                .not_null()
                .primary_key(),
        )
        .col(
            ColumnDef::new(DeployEventType::DeployEventTypeName)
                .string()
                .not_null()
                .unique_key(),
        )
        .to_owned()
}

pub fn create_initialise_stmt() -> SqResult<InsertStatement> {
    Ok(Query::insert()
        .into_table(DeployEventType::Table)
        .columns([
            DeployEventType::DeployEventTypeId,
            DeployEventType::DeployEventTypeName,
        ])
        .values(vec![
            (DeployEventTypeId::DeployAccepted as u8).into(),
            "DeployAccepted".into(),
        ])?
        .values(vec![
            (DeployEventTypeId::DeployExpired as u8).into(),
            "DeployExpired".into(),
        ])?
        .values(vec![
            (DeployEventTypeId::DeployProcessed as u8).into(),
            "DeployProcessed".into(),
        ])?
        .to_owned())
}
