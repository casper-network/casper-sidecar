use sea_query::{
    error::Result as SqResult, ColumnDef, Iden, InsertStatement, Query, Table, TableCreateStatement,
};

#[derive(Iden)]
pub(super) enum DeployEventType {
    Table,
    DeployEventTypeId,
    DeployEventTypeName,
}

pub enum DeployEventTypeId {
    Accepted = 1,
    Expired = 2,
    Processed = 3,
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
            (DeployEventTypeId::Accepted as u8).into(),
            "DeployAccepted".into(),
        ])?
        .values(vec![
            (DeployEventTypeId::Expired as u8).into(),
            "DeployExpired".into(),
        ])?
        .values(vec![
            (DeployEventTypeId::Processed as u8).into(),
            "DeployProcessed".into(),
        ])?
        .to_owned())
}
