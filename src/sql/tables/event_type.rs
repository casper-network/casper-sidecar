use sea_query::{
    error::Result as SqResult, ColumnDef, Expr, Iden, InsertStatement, Order, Query,
    SelectStatement, Table, TableCreateStatement,
};

#[derive(Iden)]
pub(super) enum EventType {
    Table,
    EventTypeId,
    EventTypeName,
}

pub enum EventTypeId {
    BlockAdded = 1,
    DeployAccepted = 2,
    DeployExpired = 3,
    DeployProcessed = 4,
    Fault = 5,
    FinalitySignature = 6,
    Step = 7,
}

pub fn create_table_stmt() -> TableCreateStatement {
    Table::create()
        .table(EventType::Table)
        .if_not_exists()
        .col(
            ColumnDef::new(EventType::EventTypeId)
                .integer()
                .not_null()
                .primary_key(),
        )
        .col(
            ColumnDef::new(EventType::EventTypeName)
                .string()
                .not_null()
                .unique_key(),
        )
        .to_owned()
}

pub fn create_initialise_stmt() -> SqResult<InsertStatement> {
    Ok(Query::insert()
        .into_table(EventType::Table)
        .columns([EventType::EventTypeId, EventType::EventTypeName])
        .values(vec![
            (EventTypeId::BlockAdded as u8).into(),
            "BlockAdded".into(),
        ])?
        .values(vec![
            (EventTypeId::DeployAccepted as u8).into(),
            "DeployAccepted".into(),
        ])?
        .values(vec![
            (EventTypeId::DeployExpired as u8).into(),
            "DeployExpired".into(),
        ])?
        .values(vec![
            (EventTypeId::DeployProcessed as u8).into(),
            "DeployProcessed".into(),
        ])?
        .values(vec![(EventTypeId::Fault as u8).into(), "Fault".into()])?
        .values(vec![
            (EventTypeId::FinalitySignature as u8).into(),
            "FinalitySignature".into(),
        ])?
        .values(vec![(EventTypeId::Step as u8).into(), "Step".into()])?
        .to_owned())
}
