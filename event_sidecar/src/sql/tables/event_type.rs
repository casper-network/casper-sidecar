use sea_query::{
    error::Result as SqResult, ColumnDef, Iden, InsertStatement, OnConflict, Query, Table,
    TableCreateStatement,
};

#[allow(clippy::enum_variant_names)]
#[derive(Iden)]
pub(super) enum EventType {
    Table,
    EventTypeId,
    EventTypeName,
}

pub enum EventTypeId {
    BlockAdded = 1,
    TransactionAccepted = 2,
    TransactionExpired = 3,
    TransactionProcessed = 4,
    Fault = 5,
    FinalitySignature = 6,
    Step = 7,
    Shutdown = 8,
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
            (EventTypeId::TransactionAccepted as u8).into(),
            "TransactionAccepted".into(),
        ])?
        .values(vec![
            (EventTypeId::TransactionExpired as u8).into(),
            "TransactionExpired".into(),
        ])?
        .values(vec![
            (EventTypeId::TransactionProcessed as u8).into(),
            "TransactionProcessed".into(),
        ])?
        .values(vec![(EventTypeId::Fault as u8).into(), "Fault".into()])?
        .values(vec![
            (EventTypeId::FinalitySignature as u8).into(),
            "FinalitySignature".into(),
        ])?
        .values(vec![(EventTypeId::Step as u8).into(), "Step".into()])?
        .values(vec![
            (EventTypeId::Shutdown as u8).into(),
            "Shutdown".into(),
        ])?
        .on_conflict(
            OnConflict::column(EventType::EventTypeId)
                .do_nothing()
                .to_owned(),
        )
        .to_owned())
}

#[test]
fn create_initialise_stmt_sql() {
    use sea_query::SqliteQueryBuilder;
    let expected_sql = "INSERT INTO \"event_type\" (\"event_type_id\", \"event_type_name\") VALUES (1, 'BlockAdded'), (2, 'TransactionAccepted'), (3, 'TransactionExpired'), (4, 'TransactionProcessed'), (5, 'Fault'), (6, 'FinalitySignature'), (7, 'Step'), (8, 'Shutdown') ON CONFLICT (\"event_type_id\") DO NOTHING";

    let got_sql = create_initialise_stmt()
        .unwrap()
        .to_string(SqliteQueryBuilder);

    assert_eq!(got_sql, expected_sql);
}
