use sea_query::{
    error::Result as SqResult, ColumnDef, ForeignKey, ForeignKeyAction, Iden, Index,
    InsertStatement, Query, Table, TableCreateStatement,
};

use super::event_log::EventLog;

#[derive(Iden)]
pub(crate) enum Shutdown {
    #[iden = "Shutdown"]
    Table,
    EventSourceAddress,
    ShutdownTimestamp,
    EventLogId,
}

pub fn create_table_stmt() -> TableCreateStatement {
    Table::create()
        .table(Shutdown::Table)
        .if_not_exists()
        .col(
            ColumnDef::new(Shutdown::EventSourceAddress)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(Shutdown::ShutdownTimestamp)
                .date_time()
                .not_null()
                .extra("DEFAULT CURRENT_TIMESTAMP".to_string()),
        )
        .col(
            ColumnDef::new(Shutdown::EventLogId)
                .big_unsigned()
                .not_null(),
        )
        .index(
            Index::create()
                .primary()
                .name("PDX_Shutdown")
                .col(Shutdown::EventSourceAddress)
                .col(Shutdown::ShutdownTimestamp),
        )
        .foreign_key(
            ForeignKey::create()
                .name("FK_event_log_id")
                .from(Shutdown::Table, Shutdown::EventLogId)
                .to(EventLog::Table, EventLog::EventLogId)
                .on_delete(ForeignKeyAction::Restrict)
                .on_update(ForeignKeyAction::Restrict),
        )
        .to_owned()
}

pub fn create_insert_stmt(
    event_source_address: String,
    event_log_id: u32,
) -> SqResult<InsertStatement> {
    Query::insert()
        .into_table(Shutdown::Table)
        .columns([Shutdown::EventSourceAddress, Shutdown::EventLogId])
        .values(vec![event_source_address.into(), event_log_id.into()])
        .map(|stmt| stmt.to_owned())
}

#[test]
fn create_table_stmt_should_produce_create_table_sql() {
    use sea_query::SqliteQueryBuilder;
    let expected_sql = "CREATE TABLE IF NOT EXISTS \"Shutdown\" ( \"event_source_address\" text NOT NULL, \"shutdown_timestamp\" text NOT NULL DEFAULT CURRENT_TIMESTAMP, \"event_log_id\" bigint NOT NULL, CONSTRAINT \"PDX_Shutdown\" PRIMARY KEY (\"event_source_address\", \"shutdown_timestamp\"), FOREIGN KEY (\"event_log_id\") REFERENCES \"event_log\" (\"event_log_id\") ON DELETE RESTRICT ON UPDATE RESTRICT )";

    let got_sql = create_table_stmt().to_string(SqliteQueryBuilder);

    assert_eq!(got_sql, expected_sql);
}

#[test]
fn create_insert_stmt_should_produce_insert_sql() {
    use sea_query::SqliteQueryBuilder;
    let expected_sql = "INSERT INTO \"Shutdown\" (\"event_source_address\", \"event_log_id\") VALUES ('http://100.100.100.1:1782', 5)";
    let address = "http://100.100.100.1:1782".to_string();

    let got_sql = create_insert_stmt(address, 5)
        .unwrap()
        .to_string(SqliteQueryBuilder);

    assert_eq!(got_sql, expected_sql);
}
