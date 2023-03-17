use chrono::{DateTime, Utc};
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
                .not_null(),
        )
        .col(
            ColumnDef::new(Shutdown::EventLogId)
                .big_unsigned()
                .not_null(),
        )
        .index(
            Index::create()
                .unique()
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
    shutdown_time: DateTime<Utc>,
    event_log_id: u32,
) -> SqResult<InsertStatement> {
    Query::insert()
        .into_table(Shutdown::Table)
        .columns([
            Shutdown::EventSourceAddress,
            Shutdown::ShutdownTimestamp,
            Shutdown::EventLogId,
        ])
        .values(vec![
            event_source_address.into(),
            shutdown_time.into(),
            event_log_id.into(),
        ])
        .map(|stmt| stmt.to_owned())
}

#[test]
fn create_table_stmt_should_produce_create_table_sql() {
    use sea_query::SqliteQueryBuilder;
    let expected_sql = "CREATE TABLE IF NOT EXISTS \"Shutdown\" ( \"event_source_address\" text NOT NULL, \"shutdown_timestamp\" text NOT NULL, \"event_log_id\" integer NOT NULL, CONSTRAINT \"PDX_Shutdown\"PRIMARY KEY (\"event_source_address\", \"shutdown_timestamp\"), FOREIGN KEY (\"event_log_id\") REFERENCES \"event_log\" (\"event_log_id\") ON DELETE RESTRICT ON UPDATE RESTRICT )";

    let got_sql = create_table_stmt().to_string(SqliteQueryBuilder);

    assert_eq!(got_sql, expected_sql);
}

#[test]
fn create_insert_stmt_should_produce_insert_sql() {
    use chrono::NaiveDate;
    use sea_query::SqliteQueryBuilder;
    let expected_sql = "INSERT INTO \"Shutdown\" (\"event_source_address\", \"shutdown_timestamp\", \"event_log_id\") VALUES ('http://100.100.100.1:1782', '2000-01-12 02:00:00 +00:00', 5)";
    let address = "http://100.100.100.1:1782".to_string();
    let naivedatetime_utc = NaiveDate::from_ymd_opt(2000, 1, 12)
        .unwrap()
        .and_hms_opt(2, 0, 0)
        .unwrap();
    let datetime_utc = DateTime::<Utc>::from_utc(naivedatetime_utc, Utc);

    let got_sql = create_insert_stmt(address, datetime_utc, 5)
        .unwrap()
        .to_string(SqliteQueryBuilder);

    assert_eq!(got_sql, expected_sql);
}
