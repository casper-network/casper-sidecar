use sea_query::{
    error::Result as SqResult, ColumnDef, Expr, ForeignKey, ForeignKeyAction, Iden, Index,
    InsertStatement, Query, SelectStatement, Table, TableCreateStatement,
};

use super::event_log::EventLog;

#[derive(Iden)]
enum Fault {
    #[iden = "Fault"]
    Table,
    Era,
    PublicKey,
    Raw,
    EventLogId,
}

pub fn create_table_stmt(db_supports_unsigned: bool) -> TableCreateStatement {
    let mut binding = ColumnDef::new(Fault::Era);
    let mut era_col_definition = binding.not_null();
    if db_supports_unsigned {
        era_col_definition = era_col_definition.big_unsigned();
    } else {
        era_col_definition = era_col_definition.decimal_len(20, 0);
    }
    Table::create()
        .table(Fault::Table)
        .if_not_exists()
        .col(era_col_definition)
        .col(ColumnDef::new(Fault::PublicKey).string().not_null())
        .col(ColumnDef::new(Fault::Raw).text().not_null())
        .col(ColumnDef::new(Fault::EventLogId).big_unsigned().not_null())
        .index(
            Index::create()
                .primary()
                .name("PDX_Fault")
                .col(Fault::Era)
                .col(Fault::PublicKey),
        )
        .foreign_key(
            ForeignKey::create()
                .name("FK_event_log_id")
                .from(Fault::Table, Fault::EventLogId)
                .to(EventLog::Table, EventLog::EventLogId)
                .on_delete(ForeignKeyAction::Restrict)
                .on_update(ForeignKeyAction::Restrict),
        )
        .to_owned()
}

pub fn create_insert_stmt(
    era: u64,
    public_key: String,
    raw: String,
    event_log_id: u64,
) -> SqResult<InsertStatement> {
    Query::insert()
        .into_table(Fault::Table)
        .columns([Fault::Era, Fault::PublicKey, Fault::Raw, Fault::EventLogId])
        .values(vec![
            era.into(),
            public_key.into(),
            raw.into(),
            event_log_id.into(),
        ])
        .map(|stmt| stmt.to_owned())
}

pub fn create_get_faults_by_public_key_stmt(public_key: String) -> SelectStatement {
    Query::select()
        .column(Fault::Raw)
        .column(EventLog::ApiVersion)
        .column(EventLog::NetworkName)
        .from(Fault::Table)
        .left_join(
            EventLog::Table,
            Expr::col((EventLog::Table, EventLog::EventLogId))
                .equals((Fault::Table, Fault::EventLogId)),
        )
        .and_where(Expr::col(Fault::PublicKey).eq(public_key))
        .to_owned()
}

pub fn create_get_faults_by_era_stmt(era: u64) -> SelectStatement {
    Query::select()
        .column(Fault::Raw)
        .column(EventLog::ApiVersion)
        .column(EventLog::NetworkName)
        .from(Fault::Table)
        .left_join(
            EventLog::Table,
            Expr::col((EventLog::Table, EventLog::EventLogId))
                .equals((Fault::Table, Fault::EventLogId)),
        )
        .and_where(Expr::col(Fault::Era).eq(era))
        .to_owned()
}
