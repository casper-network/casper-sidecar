use sea_query::{
    error::Result as SqResult, BlobSize, ColumnDef, Expr, ForeignKey, ForeignKeyAction, Iden,
    InsertStatement, Order, Query, SelectStatement, Table, TableCreateStatement,
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

pub fn create_table_stmt() -> TableCreateStatement {
    Table::create()
        .table(Fault::Table)
        .if_not_exists()
        .col(ColumnDef::new(Fault::Era).integer().not_null())
        .col(ColumnDef::new(Fault::PublicKey).string().not_null())
        .col(ColumnDef::new(Fault::Raw).blob(BlobSize::Tiny).not_null())
        .col(ColumnDef::new(Fault::EventLogId).integer().not_null())
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
        .from(Fault::Table)
        .and_where(Expr::col(Fault::PublicKey).eq(public_key))
        .to_owned()
}

pub fn create_get_faults_by_era_stmt(era: u64) -> SelectStatement {
    Query::select()
        .column(Fault::Raw)
        .from(Fault::Table)
        .and_where(Expr::col(Fault::Era).eq(era))
        .to_owned()
}
