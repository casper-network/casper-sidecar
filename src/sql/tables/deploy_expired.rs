use sea_query::{
    error::Result as SqResult, BlobSize, ColumnDef, Expr, ForeignKey, ForeignKeyAction, Iden,
    InsertStatement, Order, Query, SelectStatement, Table, TableCreateStatement,
};

use super::event_log::EventLog;

#[derive(Iden)]
enum DeployExpired {
    #[iden = "DeployExpired"]
    Table,
    DeployHash,
    Raw,
    EventLogId,
}

pub fn create_table_stmt() -> TableCreateStatement {
    Table::create()
        .table(DeployExpired::Table)
        .if_not_exists()
        .col(
            ColumnDef::new(DeployExpired::DeployHash)
                .string()
                .not_null()
                .primary_key(),
        )
        .col(
            ColumnDef::new(DeployExpired::Raw)
                .blob(BlobSize::Tiny)
                .not_null(),
        )
        .col(
            ColumnDef::new(DeployExpired::EventLogId)
                .integer()
                .not_null(),
        )
        .foreign_key(
            ForeignKey::create()
                .name("FK_event_log_id")
                .from(DeployExpired::Table, DeployExpired::EventLogId)
                .to(EventLog::Table, EventLog::EventLogId)
                .on_delete(ForeignKeyAction::Restrict)
                .on_update(ForeignKeyAction::Restrict),
        )
        .to_owned()
}

pub fn create_insert_stmt(
    deploy_hash: String,
    raw: String,
    event_log_id: u64,
) -> SqResult<InsertStatement> {
    Query::insert()
        .into_table(DeployExpired::Table)
        .columns([
            DeployExpired::DeployHash,
            DeployExpired::Raw,
            DeployExpired::EventLogId,
        ])
        .values(vec![deploy_hash.into(), raw.into(), event_log_id.into()])
        .map(|stmt| stmt.to_owned())
}

pub fn create_get_by_hash_stmt(deploy_hash: String) -> SelectStatement {
    Query::select()
        .column(DeployExpired::Raw)
        .from(DeployExpired::Table)
        .and_where(Expr::col(DeployExpired::DeployHash).eq(deploy_hash))
        .to_owned()
}
