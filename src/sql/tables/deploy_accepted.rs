use sea_query::{
    error::Result as SqResult, BlobSize, ColumnDef, Expr, ForeignKey, ForeignKeyAction, Iden,
    InsertStatement, Query, SelectStatement, Table, TableCreateStatement,
};

use super::event_log::EventLog;

#[derive(Iden)]
pub(super) enum DeployAccepted {
    #[iden = "DeployAccepted"]
    Table,
    DeployHash,
    Raw,
    EventLogId,
}

pub fn create_table_stmt() -> TableCreateStatement {
    Table::create()
        .table(DeployAccepted::Table)
        .if_not_exists()
        .col(
            ColumnDef::new(DeployAccepted::DeployHash)
                .string()
                .not_null()
                .primary_key(),
        )
        .col(
            ColumnDef::new(DeployAccepted::Raw)
                .blob(BlobSize::Tiny)
                .not_null(),
        )
        .col(
            ColumnDef::new(DeployAccepted::EventLogId)
                .big_unsigned()
                .not_null(),
        )
        .foreign_key(
            ForeignKey::create()
                .name("FK_event_log_id")
                .from(DeployAccepted::Table, DeployAccepted::EventLogId)
                .to(EventLog::Table, EventLog::EventLogId)
                .on_delete(ForeignKeyAction::Restrict)
                .on_update(ForeignKeyAction::Restrict),
        )
        .to_owned()
}

pub fn create_insert_stmt(
    deploy_hash: String,
    raw: String,
    event_log_id: u32,
) -> SqResult<InsertStatement> {
    Query::insert()
        .into_table(DeployAccepted::Table)
        .columns([
            DeployAccepted::DeployHash,
            DeployAccepted::Raw,
            DeployAccepted::EventLogId,
        ])
        .values(vec![deploy_hash.into(), raw.into(), event_log_id.into()])
        .map(|stmt| stmt.to_owned())
}

pub fn create_get_by_hash_stmt(deploy_hash: String) -> SelectStatement {
    Query::select()
        .column(DeployAccepted::Raw)
        .from(DeployAccepted::Table)
        .and_where(Expr::col(DeployAccepted::DeployHash).eq(deploy_hash))
        .to_owned()
}
