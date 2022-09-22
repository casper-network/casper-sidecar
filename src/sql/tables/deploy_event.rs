use sea_query::{
    error::Result as SqResult, ColumnDef, ForeignKey, ForeignKeyAction, Iden, InsertStatement,
    Order, Query, SelectStatement, Table, TableCreateStatement,
};

use super::event_log::EventLog;

#[derive(Iden)]
pub(super) enum DeployEvent {
    Table,
    EventLogId,
    DeployHash,
}

pub fn create_table_stmt() -> TableCreateStatement {
    Table::create()
        .table(DeployEvent::Table)
        .if_not_exists()
        .col(ColumnDef::new(DeployEvent::EventLogId).integer().not_null())
        .col(ColumnDef::new(DeployEvent::DeployHash).string().not_null())
        .foreign_key(
            ForeignKey::create()
                .name("FK_event_log_id")
                .from(DeployEvent::Table, DeployEvent::EventLogId)
                .to(EventLog::Table, EventLog::EventLogId)
                .on_delete(ForeignKeyAction::Restrict)
                .on_update(ForeignKeyAction::Restrict),
        )
        .to_owned()
}

pub fn create_insert_stmt(event_log_id: u64, deploy_hash: String) -> SqResult<InsertStatement> {
    let insert_stmt = Query::insert()
        .into_table(DeployEvent::Table)
        .columns([DeployEvent::EventLogId, DeployEvent::DeployHash])
        .values(vec![event_log_id.into(), deploy_hash.into()])?
        .to_owned();

    Ok(insert_stmt)
}

pub fn create_get_latest_deploy_hash() -> SelectStatement {
    Query::select()
        .column(DeployEvent::DeployHash)
        .from(DeployEvent::Table)
        .order_by(DeployEvent::EventLogId, Order::Asc)
        .limit(1)
        .to_owned()
}
