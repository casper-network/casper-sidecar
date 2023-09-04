use sea_query::{
    error::Result as SqResult, ColumnDef, ForeignKey, ForeignKeyAction, Iden, Index,
    InsertStatement, Query, Table, TableCreateStatement,
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
        .col(
            ColumnDef::new(DeployEvent::EventLogId)
                .big_unsigned()
                .not_null(),
        )
        .col(ColumnDef::new(DeployEvent::DeployHash).string().not_null())
        .index(
            Index::create()
                .primary()
                .name("PDX_DeployEvent")
                .col(DeployEvent::DeployHash)
                .col(DeployEvent::EventLogId),
        )
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

pub fn create_insert_stmt(event_log_id: u32, deploy_hash: String) -> SqResult<InsertStatement> {
    let insert_stmt = Query::insert()
        .into_table(DeployEvent::Table)
        .columns([DeployEvent::EventLogId, DeployEvent::DeployHash])
        .values(vec![event_log_id.into(), deploy_hash.into()])?
        .to_owned();

    Ok(insert_stmt)
}
