use sea_query::{
    error::Result as SqResult, ColumnDef, Expr, ForeignKey, ForeignKeyAction, Iden, Index,
    InsertStatement, Query, SelectStatement, Table, TableCreateStatement,
};

use super::event_log::EventLog;

#[derive(Iden)]
enum Step {
    #[iden = "Step"]
    Table,
    Era,
    Raw,
    EventLogId,
}

pub fn create_table_stmt() -> TableCreateStatement {
    Table::create()
        .table(Step::Table)
        .if_not_exists()
        .col(ColumnDef::new(Step::Era).big_unsigned().not_null())
        .col(ColumnDef::new(Step::Raw).text().not_null())
        .col(ColumnDef::new(Step::EventLogId).big_unsigned().not_null())
        .index(Index::create().primary().name("PDX_Step").col(Step::Era))
        .foreign_key(
            ForeignKey::create()
                .name("FK_event_log_id")
                .from(Step::Table, Step::EventLogId)
                .to(EventLog::Table, EventLog::EventLogId)
                .on_delete(ForeignKeyAction::Restrict)
                .on_update(ForeignKeyAction::Restrict),
        )
        .to_owned()
}

pub fn create_insert_stmt(era: u64, raw: String, event_log_id: u32) -> SqResult<InsertStatement> {
    Query::insert()
        .into_table(Step::Table)
        .columns([Step::Era, Step::Raw, Step::EventLogId])
        .values(vec![era.into(), raw.into(), event_log_id.into()])
        .map(|stmt| stmt.to_owned())
}

pub fn create_get_by_era_stmt(era: u64) -> SelectStatement {
    Query::select()
        .column(Step::Raw)
        .from(Step::Table)
        .and_where(Expr::col(Step::Era).eq(era))
        .to_owned()
}
