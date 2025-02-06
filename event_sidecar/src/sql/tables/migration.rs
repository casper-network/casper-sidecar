#[cfg(test)]
use sea_query::SqliteQueryBuilder;
use sea_query::{
    error::Result as SqResult, ColumnDef, Iden, Index, InsertStatement, Order, Query,
    SelectStatement, Table, TableCreateStatement,
};
#[derive(Iden)]
enum Migration {
    #[iden = "Migration"]
    Table,
    Version,
    IsSuccess,
}

pub fn create_table_stmt() -> TableCreateStatement {
    Table::create()
        .table(Migration::Table)
        .if_not_exists()
        .col(ColumnDef::new(Migration::Version).integer().not_null())
        .col(ColumnDef::new(Migration::IsSuccess).boolean().not_null())
        .index(
            Index::create()
                .name("PDX_Migration")
                .col(Migration::Version)
                .primary(),
        )
        .to_owned()
}

pub fn create_get_newest_migration_stmt() -> SelectStatement {
    Query::select()
        .column(Migration::Version)
        .column(Migration::IsSuccess)
        .from(Migration::Table)
        .order_by(Migration::Version, Order::Desc)
        .limit(1)
        .to_owned()
}

pub fn create_insert_stmt(version: u32, is_success: bool) -> SqResult<InsertStatement> {
    Query::insert()
        .into_table(Migration::Table)
        .columns([Migration::Version, Migration::IsSuccess])
        .values(vec![version.into(), is_success.into()])
        .map(|stmt| stmt.to_owned())
}

#[test]
fn create_table_stmt_sql() {
    let expected_sql = "CREATE TABLE IF NOT EXISTS \"Migration\" ( \"version\" integer NOT NULL, \"is_success\" boolean NOT NULL, CONSTRAINT \"PDX_Migration\" PRIMARY KEY (\"version\") )";

    let got_sql = create_table_stmt().to_string(SqliteQueryBuilder);

    assert_eq!(got_sql, expected_sql);
}

#[test]
fn create_get_newest_migration_stmt_sql() {
    let expected_sql =
        "SELECT \"version\", \"is_success\" FROM \"Migration\" ORDER BY \"version\" DESC LIMIT 1";

    let got_sql = create_get_newest_migration_stmt().to_string(SqliteQueryBuilder);

    assert_eq!(got_sql, expected_sql);
}

#[test]
fn create_insert_stmt_sql() {
    let expected_sql = "INSERT INTO \"Migration\" (\"version\", \"is_success\") VALUES (561, TRUE)";

    let got_sql = create_insert_stmt(561, true)
        .unwrap()
        .to_string(SqliteQueryBuilder);

    assert_eq!(got_sql, expected_sql);
}
