use sea_query::{
    error::Result as SqResult, ColumnDef, Expr, Iden, Index, IndexCreateStatement, InsertStatement,
    Query, SelectStatement, Table, TableCreateStatement,
};

#[derive(Iden)]
pub(super) enum BlockDeploys {
    #[iden = "BlockDeploys"]
    Table,
    DeployHash,
    BlockHash,
    BlockTimestampUtcEpochMillis,
}

pub fn create_table_stmt() -> TableCreateStatement {
    Table::create()
        .table(BlockDeploys::Table)
        .if_not_exists()
        .col(ColumnDef::new(BlockDeploys::DeployHash).string().not_null())
        .col(ColumnDef::new(BlockDeploys::BlockHash).string().not_null())
        .col(
            ColumnDef::new(BlockDeploys::BlockTimestampUtcEpochMillis)
                .integer_len(13)
                .not_null(),
        )
        .index(
            Index::create()
                .unique()
                .primary()
                .name("PK_BlockDeploys")
                .col(BlockDeploys::BlockHash)
                .col(BlockDeploys::DeployHash),
        )
        .to_owned()
}

pub fn create_block_deploys_deploy_hash_index() -> IndexCreateStatement {
    Index::create()
        .if_not_exists()
        .name("IDX_BlockDeploys_DeployHash")
        .table(BlockDeploys::Table)
        .col(BlockDeploys::DeployHash)
        .to_owned()
}

pub fn create_get_by_deploy_hash_stmt(deploy_hash: String) -> SelectStatement {
    Query::select()
        .column(BlockDeploys::BlockHash)
        .column(BlockDeploys::DeployHash)
        .column(BlockDeploys::BlockTimestampUtcEpochMillis)
        .from(BlockDeploys::Table)
        .and_where(Expr::col(BlockDeploys::DeployHash).eq(deploy_hash))
        .to_owned()
}

pub fn create_block_deploys_block_hash_index() -> IndexCreateStatement {
    Index::create()
        .if_not_exists()
        .name("IDX_BlockDeploys_BlockHash")
        .table(BlockDeploys::Table)
        .col(BlockDeploys::BlockHash)
        .to_owned()
}

pub fn create_insert_stmt(
    deploy_hashes: Vec<String>,
    block_hash: String,
    block_timestamp: u64,
) -> SqResult<InsertStatement> {
    let mut query = Query::insert();
    let builder = query.into_table(BlockDeploys::Table).columns([
        BlockDeploys::DeployHash,
        BlockDeploys::BlockHash,
        BlockDeploys::BlockTimestampUtcEpochMillis,
    ]);
    for deploy_hash in deploy_hashes.iter() {
        builder.values(vec![
            deploy_hash.to_owned().into(),
            block_hash.clone().into(),
            block_timestamp.into(),
        ])?;
    }
    Ok(builder.to_owned())
}

#[test]
fn create_table_stmt_should_produce_ddl_sql() {
    use sea_query::SqliteQueryBuilder;
    let expected_sql = "CREATE TABLE IF NOT EXISTS \"BlockDeploys\" ( \"deploy_hash\" text NOT NULL, \"block_hash\" text NOT NULL, \"block_timestamp_utc_epoch_millis\" integer(13) NOT NULL, CONSTRAINT \"PK_BlockDeploys\"PRIMARY KEY (\"block_hash\", \"deploy_hash\") )";

    let got_sql = create_table_stmt().to_string(SqliteQueryBuilder);

    assert_eq!(got_sql, expected_sql);
}

#[test]
fn create_block_deploys_deploy_hash_index_should_produce_ddl_sql() {
    use sea_query::SqliteQueryBuilder;
    let expected_sql = "CREATE INDEX IF NOT EXISTS \"IDX_BlockDeploys_DeployHash\" ON \"BlockDeploys\" (\"deploy_hash\")";

    let got_sql = create_block_deploys_deploy_hash_index().to_string(SqliteQueryBuilder);

    assert_eq!(got_sql, expected_sql);
}

#[test]
fn create_block_deploys_block_hash_index_should_produce_ddl_sql() {
    use sea_query::SqliteQueryBuilder;
    let expected_sql = "CREATE INDEX IF NOT EXISTS \"IDX_BlockDeploys_BlockHash\" ON \"BlockDeploys\" (\"block_hash\")";

    let got_sql = create_block_deploys_block_hash_index().to_string(SqliteQueryBuilder);

    assert_eq!(got_sql, expected_sql);
}

#[test]
fn create_insert_stmt_given_list_of_hashes_should_produce_insert_statement() {
    use sea_query::SqliteQueryBuilder;
    let expected_sql = "INSERT INTO \"BlockDeploys\" (\"deploy_hash\", \"block_hash\", \"block_timestamp_utc_epoch_millis\") VALUES ('X', 'bh-1', 1674038195060), ('Y', 'bh-1', 1674038195060), ('Z', 'bh-1', 1674038195060)";
    let deploy_hashes = vec![String::from("X"), String::from("Y"), String::from("Z")];
    let block_hash = String::from("bh-1");
    let timestamp_in_millis = 1674038195060;
    let got_sql = create_insert_stmt(deploy_hashes, block_hash, timestamp_in_millis)
        .unwrap()
        .to_string(SqliteQueryBuilder);

    assert_eq!(got_sql, expected_sql);
}
