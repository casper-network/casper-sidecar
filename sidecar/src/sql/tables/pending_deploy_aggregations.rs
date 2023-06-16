use sea_query::InsertStatement;
#[cfg(test)]
use sea_query::SqliteQueryBuilder;
use sea_query::{error::Result as SqResult, ColumnDef, Iden, Query, Table, TableCreateStatement};

#[derive(Iden, Clone)]
pub(super) enum PendingDeployAggregations {
    #[iden = "PendingDeployAggregations"]
    Table,
    Id,
    DeployHash,
    BlockHash,
    BlockTimestamp,
    CreatedAt,
}

/// Creates insert statement for a command to assemble aggregate DeployAggregate.
/// The maybe_block_data is a option tuple of blocks deploy hash and its creation timestamp.
pub fn create_insert_stmt(
    deploy_hash: String,
    maybe_block_data: Option<(String, u64)>,
) -> SqResult<InsertStatement> {
    let mut cols = vec![PendingDeployAggregations::DeployHash];
    let mut vals = vec![deploy_hash.into()];
    if let Some((block_hash, timestamp)) = maybe_block_data {
        cols.push(PendingDeployAggregations::BlockHash);
        cols.push(PendingDeployAggregations::BlockTimestamp);
        vals.push(block_hash.into());
        vals.push(timestamp.into());
    }
    Ok(Query::insert()
        .into_table(PendingDeployAggregations::Table)
        .columns(cols)
        .values(vals)?
        .to_owned())
}

pub fn create_table_stmt() -> TableCreateStatement {
    Table::create()
        .table(PendingDeployAggregations::Table)
        .if_not_exists()
        .col(
            ColumnDef::new(PendingDeployAggregations::Id)
                .big_unsigned()
                .auto_increment()
                .not_null()
                .primary_key(),
        )
        .col(
            ColumnDef::new(PendingDeployAggregations::DeployHash)
                .not_null()
                .string(),
        )
        .col(ColumnDef::new(PendingDeployAggregations::BlockHash).string())
        .col(ColumnDef::new(PendingDeployAggregations::BlockTimestamp).integer_len(13))
        .col(
            ColumnDef::new(PendingDeployAggregations::CreatedAt)
                .timestamp()
                .not_null()
                // This can be replaced with better syntax when https://github.com/SeaQL/sea-query/pull/428 merges.
                .extra("DEFAULT CURRENT_TIMESTAMP".to_string()),
        )
        .to_owned()
}

#[test]
pub fn create_insert_stmt_test() {
    let sql = create_insert_stmt("abc".to_string(), Some(("block_hash_1".to_string(), 555)))
        .unwrap()
        .to_string(SqliteQueryBuilder);
    assert_eq!(
        sql,
        "INSERT INTO \"PendingDeployAggregations\" (\"deploy_hash\", \"block_hash\", \"block_timestamp\") VALUES ('abc', 'block_hash_1', 555)"
    )
}

#[test]
pub fn create_insert_stmt_without_block_data_test() {
    let sql = create_insert_stmt("abc".to_string(), None)
        .unwrap()
        .to_string(SqliteQueryBuilder);
    assert_eq!(
        sql,
        "INSERT INTO \"PendingDeployAggregations\" (\"deploy_hash\") VALUES ('abc')"
    )
}
