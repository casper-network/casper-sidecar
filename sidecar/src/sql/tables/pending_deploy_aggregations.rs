#[cfg(test)]
use sea_query::SqliteQueryBuilder;
use sea_query::{error::Result as SqResult, ColumnDef, Iden, Query, Table, TableCreateStatement};
use sea_query::{Alias, DeleteStatement, Expr, InsertStatement, SelectStatement};
use serde::Deserialize;
use sqlx::FromRow;

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

#[derive(Debug, Deserialize, FromRow)]
pub struct PendingDeployAggregationEntity {
    id: u32,
    deploy_hash: String,
    block_hash: Option<String>,
    block_timestamp: Option<i64>,
}

impl PendingDeployAggregationEntity {
    pub fn get_id(&self) -> u32 {
        self.id
    }

    pub fn deploy_hash(&self) -> String {
        self.deploy_hash.clone()
    }

    pub fn get_block_data(&self) -> Option<(String, u64)> {
        match (&self.block_hash, self.block_timestamp) {
            (Some(hash), Some(timestamp)) => Some((hash.clone(), timestamp as u64)),
            _ => None,
        }
    }
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

pub fn select_stmt(number_to_fetch: u32) -> SelectStatement {
    Query::select()
        .expr_as(
            Expr::col((
                PendingDeployAggregations::Table,
                PendingDeployAggregations::Id,
            )),
            Alias::new("id"),
        )
        .expr_as(
            Expr::col((
                PendingDeployAggregations::Table,
                PendingDeployAggregations::DeployHash,
            )),
            Alias::new("deploy_hash"),
        )
        .expr_as(
            Expr::col((
                PendingDeployAggregations::Table,
                PendingDeployAggregations::BlockHash,
            )),
            Alias::new("block_hash"),
        )
        .expr_as(
            Expr::col((
                PendingDeployAggregations::Table,
                PendingDeployAggregations::BlockTimestamp,
            )),
            Alias::new("block_timestamp"),
        )
        .from(PendingDeployAggregations::Table)
        //We're ordering by deploy hash so the probability of assemble command regarding one deploy hash end up in one select is higher
        .order_by(PendingDeployAggregations::DeployHash, sea_query::Order::Asc)
        .limit(number_to_fetch as u64)
        .to_owned()
}

pub fn delete_stmt(ids: Vec<u32>) -> DeleteStatement {
    Query::delete()
        .from_table(PendingDeployAggregations::Table)
        .cond_where(Expr::col(PendingDeployAggregations::Id).is_in(ids))
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

#[test]
pub fn select_oldest_stmt_test() {
    let sql = select_stmt(512).to_string(SqliteQueryBuilder);
    assert_eq!(
        sql,
        "SELECT \"PendingDeployAggregations\".\"id\" AS \"id\", \"PendingDeployAggregations\".\"deploy_hash\" AS \"deploy_hash\", \"PendingDeployAggregations\".\"block_hash\" AS \"block_hash\", \"PendingDeployAggregations\".\"block_timestamp\" AS \"block_timestamp\" FROM \"PendingDeployAggregations\" ORDER BY \"deploy_hash\" ASC LIMIT 512"
    )
}

#[test]
pub fn delete_stmt_test() {
    let sql = delete_stmt(vec![1, 5, 15]).to_string(SqliteQueryBuilder);
    assert_eq!(
        sql,
        "DELETE FROM \"PendingDeployAggregations\" WHERE \"id\" IN (1, 5, 15)"
    )
}
