use sea_query::{
    error::Result as SqResult, ColumnDef, Iden, Index, Query, Table, TableCreateStatement,
};

use sea_query::{
    Alias, DeleteStatement, Expr, IndexCreateStatement, InsertStatement, SelectStatement,
};
use serde::Deserialize;
use sqlx::FromRow;

#[cfg(test)]
use sea_query::SqliteQueryBuilder;

use super::deploy_accepted::DeployAccepted;

#[derive(Iden, Clone)]
pub(super) enum AssembleDeployAggregate {
    #[iden = "AssembleDeployAggregate"]
    Table,
    Id,
    DeployHash,
    BlockHash,
    BlockTimestamp,
    CreatedAt,
}

#[derive(Debug, Deserialize, FromRow)]
pub struct AssembleDeployAggregateEntity {
    id: u32,
    deploy_hash: String,
    block_hash: Option<String>,
    block_timestamp: Option<i64>,
}

impl AssembleDeployAggregateEntity {
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
    let mut cols = vec![AssembleDeployAggregate::DeployHash];
    let mut vals = vec![deploy_hash.into()];
    match maybe_block_data {
        Some((block_hash, timestamp)) => {
            cols.push(AssembleDeployAggregate::BlockHash);
            cols.push(AssembleDeployAggregate::BlockTimestamp);
            vals.push(block_hash.into());
            vals.push(timestamp.into());
        }
        _ => {}
    }
    Ok(Query::insert()
        .into_table(AssembleDeployAggregate::Table)
        .columns(cols)
        .values(vals)?
        .to_owned())
}

/// Creates insert statement for a command to assemble aggregate DeployAggregate.
/// The maybe_block_data is a option tuple of blocks deploy hash and its creation timestamp.
pub fn create_multi_insert_stmt(
    //vector of (deploy_hash, (Option(block_hash, block_timestamp)))
    data: Vec<(String, Option<(String, u64)>)>,
) -> SqResult<InsertStatement> {
    let cols = vec![AssembleDeployAggregate::DeployHash, AssembleDeployAggregate::BlockHash, AssembleDeployAggregate::BlockTimestamp];
    let mut builder = Query::insert()
        .into_table(AssembleDeployAggregate::Table)
        .columns(cols)
        .to_owned();
    for (deploy_hash, maybe_block_data) in data {
        match maybe_block_data {
            Some((block_hash, timestamp)) => {
                builder.values(vec![deploy_hash.into(), block_hash.into(), timestamp.into()])?;
            }
            _ => {
                builder.values(vec![deploy_hash.into(), None::<String>.into(), None::<u64>.into()])?;
            }
        }
    }
    
    Ok(builder.to_owned())
}

pub fn create_insert_from_deploy_accepted() -> SqResult<InsertStatement> {
    let sub_select_deploy_hashes_deploy_accepted = Query::select()
        .expr(Expr::col(DeployAccepted::DeployHash))
        .from(DeployAccepted::Table)
        .to_owned();
    let builder = Query::insert()
        .into_table(AssembleDeployAggregate::Table)
        .columns(vec![AssembleDeployAggregate::DeployHash])
        .select_from(sub_select_deploy_hashes_deploy_accepted)?
        .to_owned();
    Ok(builder)
}

pub fn create_table_stmt() -> TableCreateStatement {
    Table::create()
        .table(AssembleDeployAggregate::Table)
        .if_not_exists()
        .col(
            ColumnDef::new(AssembleDeployAggregate::Id)
                .big_unsigned()
                .auto_increment()
                .not_null()
                .primary_key(),
        )
        .col(
            ColumnDef::new(AssembleDeployAggregate::DeployHash)
                .not_null()
                .string(),
        )
        .col(ColumnDef::new(AssembleDeployAggregate::BlockHash).string())
        .col(ColumnDef::new(AssembleDeployAggregate::BlockTimestamp).integer_len(13))
        .col(
            ColumnDef::new(AssembleDeployAggregate::CreatedAt)
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
            Expr::col((AssembleDeployAggregate::Table, AssembleDeployAggregate::Id)),
            Alias::new("id"),
        )
        .expr_as(
            Expr::col((
                AssembleDeployAggregate::Table,
                AssembleDeployAggregate::DeployHash,
            )),
            Alias::new("deploy_hash"),
        )
        .expr_as(
            Expr::col((
                AssembleDeployAggregate::Table,
                AssembleDeployAggregate::BlockHash,
            )),
            Alias::new("block_hash"),
        )
        .expr_as(
            Expr::col((
                AssembleDeployAggregate::Table,
                AssembleDeployAggregate::BlockTimestamp,
            )),
            Alias::new("block_timestamp"),
        )
        .from(AssembleDeployAggregate::Table)
        //We're ordering by deploy hash so the probability of assemble command regarding one deploy hash end up in one select is higher
        .order_by(AssembleDeployAggregate::DeployHash, sea_query::Order::Asc)
        .limit(number_to_fetch as u64)
        .to_owned()
}

pub fn delete_stmt(ids: Vec<u32>) -> DeleteStatement {
    Query::delete()
        .from_table(AssembleDeployAggregate::Table)
        .cond_where(Expr::col(AssembleDeployAggregate::Id).is_in(ids))
        .to_owned()
}


#[test]
pub fn z_test() {
println!("{}", create_table_stmt().to_string(SqliteQueryBuilder))
}

#[test]
pub fn select_oldest_stmt_test() {
    let sql = select_stmt(512).to_string(SqliteQueryBuilder);
    assert_eq!(
        sql,
        "SELECT \"AssembleDeployAggregate\".\"id\" AS \"id\", \"AssembleDeployAggregate\".\"deploy_hash\" AS \"deploy_hash\", \"AssembleDeployAggregate\".\"block_hash\" AS \"block_hash\", \"AssembleDeployAggregate\".\"block_timestamp\" AS \"block_timestamp\" FROM \"AssembleDeployAggregate\" ORDER BY \"id\" ASC LIMIT 512"
    )
}

#[test]
pub fn delete_stmt_test() {
    let sql = delete_stmt(vec![1, 5, 15]).to_string(SqliteQueryBuilder);
    assert_eq!(
        sql,
        "DELETE FROM \"AssembleDeployAggregate\" WHERE \"id\" IN (1, 5, 15)"
    )
}

#[test]
pub fn create_insert_stmt_test() {
    let sql = create_insert_stmt("abc".to_string(), Some(("block_hash_1".to_string(), 555)))
        .unwrap()
        .to_string(SqliteQueryBuilder);
    assert_eq!(
        sql,
        "INSERT INTO \"AssembleDeployAggregate\" (\"deploy_hash\", \"block_hash\", \"block_timestamp\") VALUES ('abc', 'block_hash_1', 555)"
    )
}

#[test]
pub fn create_insert_stmt_without_block_data_test() {
    let sql = create_insert_stmt("abc".to_string(), None)
        .unwrap()
        .to_string(SqliteQueryBuilder);
    assert_eq!(
        sql,
        "INSERT INTO \"AssembleDeployAggregate\" (\"deploy_hash\") VALUES ('abc')"
    )
}
