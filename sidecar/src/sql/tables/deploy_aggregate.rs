use sea_query::{
    error::Result as SqResult, Alias, BlobSize, ColumnDef, Iden, Index, NullOrdering,
    OrderedStatement, Query, SelectStatement, Table, TableCreateStatement,
};

use super::block_deploys::BlockDeploys;
use crate::sql::tables::deploy_accepted::DeployAccepted;
use crate::sql::tables::deploy_expired::DeployExpired;
use crate::sql::tables::deploy_processed::DeployProcessed;
use crate::types::database::{DeployAggregateFilter, DeployAggregateSortColumn, SortOrder};
use sea_query::{Cond, Expr, InsertStatement, JoinType, OnConflict, Order, UpdateStatement};

pub fn create_list_by_filter_query(filter: DeployAggregateFilter) -> SelectStatement {
    let offset = filter.offset;
    let limit = filter.limit;
    let sort_column = filter.sort_column;
    let sort_order = filter.sort_order;
    decorate_with_order_by(
        sort_column,
        sort_order,
        decorate_with_joins(
            filter.exclude_expired,
            filter.exclude_not_processed,
            Query::select()
                .expr_as(
                    Expr::col((DeployAccepted::Table, DeployAccepted::DeployHash)),
                    Alias::new("deploy_hash"),
                )
                .expr_as(
                    Expr::col((DeployAccepted::Table, DeployAccepted::Raw)),
                    Alias::new("deploy_accepted_raw"),
                )
                .expr_as(
                    Expr::col((DeployProcessed::Table, DeployProcessed::Raw)),
                    Alias::new("deploy_processed_raw"),
                )
                .expr_as(
                    Expr::col((DeployExpired::Table, DeployExpired::DeployHash)).is_not_null(),
                    Alias::new("is_expired"),
                )
                .expr_as(
                    Expr::col((
                        BlockDeploys::Table,
                        BlockDeploys::BlockTimestampUtcEpochMillis,
                    )),
                    Alias::new("block_timestamp"),
                ),
        ),
    )
    .offset(u64::from(offset))
    .limit(u64::from(limit))
    .to_owned()
}

pub fn create_count_aggregate_deploys_query(filter: DeployAggregateFilter) -> SelectStatement {
    decorate_with_joins(
        filter.exclude_expired,
        filter.exclude_not_processed,
        Query::select().expr_as(
            Expr::col((DeployAccepted::Table, DeployAccepted::DeployHash)).count(),
            Alias::new("count"),
        ),
    )
    .to_owned()
}

fn decorate_with_order_by(
    sort_column: Option<DeployAggregateSortColumn>,
    sort_order: Option<SortOrder>,
    select: &mut SelectStatement,
) -> &mut SelectStatement {
    match sort_column {
        Some(column) => {
            let sort_order = sort_order
                .map(|s| match s {
                    SortOrder::Desc => Order::Desc,
                    SortOrder::Asc => Order::Asc,
                })
                .unwrap_or_else(|| Order::Desc);
            let sort_column = match column {
                DeployAggregateSortColumn::BlockTimestamp => (
                    BlockDeploys::Table,
                    BlockDeploys::BlockTimestampUtcEpochMillis,
                ),
            };
            select.order_by_with_nulls(sort_column, sort_order, NullOrdering::Last)
        }
        None => select,
    }
}

fn decorate_with_joins(
    exclude_expired: bool,
    exclude_not_processed: bool,
    select: &mut SelectStatement,
) -> &mut SelectStatement {
    let processed_table = Expr::tbl(DeployProcessed::Table, DeployProcessed::DeployHash)
        .equals(BlockDeploys::Table, BlockDeploys::DeployHash);
    let expired_table = Expr::tbl(DeployExpired::Table, DeployExpired::DeployHash)
        .equals(BlockDeploys::Table, BlockDeploys::DeployHash);
    let accepted_table = Expr::tbl(DeployAccepted::Table, DeployAccepted::DeployHash)
        .equals(BlockDeploys::Table, BlockDeploys::DeployHash);
    let mut conditions = Cond::all();
    conditions =
        conditions.add(Expr::tbl(DeployAccepted::Table, DeployAccepted::DeployHash).is_not_null());
    if exclude_expired {
        conditions =
            conditions.add(Expr::tbl(DeployExpired::Table, DeployExpired::DeployHash).is_null())
    }
    if exclude_not_processed {
        conditions = conditions
            .add(Expr::tbl(DeployProcessed::Table, DeployProcessed::DeployHash).is_not_null())
    }
    select
        .from(BlockDeploys::Table)
        .join(JoinType::LeftJoin, DeployAccepted::Table, accepted_table)
        .join(JoinType::LeftJoin, DeployProcessed::Table, processed_table)
        .join(JoinType::LeftJoin, DeployExpired::Table, expired_table)
        .cond_where(conditions)
}

#[derive(Iden)]
pub(super) enum DeployAggregate {
    #[iden = "DeployAggregate"]
    Table,
    DeployHash,
    DeployAcceptedRaw,
    DeployProcessedRaw,
    DeployExpiredRaw,
    BlockHash,
    BlockTimestampUtcEpochMillis,
}

pub fn create_table_stmt() -> TableCreateStatement {
    Table::create()
        .table(DeployAggregate::Table)
        .if_not_exists()
        .col(
            ColumnDef::new(DeployAggregate::DeployHash)
                .string()
                .not_null(),
        )
        .col(
            ColumnDef::new(DeployAggregate::DeployAcceptedRaw)
                .not_null()
                .blob(BlobSize::Tiny),
        )
        .col(ColumnDef::new(DeployAggregate::DeployProcessedRaw).blob(BlobSize::Tiny))
        .col(ColumnDef::new(DeployAggregate::DeployExpiredRaw).blob(BlobSize::Tiny))
        .col(ColumnDef::new(DeployAggregate::BlockHash).string())
        .col(
            ColumnDef::new(DeployAggregate::BlockTimestampUtcEpochMillis)
                .integer_len(13)
                .not_null(),
        )
        .index(
            Index::create()
                .unique()
                .primary()
                .name("PDX_DeployAggregate")
                .col(DeployAggregate::DeployHash),
        )
        .index(
            Index::create()
                .if_not_exists()
                .name("IDX_DeployAggregate_BlockTimestamp")
                .table(DeployAggregate::Table)
                .col(DeployAggregate::BlockTimestampUtcEpochMillis),
        )
        .index(
            Index::create()
                .if_not_exists()
                .name("IDX_DeployAggregate_BlockHash")
                .table(DeployAggregate::Table)
                .col(DeployAggregate::BlockHash),
        )
        .to_owned()
}

pub fn create_update_stmt(
    deploy_hash: String,
    maybe_deploy_processed_raw: Option<String>,
    maybe_deploy_expired_raw: Option<String>,
    maybe_block_data: Option<(String, u64)>,
) -> UpdateStatement {
    let mut values = Vec::new();
    if let Some(deploy_processed_raw) = maybe_deploy_processed_raw {
        values.push((DeployAggregate::DeployProcessedRaw, deploy_processed_raw.into()));
    }
    if let Some(deploy_expired_raw) = maybe_deploy_expired_raw {
        values.push((DeployAggregate::DeployExpiredRaw, deploy_expired_raw.into()));
    }
    if let Some((block_hash, block_timestamp)) = maybe_block_data {
        values.push((DeployAggregate::BlockHash, block_hash.into()));
        values.push((DeployAggregate::BlockTimestampUtcEpochMillis, block_timestamp.into()));
    }

    Query::update()
        .table(DeployAggregate::Table)
        .values(values)
        .and_where(Expr::col(DeployAggregate::BlockHash).eq(deploy_hash.into()))
        .to_owned()
}

pub fn create_insert_stmt(
    deploy_hash: String,
    deploy_accepted_raw: String,
) -> SqResult<InsertStatement> {
    Ok(Query::insert()
        .into_table(DeployAggregate::Table)
        .columns([
            DeployAggregate::DeployHash,
            DeployAggregate::DeployAcceptedRaw,
        ])
        .values(vec![deploy_hash.into(), deploy_accepted_raw.into()])?
        .on_conflict(
            OnConflict::column(DeployAggregate::DeployHash)
                .do_nothing()
                .to_owned(),
        )
        .to_owned())
}
