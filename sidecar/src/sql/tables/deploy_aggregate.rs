use sea_query::{
    error::Result as SqResult, Alias, BlobSize, ColumnDef, Iden, Index, NullOrdering,
    OrderedStatement, Query, SelectStatement, Table, TableCreateStatement, IndexCreateStatement,
};

use crate::types::database::{DeployAggregateFilter, DeployAggregateSortColumn, SortOrder};
use sea_query::{Cond, Expr, InsertStatement, OnConflict, Order, UpdateStatement};

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
                    Expr::col((DeployAggregate::Table, DeployAggregate::DeployHash)),
                    Alias::new("deploy_hash"),
                )
                .expr_as(
                    Expr::col((DeployAggregate::Table, DeployAggregate::DeployAcceptedRaw)),
                    Alias::new("deploy_accepted_raw"),
                )
                .expr_as(
                    Expr::col((DeployAggregate::Table, DeployAggregate::DeployProcessedRaw)),
                    Alias::new("deploy_processed_raw"),
                )
                .expr_as(
                    Expr::col((DeployAggregate::Table, DeployAggregate::DeployExpiredRaw)).is_not_null(),
                    Alias::new("is_expired"),
                )
                .expr_as(
                    Expr::col((
                        DeployAggregate::Table,
                        DeployAggregate::BlockTimestampUtcEpochMillis,
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
            Expr::col((DeployAggregate::Table, DeployAggregate::DeployHash)).count(),
            Alias::new("count"),
        ),
    )
    .to_owned()
}

pub fn create_deploy_aggregate_block_hash_timestamp_index() -> IndexCreateStatement {
    Index::create()
        .if_not_exists()
        .name("IDX_DeployAggregate_BlockTimestamp")
        .table(DeployAggregate::Table)
        .col(DeployAggregate::BlockTimestampUtcEpochMillis)
        .to_owned()
}

pub fn create_deploy_aggregate_block_hash_index() -> IndexCreateStatement {
    Index::create()
        .if_not_exists()
        .name("IDX_DeployAggregate_BlockHash")
        .table(DeployAggregate::Table)
        .col(DeployAggregate::BlockHash)
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
                    DeployAggregate::Table,
                    DeployAggregate::BlockTimestampUtcEpochMillis,
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
    let mut conditions = Cond::all();
    if exclude_expired {
        conditions =
            conditions.add(Expr::tbl(DeployAggregate::Table, DeployAggregate::DeployExpiredRaw).is_not_null())
    }
    if exclude_not_processed {
        conditions = conditions
            .add(Expr::tbl(DeployAggregate::Table, DeployAggregate::DeployProcessedRaw).is_not_null())
    }
    select
        .from(DeployAggregate::Table)
        .cond_where(conditions)
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
                .integer_len(13),
        )
        .index(
            Index::create()
                .unique()
                .primary()
                .name("PDX_DeployAggregate")
                .col(DeployAggregate::DeployHash),
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
        .and_where(Expr::col(DeployAggregate::DeployHash).eq(deploy_hash))
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
