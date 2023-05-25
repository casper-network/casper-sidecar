use sea_query::{Alias, NullOrdering, OrderedStatement, Query, SelectStatement};

use super::block_deploys::BlockDeploys;
use crate::sql::tables::deploy_accepted::DeployAccepted;
use crate::sql::tables::deploy_expired::DeployExpired;
use crate::sql::tables::deploy_processed::DeployProcessed;
use crate::types::database::{DeployAggregateFilter, DeployAggregateSortColumn, SortOrder};
use sea_query::{Cond, Expr, JoinType, Order};

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
    conditions = conditions.add(Expr::tbl(DeployAccepted::Table, DeployAccepted::DeployHash).is_not_null());
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

#[test]
fn create_list_by_filter_query_given_filter_should_create_paginated_sql() {
    use sea_query::SqliteQueryBuilder;
    let expected_sql = "SELECT \"DeployAccepted\".\"deploy_hash\" AS \"deploy_hash\", \"DeployAccepted\".\"raw\" AS \"deploy_accepted_raw\", \"DeployProcessed\".\"raw\" AS \"deploy_processed_raw\", \"DeployExpired\".\"deploy_hash\" IS NOT NULL AS \"is_expired\", \"BlockDeploys\".\"block_timestamp_utc_epoch_millis\" AS \"block_timestamp\" FROM \"DeployAccepted\" LEFT JOIN \"DeployProcessed\" ON \"DeployProcessed\".\"deploy_hash\" = \"DeployAccepted\".\"deploy_hash\" LEFT JOIN \"DeployExpired\" ON \"DeployExpired\".\"deploy_hash\" = \"DeployAccepted\".\"deploy_hash\" LEFT JOIN \"BlockDeploys\" ON \"BlockDeploys\".\"deploy_hash\" = \"DeployAccepted\".\"deploy_hash\" LIMIT 17 OFFSET 20";
    let filter = DeployAggregateFilter {
        limit: 17,
        offset: 20,
        exclude_expired: false,
        exclude_not_processed: false,
        sort_column: None,
        sort_order: None,
    };

    let got_sql = create_list_by_filter_query(filter).to_string(SqliteQueryBuilder);

    assert_eq!(got_sql, expected_sql)
}

#[test]
fn create_list_by_filter_query_given_filter_with_exclude_expired_and_exclude_not_processed_should_create_sql_without_expired_and_not_processed(
) {
    use sea_query::SqliteQueryBuilder;
    let expected_sql = "SELECT \"DeployAccepted\".\"deploy_hash\" AS \"deploy_hash\", \"DeployAccepted\".\"raw\" AS \"deploy_accepted_raw\", \"DeployProcessed\".\"raw\" AS \"deploy_processed_raw\", \"DeployExpired\".\"deploy_hash\" IS NOT NULL AS \"is_expired\", \"BlockDeploys\".\"block_timestamp_utc_epoch_millis\" AS \"block_timestamp\" FROM \"DeployAccepted\" LEFT JOIN \"DeployProcessed\" ON \"DeployProcessed\".\"deploy_hash\" = \"DeployAccepted\".\"deploy_hash\" LEFT JOIN \"DeployExpired\" ON \"DeployExpired\".\"deploy_hash\" = \"DeployAccepted\".\"deploy_hash\" LEFT JOIN \"BlockDeploys\" ON \"BlockDeploys\".\"deploy_hash\" = \"DeployAccepted\".\"deploy_hash\" WHERE \"DeployExpired\".\"deploy_hash\" IS NULL AND \"DeployProcessed\".\"deploy_hash\" IS NOT NULL LIMIT 17 OFFSET 20";
    let filter = DeployAggregateFilter {
        limit: 17,
        offset: 20,
        exclude_expired: true,
        exclude_not_processed: true,
        sort_column: None,
        sort_order: None,
    };

    let got_sql = create_list_by_filter_query(filter).to_string(SqliteQueryBuilder);

    assert_eq!(got_sql, expected_sql)
}

#[test]
fn create_list_by_filter_query_given_filter_with_no_sort_order_should_default_to_desc() {
    use sea_query::SqliteQueryBuilder;
    let expected_sql = "SELECT \"DeployAccepted\".\"deploy_hash\" AS \"deploy_hash\", \"DeployAccepted\".\"raw\" AS \"deploy_accepted_raw\", \"DeployProcessed\".\"raw\" AS \"deploy_processed_raw\", \"DeployExpired\".\"deploy_hash\" IS NOT NULL AS \"is_expired\", \"BlockDeploys\".\"block_timestamp_utc_epoch_millis\" AS \"block_timestamp\" FROM \"DeployAccepted\" LEFT JOIN \"DeployProcessed\" ON \"DeployProcessed\".\"deploy_hash\" = \"DeployAccepted\".\"deploy_hash\" LEFT JOIN \"DeployExpired\" ON \"DeployExpired\".\"deploy_hash\" = \"DeployAccepted\".\"deploy_hash\" LEFT JOIN \"BlockDeploys\" ON \"BlockDeploys\".\"deploy_hash\" = \"DeployAccepted\".\"deploy_hash\" ORDER BY \"BlockDeploys\".\"block_timestamp_utc_epoch_millis\" DESC NULLS LAST LIMIT 11 OFFSET 55";
    let filter = DeployAggregateFilter {
        limit: 11,
        offset: 55,
        exclude_expired: false,
        exclude_not_processed: false,
        sort_column: Some(DeployAggregateSortColumn::BlockTimestamp),
        sort_order: None,
    };

    let got_sql = create_list_by_filter_query(filter).to_string(SqliteQueryBuilder);

    assert_eq!(got_sql, expected_sql)
}

#[test]
fn create_list_by_filter_query_given_asc_sort_order_should_create_asc_sql() {
    use sea_query::SqliteQueryBuilder;
    let expected_sql = "SELECT \"DeployAccepted\".\"deploy_hash\" AS \"deploy_hash\", \"DeployAccepted\".\"raw\" AS \"deploy_accepted_raw\", \"DeployProcessed\".\"raw\" AS \"deploy_processed_raw\", \"DeployExpired\".\"deploy_hash\" IS NOT NULL AS \"is_expired\", \"BlockDeploys\".\"block_timestamp_utc_epoch_millis\" AS \"block_timestamp\" FROM \"DeployAccepted\" LEFT JOIN \"DeployProcessed\" ON \"DeployProcessed\".\"deploy_hash\" = \"DeployAccepted\".\"deploy_hash\" LEFT JOIN \"DeployExpired\" ON \"DeployExpired\".\"deploy_hash\" = \"DeployAccepted\".\"deploy_hash\" LEFT JOIN \"BlockDeploys\" ON \"BlockDeploys\".\"deploy_hash\" = \"DeployAccepted\".\"deploy_hash\" ORDER BY \"BlockDeploys\".\"block_timestamp_utc_epoch_millis\" ASC NULLS LAST LIMIT 11 OFFSET 55";
    let filter = DeployAggregateFilter {
        limit: 11,
        offset: 55,
        exclude_expired: false,
        exclude_not_processed: false,
        sort_column: Some(DeployAggregateSortColumn::BlockTimestamp),
        sort_order: Some(SortOrder::Asc),
    };

    let got_sql = create_list_by_filter_query(filter).to_string(SqliteQueryBuilder);

    assert_eq!(got_sql, expected_sql)
}

#[test]
fn create_list_by_filter_query_given_desc_sort_order_should_create_desc_sql() {
    use sea_query::SqliteQueryBuilder;
    let expected_sql = "SELECT \"DeployAccepted\".\"deploy_hash\" AS \"deploy_hash\", \"DeployAccepted\".\"raw\" AS \"deploy_accepted_raw\", \"DeployProcessed\".\"raw\" AS \"deploy_processed_raw\", \"DeployExpired\".\"deploy_hash\" IS NOT NULL AS \"is_expired\", \"BlockDeploys\".\"block_timestamp_utc_epoch_millis\" AS \"block_timestamp\" FROM \"DeployAccepted\" LEFT JOIN \"DeployProcessed\" ON \"DeployProcessed\".\"deploy_hash\" = \"DeployAccepted\".\"deploy_hash\" LEFT JOIN \"DeployExpired\" ON \"DeployExpired\".\"deploy_hash\" = \"DeployAccepted\".\"deploy_hash\" LEFT JOIN \"BlockDeploys\" ON \"BlockDeploys\".\"deploy_hash\" = \"DeployAccepted\".\"deploy_hash\" ORDER BY \"BlockDeploys\".\"block_timestamp_utc_epoch_millis\" DESC NULLS LAST LIMIT 11 OFFSET 55";
    let filter = DeployAggregateFilter {
        limit: 11,
        offset: 55,
        exclude_expired: false,
        exclude_not_processed: false,
        sort_column: Some(DeployAggregateSortColumn::BlockTimestamp),
        sort_order: Some(SortOrder::Desc),
    };

    let got_sql = create_list_by_filter_query(filter).to_string(SqliteQueryBuilder);

    assert_eq!(got_sql, expected_sql)
}

#[test]
fn create_count_aggregate_deploys_query_should_create_count_sql() {
    use sea_query::SqliteQueryBuilder;
    let expected_sql = "SELECT COUNT(\"DeployAccepted\".\"deploy_hash\") AS \"count\" FROM \"DeployAccepted\" LEFT JOIN \"DeployProcessed\" ON \"DeployProcessed\".\"deploy_hash\" = \"DeployAccepted\".\"deploy_hash\" LEFT JOIN \"DeployExpired\" ON \"DeployExpired\".\"deploy_hash\" = \"DeployAccepted\".\"deploy_hash\" LEFT JOIN \"BlockDeploys\" ON \"BlockDeploys\".\"deploy_hash\" = \"DeployAccepted\".\"deploy_hash\"";
    let filter = DeployAggregateFilter {
        limit: 17,
        offset: 20,
        exclude_expired: false,
        exclude_not_processed: false,
        sort_column: None,
        sort_order: None,
    };

    let got_sql = create_count_aggregate_deploys_query(filter).to_string(SqliteQueryBuilder);

    assert_eq!(got_sql, expected_sql)
}
