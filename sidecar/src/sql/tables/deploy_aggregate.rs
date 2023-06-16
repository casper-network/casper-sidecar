use crate::sql::tables::deploy_expired::DeployExpired;
use crate::sql::tables::deploy_processed::DeployProcessed;
use crate::types::database::{DeployAggregateFilter, DeployAggregateSortColumn, SortOrder};
#[cfg(test)]
use sea_query::SqliteQueryBuilder;
use sea_query::{
    error::Result as SqResult, Alias, BlobSize, ColumnDef, Iden, Index, IndexCreateStatement,
    InsertStatement, NullOrdering, OrderedStatement, Query, SelectStatement, Table,
    TableCreateStatement,
};
use sea_query::{Cond, Expr, OnConflict, Order, SimpleExpr, SubQueryStatement};

use super::deploy_accepted::DeployAccepted;

#[derive(Iden, Clone)]
pub(super) enum DeployAggregate {
    #[iden = "DeployAggregate"]
    Table,
    DeployHash,
    DeployAcceptedRaw,
    IsAccepted,
    DeployProcessedRaw,
    IsProcessed,
    IsExpired,
    BlockHash,
    BlockTimestampUtcEpochMillis,
}

/// Creates SelectStatement based on input filter.
/// Applies according conditions based on filter flags.
pub fn create_list_by_filter_query(filter: DeployAggregateFilter) -> SelectStatement {
    let offset = filter.offset;
    let limit = filter.limit;
    let sort_column = filter.sort_column;
    let sort_order = filter.sort_order;
    decorate_with_order_by(
        sort_column,
        sort_order,
        decorate_with_conditions(
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
                    Expr::col((DeployAggregate::Table, DeployAggregate::IsExpired)),
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
    decorate_with_conditions(
        filter.exclude_expired,
        filter.exclude_not_processed,
        Query::select().expr_as(Expr::asterisk().count(), Alias::new("count")),
    )
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

pub fn create_deploy_aggregate_is_accepted_and_timestamp_index() -> IndexCreateStatement {
    Index::create()
        .if_not_exists()
        .name("IDX_DeployAggregate_IsAccepted_Timestamp")
        .table(DeployAggregate::Table)
        .col(DeployAggregate::IsAccepted)
        .col(DeployAggregate::BlockTimestampUtcEpochMillis)
        .to_owned()
}

pub fn create_deploy_aggregate_is_processed_index() -> IndexCreateStatement {
    Index::create()
        .if_not_exists()
        .name("IDX_DeployAggregate_IsProcessed")
        .table(DeployAggregate::Table)
        .col(DeployAggregate::IsProcessed)
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

fn decorate_with_conditions(
    exclude_expired: bool,
    exclude_not_processed: bool,
    select: &mut SelectStatement,
) -> &mut SelectStatement {
    let mut conditions =
        Cond::all().add(Expr::tbl(DeployAggregate::Table, DeployAggregate::IsAccepted).eq(true));
    if exclude_expired {
        conditions =
            conditions.add(Expr::tbl(DeployAggregate::Table, DeployAggregate::IsExpired).eq(false))
    }
    if exclude_not_processed {
        conditions =
            conditions.add(Expr::tbl(DeployAggregate::Table, DeployAggregate::IsProcessed).eq(true))
    }
    select.from(DeployAggregate::Table).cond_where(conditions)
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
        .col(ColumnDef::new(DeployAggregate::DeployAcceptedRaw).blob(BlobSize::Tiny))
        .col(
            ColumnDef::new(DeployAggregate::IsAccepted)
                .boolean()
                .not_null(),
        )
        .col(ColumnDef::new(DeployAggregate::DeployProcessedRaw).blob(BlobSize::Tiny))
        .col(
            ColumnDef::new(DeployAggregate::IsProcessed)
                .boolean()
                .not_null(),
        )
        .col(
            ColumnDef::new(DeployAggregate::IsExpired)
                .boolean()
                .not_null(),
        )
        .col(ColumnDef::new(DeployAggregate::BlockHash).string())
        .col(ColumnDef::new(DeployAggregate::BlockTimestampUtcEpochMillis).integer_len(13))
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
    maybe_block_data: Option<(String, u64)>,
) -> SqResult<InsertStatement> {
    let mut cols = vec![
        DeployAggregate::DeployHash,
        DeployAggregate::DeployAcceptedRaw,
        DeployAggregate::IsAccepted,
        DeployAggregate::IsExpired,
        DeployAggregate::DeployProcessedRaw,
        DeployAggregate::IsProcessed,
    ];
    let deploy_hash_ref = deploy_hash.as_str();
    let mut exprs = vec![
        Expr::value(deploy_hash_ref),
        sub_select_raw_accepted(deploy_hash_ref),
        Expr::expr(sub_select_is_accepted(deploy_hash_ref)).if_null(false),
        Expr::expr(sub_select_is_expired(deploy_hash_ref)).if_null(false),
        sub_select_raw_processed(deploy_hash_ref),
        Expr::expr(sub_select_is_processed(deploy_hash_ref)).if_null(false),
    ];
    if let Some((block_hash, block_timestamp)) = maybe_block_data {
        cols.push(DeployAggregate::BlockHash);
        cols.push(DeployAggregate::BlockTimestampUtcEpochMillis);
        exprs.push(Expr::value(block_hash));
        exprs.push(Expr::value(block_timestamp));
    }
    let builder = Query::insert()
        .into_table(DeployAggregate::Table)
        .columns(cols.clone())
        .exprs(exprs)?
        .on_conflict(
            OnConflict::column(DeployAggregate::DeployHash)
                .update_columns(cols)
                .to_owned(),
        )
        .to_owned();
    Ok(builder)
}

fn sub_select_raw_accepted(deploy_hash: &str) -> SimpleExpr {
    let sub_select_raw_accepted = Query::select()
        .expr(Expr::col(DeployAccepted::Raw))
        .from(DeployAccepted::Table)
        .and_where(Expr::col(DeployAccepted::DeployHash).eq(deploy_hash))
        .to_owned();
    SimpleExpr::SubQuery(Box::new(SubQueryStatement::SelectStatement(
        sub_select_raw_accepted,
    )))
}

fn sub_select_is_accepted(deploy_hash: &str) -> SimpleExpr {
    let sub_select_is_accepted = Query::select()
        .expr(Expr::col(DeployAccepted::Raw).is_not_null())
        .from(DeployAccepted::Table)
        .and_where(Expr::col(DeployAccepted::DeployHash).eq(deploy_hash))
        .to_owned();
    SimpleExpr::SubQuery(Box::new(SubQueryStatement::SelectStatement(
        sub_select_is_accepted,
    )))
}

fn sub_select_raw_processed(deploy_hash: &str) -> SimpleExpr {
    let sub_select_raw_processed = Query::select()
        .expr(Expr::col(DeployProcessed::Raw))
        .from(DeployProcessed::Table)
        .and_where(Expr::col(DeployProcessed::DeployHash).eq(deploy_hash))
        .to_owned();
    SimpleExpr::SubQuery(Box::new(SubQueryStatement::SelectStatement(
        sub_select_raw_processed,
    )))
}

fn sub_select_is_processed(deploy_hash: &str) -> SimpleExpr {
    let sub_select_is_processed = Query::select()
        .expr(Expr::col(DeployProcessed::Raw).is_not_null())
        .from(DeployProcessed::Table)
        .and_where(Expr::col(DeployProcessed::DeployHash).eq(deploy_hash))
        .to_owned();
    SimpleExpr::SubQuery(Box::new(SubQueryStatement::SelectStatement(
        sub_select_is_processed,
    )))
}

fn sub_select_is_expired(deploy_hash: &str) -> SimpleExpr {
    let sub_select_raw_expired = Query::select()
        .expr(Expr::col(DeployExpired::Raw).is_not_null())
        .from(DeployExpired::Table)
        .and_where(Expr::col(DeployExpired::DeployHash).eq(deploy_hash))
        .to_owned();
    SimpleExpr::SubQuery(Box::new(SubQueryStatement::SelectStatement(
        sub_select_raw_expired,
    )))
}

#[test]
pub fn create_list_stmt_with_no_filter_values() {
    let filter = DeployAggregateFilter {
        exclude_expired: false,
        exclude_not_processed: false,
        limit: 50,
        offset: 15,
        sort_column: None,
        sort_order: None,
    };
    let got = create_list_by_filter_query(filter).to_string(SqliteQueryBuilder);
    let expected = "SELECT \"DeployAggregate\".\"deploy_hash\" AS \"deploy_hash\", \"DeployAggregate\".\"deploy_accepted_raw\" AS \"deploy_accepted_raw\", \"DeployAggregate\".\"deploy_processed_raw\" AS \"deploy_processed_raw\", \"DeployAggregate\".\"is_expired\" AS \"is_expired\", \"DeployAggregate\".\"block_timestamp_utc_epoch_millis\" AS \"block_timestamp\" FROM \"DeployAggregate\" WHERE \"DeployAggregate\".\"is_accepted\" = TRUE LIMIT 50 OFFSET 15";
    assert_eq!(got, expected);
}

#[test]
pub fn create_list_stmt_with_filter_values_block_timestamp_asc() {
    let filter = DeployAggregateFilter {
        exclude_expired: true,
        exclude_not_processed: true,
        limit: 60,
        offset: 17,
        sort_column: Some(DeployAggregateSortColumn::BlockTimestamp),
        sort_order: Some(SortOrder::Asc),
    };
    let got = create_list_by_filter_query(filter).to_string(SqliteQueryBuilder);
    let expected = "SELECT \"DeployAggregate\".\"deploy_hash\" AS \"deploy_hash\", \"DeployAggregate\".\"deploy_accepted_raw\" AS \"deploy_accepted_raw\", \"DeployAggregate\".\"deploy_processed_raw\" AS \"deploy_processed_raw\", \"DeployAggregate\".\"is_expired\" AS \"is_expired\", \"DeployAggregate\".\"block_timestamp_utc_epoch_millis\" AS \"block_timestamp\" FROM \"DeployAggregate\" WHERE \"DeployAggregate\".\"is_accepted\" = TRUE AND \"DeployAggregate\".\"is_expired\" = FALSE AND \"DeployAggregate\".\"is_processed\" = TRUE ORDER BY \"DeployAggregate\".\"block_timestamp_utc_epoch_millis\" ASC NULLS LAST LIMIT 60 OFFSET 17";
    assert_eq!(got, expected);
}

#[test]
pub fn create_list_stmt_with_filter_values_block_timestamp_des() {
    let filter = DeployAggregateFilter {
        exclude_expired: true,
        exclude_not_processed: true,
        limit: 60,
        offset: 17,
        sort_column: Some(DeployAggregateSortColumn::BlockTimestamp),
        sort_order: Some(SortOrder::Desc),
    };
    let got = create_list_by_filter_query(filter).to_string(SqliteQueryBuilder);
    let expected = "SELECT \"DeployAggregate\".\"deploy_hash\" AS \"deploy_hash\", \"DeployAggregate\".\"deploy_accepted_raw\" AS \"deploy_accepted_raw\", \"DeployAggregate\".\"deploy_processed_raw\" AS \"deploy_processed_raw\", \"DeployAggregate\".\"is_expired\" AS \"is_expired\", \"DeployAggregate\".\"block_timestamp_utc_epoch_millis\" AS \"block_timestamp\" FROM \"DeployAggregate\" WHERE \"DeployAggregate\".\"is_accepted\" = TRUE AND \"DeployAggregate\".\"is_expired\" = FALSE AND \"DeployAggregate\".\"is_processed\" = TRUE ORDER BY \"DeployAggregate\".\"block_timestamp_utc_epoch_millis\" DESC NULLS LAST LIMIT 60 OFFSET 17";
    assert_eq!(got, expected);
}

#[test]
pub fn create_count_stmt_with_filter_values() {
    let filter = DeployAggregateFilter {
        exclude_expired: true,
        exclude_not_processed: true,
        limit: 60,
        offset: 17,
        sort_column: Some(DeployAggregateSortColumn::BlockTimestamp),
        sort_order: Some(SortOrder::Desc),
    };
    let got = create_count_aggregate_deploys_query(filter).to_string(SqliteQueryBuilder);
    let expected = "SELECT COUNT(*) AS \"count\" FROM \"DeployAggregate\" WHERE \"DeployAggregate\".\"is_accepted\" = TRUE AND \"DeployAggregate\".\"is_expired\" = FALSE AND \"DeployAggregate\".\"is_processed\" = TRUE";
    assert_eq!(got, expected);
}

#[test]
pub fn create_count_stmt_without_filter_values() {
    let filter = DeployAggregateFilter {
        exclude_expired: false,
        exclude_not_processed: false,
        limit: 60,
        offset: 17,
        sort_column: Some(DeployAggregateSortColumn::BlockTimestamp),
        sort_order: Some(SortOrder::Desc),
    };
    let got = create_count_aggregate_deploys_query(filter).to_string(SqliteQueryBuilder);
    let expected = "SELECT COUNT(*) AS \"count\" FROM \"DeployAggregate\" WHERE \"DeployAggregate\".\"is_accepted\" = TRUE";
    assert_eq!(got, expected);
}

#[test]
pub fn create_table_stmt_produces_ddl_sql() {
    let got = create_table_stmt().to_string(SqliteQueryBuilder);
    let expected = "CREATE TABLE IF NOT EXISTS \"DeployAggregate\" ( \"deploy_hash\" text NOT NULL, \"deploy_accepted_raw\" blob, \"is_accepted\" integer NOT NULL, \"deploy_processed_raw\" blob, \"is_processed\" integer NOT NULL, \"is_expired\" integer NOT NULL, \"block_hash\" text, \"block_timestamp_utc_epoch_millis\" integer(13), CONSTRAINT \"PDX_DeployAggregate\"PRIMARY KEY (\"deploy_hash\") )";
    assert_eq!(got, expected);
}

#[test]
pub fn create_update_stmt_test() {
    let sql = create_update_stmt("dpl1".to_string(), Some(("block_1".to_string(), 123456)))
        .unwrap()
        .to_string(SqliteQueryBuilder);
    assert_eq!(
            sql,
            "INSERT INTO \"DeployAggregate\" (\"deploy_hash\", \"deploy_accepted_raw\", \"is_accepted\", \"is_expired\", \"deploy_processed_raw\", \"is_processed\", \"block_hash\", \"block_timestamp_utc_epoch_millis\") VALUES ('dpl1', (SELECT \"raw\" FROM \"DeployAccepted\" WHERE \"deploy_hash\" = 'dpl1'), IFNULL((SELECT \"raw\" IS NOT NULL FROM \"DeployAccepted\" WHERE \"deploy_hash\" = 'dpl1'), FALSE), IFNULL((SELECT \"raw\" IS NOT NULL FROM \"DeployExpired\" WHERE \"deploy_hash\" = 'dpl1'), FALSE), (SELECT \"raw\" FROM \"DeployProcessed\" WHERE \"deploy_hash\" = 'dpl1'), IFNULL((SELECT \"raw\" IS NOT NULL FROM \"DeployProcessed\" WHERE \"deploy_hash\" = 'dpl1'), FALSE), 'block_1', 123456) ON CONFLICT (\"deploy_hash\") DO UPDATE SET \"deploy_hash\" = \"excluded\".\"deploy_hash\", \"deploy_accepted_raw\" = \"excluded\".\"deploy_accepted_raw\", \"is_accepted\" = \"excluded\".\"is_accepted\", \"is_expired\" = \"excluded\".\"is_expired\", \"deploy_processed_raw\" = \"excluded\".\"deploy_processed_raw\", \"is_processed\" = \"excluded\".\"is_processed\", \"block_hash\" = \"excluded\".\"block_hash\", \"block_timestamp_utc_epoch_millis\" = \"excluded\".\"block_timestamp_utc_epoch_millis\""
        )
}

#[test]
pub fn create_update_stmt_test_without_block_data() {
    let sql = create_update_stmt("dpl1".to_string(), None)
        .unwrap()
        .to_string(SqliteQueryBuilder);
    assert_eq!(
            sql,
            "INSERT INTO \"DeployAggregate\" (\"deploy_hash\", \"deploy_accepted_raw\", \"is_accepted\", \"is_expired\", \"deploy_processed_raw\", \"is_processed\") VALUES ('dpl1', (SELECT \"raw\" FROM \"DeployAccepted\" WHERE \"deploy_hash\" = 'dpl1'), IFNULL((SELECT \"raw\" IS NOT NULL FROM \"DeployAccepted\" WHERE \"deploy_hash\" = 'dpl1'), FALSE), IFNULL((SELECT \"raw\" IS NOT NULL FROM \"DeployExpired\" WHERE \"deploy_hash\" = 'dpl1'), FALSE), (SELECT \"raw\" FROM \"DeployProcessed\" WHERE \"deploy_hash\" = 'dpl1'), IFNULL((SELECT \"raw\" IS NOT NULL FROM \"DeployProcessed\" WHERE \"deploy_hash\" = 'dpl1'), FALSE)) ON CONFLICT (\"deploy_hash\") DO UPDATE SET \"deploy_hash\" = \"excluded\".\"deploy_hash\", \"deploy_accepted_raw\" = \"excluded\".\"deploy_accepted_raw\", \"is_accepted\" = \"excluded\".\"is_accepted\", \"is_expired\" = \"excluded\".\"is_expired\", \"deploy_processed_raw\" = \"excluded\".\"deploy_processed_raw\", \"is_processed\" = \"excluded\".\"is_processed\""
        )
}
