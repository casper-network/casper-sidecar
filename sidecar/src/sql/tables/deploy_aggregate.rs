use super::deploy_accepted::DeployAccepted;
use super::{deploy_expired::DeployExpired, deploy_processed::DeployProcessed};
use crate::types::database::{DeployAggregateFilter, DeployAggregateSortColumn, SortOrder};
use sea_query::{
    error::Result as SqResult, Alias, BlobSize, ColumnDef, Iden, Index, IndexCreateStatement,
    NullOrdering, OrderedStatement, Query, SelectStatement, SimpleExpr, SubQueryStatement, Table,
    TableCreateStatement,
};
use sea_query::{Cond, Expr, InsertStatement, OnConflict, Order};

#[cfg(test)]
use sea_query::SqliteQueryBuilder;

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
    decorate_with_joins(
        filter.exclude_expired,
        filter.exclude_not_processed,
        Query::select().expr_as(
            Expr::asterisk().count(),
            Alias::new("count"),
        ),
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

fn decorate_with_joins(
    exclude_expired: bool,
    exclude_not_processed: bool,
    select: &mut SelectStatement,
) -> &mut SelectStatement {
    let mut conditions = Cond::all()
        .add(Expr::tbl(DeployAggregate::Table, DeployAggregate::IsAccepted).eq(true));
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
        .col(ColumnDef::new(DeployAggregate::IsAccepted).boolean().not_null())
        .col(ColumnDef::new(DeployAggregate::DeployProcessedRaw).blob(BlobSize::Tiny))
        .col(ColumnDef::new(DeployAggregate::IsProcessed).boolean().not_null())
        .col(ColumnDef::new(DeployAggregate::IsExpired).boolean().not_null())
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
    let sub_select_raw_accepted = Query::select()
        .expr(Expr::col(DeployAccepted::Raw))
        .from(DeployAccepted::Table)
        .and_where(Expr::col(DeployAccepted::DeployHash).eq(deploy_hash.clone()))
        .to_owned();
    let accepted_expr = SimpleExpr::SubQuery(Box::new(SubQueryStatement::SelectStatement(
        sub_select_raw_accepted,
    )));
    let sub_select_is_accepted = Query::select()
        .expr(Expr::col(DeployAccepted::Raw).is_not_null())
        .from(DeployAccepted::Table)
        .and_where(Expr::col(DeployAccepted::DeployHash).eq(deploy_hash.clone()))
        .to_owned();
    let is_accepted_expr: SimpleExpr = SimpleExpr::SubQuery(Box::new(SubQueryStatement::SelectStatement(
        sub_select_is_accepted,
    )));
    let sub_select_raw_processed = Query::select()
        .expr(Expr::col(DeployProcessed::Raw))
        .from(DeployProcessed::Table)
        .and_where(Expr::col(DeployProcessed::DeployHash).eq(deploy_hash.clone()))
        .to_owned();
    let processed_expr = SimpleExpr::SubQuery(Box::new(SubQueryStatement::SelectStatement(
        sub_select_raw_processed,
    )));
    let sub_select_is_processed = Query::select()
        .expr(Expr::col(DeployProcessed::Raw).is_not_null())
        .from(DeployProcessed::Table)
        .and_where(Expr::col(DeployProcessed::DeployHash).eq(deploy_hash.clone()))
        .to_owned();
    let is_processed_expr = SimpleExpr::SubQuery(Box::new(SubQueryStatement::SelectStatement(
        sub_select_is_processed,
    )));
    let sub_select_raw_expired = Query::select()
        .expr(Expr::col(DeployExpired::Raw).is_not_null())
        .from(DeployExpired::Table)
        .and_where(Expr::col(DeployExpired::DeployHash).eq(deploy_hash.clone()))
        
        .to_owned();
    let is_expired_expr: SimpleExpr = SimpleExpr::SubQuery(Box::new(SubQueryStatement::SelectStatement(
        sub_select_raw_expired,
    )));
    let mut cols = vec![
        DeployAggregate::DeployHash,
        DeployAggregate::DeployAcceptedRaw,
        DeployAggregate::IsAccepted,
        DeployAggregate::IsExpired,
        DeployAggregate::DeployProcessedRaw,
        DeployAggregate::IsProcessed,
    ];
    let mut exprs = vec![
        Expr::value(deploy_hash),
        accepted_expr,
        Expr::expr(is_accepted_expr).if_null(false),
        Expr::expr(is_expired_expr).if_null(false),
        processed_expr,
        Expr::expr(is_processed_expr).if_null(false),
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

#[test]
pub fn create_update_stmt_test() {
    let sql = create_update_stmt(
        "dpl1".to_string(),
        Some(("block_1".to_string(), 123456 as u64)),
    )
    .unwrap()
    .to_string(SqliteQueryBuilder);
    assert_eq!(
        sql,
        "INSERT INTO \"DeployAggregate\" (\"deploy_hash\", \"deploy_accepted_raw\", \"is_accepted\", \"is_expired\", \"deploy_processed_raw\", \"is_processed\", \"block_hash\", \"block_timestamp_utc_epoch_millis\") VALUES ('dpl1', (SELECT \"raw\" FROM \"DeployAccepted\" WHERE \"deploy_hash\" = 'dpl1'), (SELECT \"raw\" IS NOT NULL FROM \"DeployAccepted\" WHERE \"deploy_hash\" = 'dpl1'), (SELECT \"raw\" IS NOT NULL FROM \"DeployExpired\" WHERE \"deploy_hash\" = 'dpl1'), (SELECT \"raw\" FROM \"DeployProcessed\" WHERE \"deploy_hash\" = 'dpl1'), (SELECT \"raw\" IS NOT NULL FROM \"DeployProcessed\" WHERE \"deploy_hash\" = 'dpl1'), 'block_1', 123456) ON CONFLICT (\"deploy_hash\") DO UPDATE SET \"deploy_hash\" = \"excluded\".\"deploy_hash\", \"deploy_accepted_raw\" = \"excluded\".\"deploy_accepted_raw\", \"is_accepted\" = \"excluded\".\"is_accepted\", \"is_expired\" = \"excluded\".\"is_expired\", \"deploy_processed_raw\" = \"excluded\".\"deploy_processed_raw\", \"is_processed\" = \"excluded\".\"is_processed\", \"block_hash\" = \"excluded\".\"block_hash\", \"block_timestamp_utc_epoch_millis\" = \"excluded\".\"block_timestamp_utc_epoch_millis\""
    )
}

#[test]
pub fn create_update_stmt_test_without_block_data() {
    let sql = create_update_stmt("dpl1".to_string(), None)
        .unwrap()
        .to_string(SqliteQueryBuilder);
    assert_eq!(
        sql,
        "INSERT INTO \"DeployAggregate\" (\"deploy_hash\", \"deploy_accepted_raw\", \"is_accepted\", \"is_expired\", \"deploy_processed_raw\", \"is_processed\") VALUES ('dpl1', (SELECT \"raw\" FROM \"DeployAccepted\" WHERE \"deploy_hash\" = 'dpl1'), (SELECT \"raw\" IS NOT NULL FROM \"DeployAccepted\" WHERE \"deploy_hash\" = 'dpl1'), (SELECT \"raw\" IS NOT NULL FROM \"DeployExpired\" WHERE \"deploy_hash\" = 'dpl1'), (SELECT \"raw\" FROM \"DeployProcessed\" WHERE \"deploy_hash\" = 'dpl1'), (SELECT \"raw\" IS NOT NULL FROM \"DeployProcessed\" WHERE \"deploy_hash\" = 'dpl1')) ON CONFLICT (\"deploy_hash\") DO UPDATE SET \"deploy_hash\" = \"excluded\".\"deploy_hash\", \"deploy_accepted_raw\" = \"excluded\".\"deploy_accepted_raw\", \"is_accepted\" = \"excluded\".\"is_accepted\", \"is_expired\" = \"excluded\".\"is_expired\", \"deploy_processed_raw\" = \"excluded\".\"deploy_processed_raw\", \"is_processed\" = \"excluded\".\"is_processed\""
    )
}