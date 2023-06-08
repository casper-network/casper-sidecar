use crate::types::database::{DeployAggregateFilter, DeployAggregateSortColumn, SortOrder};
#[cfg(test)]
use sea_query::{
    error::Result as SqResult, InsertStatement, SqliteQueryBuilder, UpdateStatement, Value,
};
use sea_query::{
    Alias, BlobSize, ColumnDef, Iden, Index, IndexCreateStatement, NullOrdering, OrderedStatement,
    Query, SelectStatement, Table, TableCreateStatement,
};
use sea_query::{Cond, Expr, Order};

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

#[cfg(test)]
pub fn create_insert_stmt(
    deploy_hash: String,
    deploy_accepted_raw: Option<String>,
    deploy_processed_raw: Option<String>,
    is_expired: bool,
    block_hash: Option<String>,
    block_timestamp: Option<u64>,
) -> SqResult<InsertStatement> {
    let is_accepted = deploy_accepted_raw.is_some();
    let is_processed = deploy_processed_raw.is_some();
    Query::insert()
        .into_table(DeployAggregate::Table)
        .columns([
            DeployAggregate::DeployHash,
            DeployAggregate::DeployAcceptedRaw,
            DeployAggregate::IsAccepted,
            DeployAggregate::DeployProcessedRaw,
            DeployAggregate::IsProcessed,
            DeployAggregate::IsExpired,
            DeployAggregate::BlockHash,
            DeployAggregate::BlockTimestampUtcEpochMillis,
        ])
        .values(vec![
            deploy_hash.into(),
            deploy_accepted_raw.into(),
            is_accepted.into(),
            deploy_processed_raw.into(),
            is_processed.into(),
            is_expired.into(),
            block_hash.into(),
            block_timestamp.into(),
        ])
        .map(|stmt| stmt.to_owned())
}

#[cfg(test)]
pub fn create_update_stmt(
    deploy_hash: String,
    deploy_accepted_raw: Option<String>,
    deploy_processed_raw: Option<String>,
    is_expired: bool,
    block_hash: Option<String>,
    block_timestamp: Option<u64>,
) -> UpdateStatement {
    let mut values: Vec<(DeployAggregate, Value)> =
        vec![(DeployAggregate::IsExpired, is_expired.into())];
    if let Some(da) = deploy_accepted_raw {
        values.push((DeployAggregate::DeployAcceptedRaw, da.into()));
        values.push((DeployAggregate::IsAccepted, true.into()));
    }
    if let Some(da) = deploy_processed_raw {
        values.push((DeployAggregate::DeployProcessedRaw, da.into()));
        values.push((DeployAggregate::IsProcessed, true.into()));
    }
    if let Some(bh) = block_hash {
        values.push((DeployAggregate::BlockHash, bh.into()));
    }
    if let Some(bt) = block_timestamp {
        values.push((DeployAggregate::BlockTimestampUtcEpochMillis, bt.into()));
    }
    Query::update()
        .table(DeployAggregate::Table)
        .values(values)
        .and_where(Expr::col(DeployAggregate::DeployHash).eq(deploy_hash))
        .to_owned()
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
