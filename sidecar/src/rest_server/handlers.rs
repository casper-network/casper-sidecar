use std::time::Instant;

use super::{
    errors::StorageError,
    requests::{ListDeploysRequest, Page},
};
use crate::{
    rest_server::errors::{InvalidParam, Unexpected},
    types::database::{DatabaseReadError, DatabaseReader, DeployAggregateFilter},
};
use anyhow::Error;
use casper_event_types::metrics::{metrics_summary, self};
use serde::Serialize;
use warp::{http::StatusCode, Rejection, Reply};

pub(super) async fn get_latest_block<Db: DatabaseReader + Clone + Send>(
    db: Db,
) -> Result<impl Reply, Rejection> {
    let db_result = db.get_latest_block().await;
    format_or_reject_storage_result(db_result)
}

pub(super) async fn get_block_by_hash<Db: DatabaseReader + Clone + Send>(
    hash: String,
    db: Db,
) -> Result<impl Reply, Rejection> {
    check_hash_is_correct_format(&hash)?;
    let db_result = db.get_block_by_hash(&hash).await;
    format_or_reject_storage_result(db_result)
}

pub(super) async fn get_block_by_height<Db: DatabaseReader + Clone + Send>(
    height: u64,
    db: Db,
) -> Result<impl Reply, Rejection> {
    let db_result = db.get_block_by_height(height).await;
    format_or_reject_storage_result(db_result)
}

pub(super) async fn get_deploy_by_hash<Db: DatabaseReader + Clone + Send>(
    hash: String,
    db: Db,
) -> Result<impl Reply, Rejection> {
    check_hash_is_correct_format(&hash)?;
    let db_result = db.get_deploy_aggregate_by_hash(&hash).await;
    format_or_reject_storage_result(db_result)
}

pub(super) async fn list_deploys<Db: DatabaseReader + Clone + Send>(
    list_deploys_request: ListDeploysRequest,
    db: Db,
) -> Result<impl Reply, Rejection> {
    let start = Instant::now();
    list_deploys_request.validate()?;
    let offset = list_deploys_request.offset.unwrap_or(0);
    let limit = list_deploys_request.get_limit();
    let list_result = db
        .list_deploy_aggregate(build_deploy_aggregate_filter(list_deploys_request))
        .await
        .map(|tuple| Page {
            data: tuple.0,
            item_count: tuple.1,
            limit,
            offset,
        });
    let final_result = format_or_reject_storage_result(list_result);
    let millis = start.elapsed().as_millis();
    metrics::LIST_DEPLOYS.with_label_values(&["aaa_full_response"]).observe(millis as f64);
    final_result
}

pub(super) async fn get_deploy_accepted_by_hash<Db: DatabaseReader + Clone + Send>(
    hash: String,
    db: Db,
) -> Result<impl Reply, Rejection> {
    check_hash_is_correct_format(&hash)?;
    let db_result = db.get_deploy_accepted_by_hash(&hash).await;
    format_or_reject_storage_result(db_result)
}

pub(super) async fn get_deploy_processed_by_hash<Db: DatabaseReader + Clone + Send>(
    hash: String,
    db: Db,
) -> Result<impl Reply, Rejection> {
    check_hash_is_correct_format(&hash)?;
    let db_result = db.get_deploy_processed_by_hash(&hash).await;
    format_or_reject_storage_result(db_result)
}

pub(super) async fn get_deploy_expired_by_hash<Db: DatabaseReader + Clone + Send>(
    hash: String,
    db: Db,
) -> Result<impl Reply, Rejection> {
    check_hash_is_correct_format(&hash)?;
    let db_result = db.get_deploy_expired_by_hash(&hash).await;
    format_or_reject_storage_result(db_result)
}

pub(super) async fn get_step_by_era<Db: DatabaseReader + Clone + Send>(
    era_id: u64,
    db: Db,
) -> Result<impl Reply, Rejection> {
    let db_result = db.get_step_by_era(era_id).await;
    format_or_reject_storage_result(db_result)
}

pub(super) async fn get_faults_by_public_key<Db: DatabaseReader + Clone + Send>(
    public_key: String,
    db: Db,
) -> Result<impl Reply, Rejection> {
    check_public_key_is_correct_format(&public_key)?;
    let db_result = db.get_faults_by_public_key(&public_key).await;
    format_or_reject_storage_result(db_result)
}

pub(super) async fn get_faults_by_era<Db: DatabaseReader + Clone + Send>(
    era: u64,
    db: Db,
) -> Result<impl Reply, Rejection> {
    let db_result = db.get_faults_by_era(era).await;
    format_or_reject_storage_result(db_result)
}

pub(super) async fn get_finality_signatures_by_block<Db: DatabaseReader + Clone + Send>(
    block_hash: String,
    db: Db,
) -> Result<impl Reply, Rejection> {
    check_hash_is_correct_format(&block_hash)?;
    let db_result = db.get_finality_signatures_by_block(&block_hash).await;
    format_or_reject_storage_result(db_result)
}

fn format_or_reject_storage_result<T>(
    storage_result: Result<T, DatabaseReadError>,
) -> Result<impl Reply, Rejection>
where
    T: Serialize,
{
    match storage_result {
        Ok(data) => {
            let start = Instant::now();
            let json = warp::reply::json(&data);
            let millis = start.elapsed().as_millis();
            metrics::LIST_DEPLOYS.with_label_values(&["final_deserialization"]).observe(millis as f64);
            Ok(warp::reply::with_status(json, StatusCode::OK).into_response())
        }
        Err(req_err) => Err(warp::reject::custom(StorageError(req_err))),
    }
}

pub async fn metrics_handler() -> Result<impl Reply, Rejection> {
    let res_custom = metrics_summary()
        .map_err(|err| warp::reject::custom(Unexpected(Error::msg(err.to_string()))))?;

    Ok(res_custom)
}

fn check_hash_is_correct_format(hash: &str) -> Result<(), Rejection> {
    let hash_regex = regex::Regex::new("^([0-9A-Fa-f]){64}$")
        .map_err(|err| warp::reject::custom(Unexpected(err.into())))?;
    if !hash_regex.is_match(hash) {
        return Err(warp::reject::custom(InvalidParam(Error::msg(format!(
            "Expected hex-encoded hash (64 chars), received: {} (length: {})",
            hash,
            hash.len()
        )))));
    }
    Ok(())
}

fn check_public_key_is_correct_format(public_key_hex: &str) -> Result<(), Rejection> {
    let public_key_regex = regex::Regex::new("^([0-9A-Fa-f]{2}){33,34}$")
        .map_err(|err| warp::reject::custom(Unexpected(err.into())))?;
    if !public_key_regex.is_match(public_key_hex) {
        return Err(warp::reject::custom(InvalidParam(Error::msg(format!(
            "Expected hex-encoded public key (66/68 chars), received: {} (length: {})",
            public_key_hex,
            public_key_hex.len()
        )))));
    }
    Ok(())
}

fn build_deploy_aggregate_filter(request: ListDeploysRequest) -> DeployAggregateFilter {
    let offset = request.offset.unwrap_or(0);
    let limit = request.get_limit();
    let exclude_not_processed = request.exclude_not_processed.unwrap_or(false);
    let exclude_expired = request.exclude_expired.unwrap_or(false);

    DeployAggregateFilter {
        exclude_expired,
        exclude_not_processed,
        limit,
        offset,
        sort_column: request.sort_column,
        sort_order: request.sort_order,
    }
}
