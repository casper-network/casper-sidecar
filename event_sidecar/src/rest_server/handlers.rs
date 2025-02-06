use super::{errors::StorageError, filters::TransactionTypeIdFilter};
use crate::{
    rest_server::errors::InvalidParam,
    types::database::{DatabaseReadError, DatabaseReader},
    utils::Unexpected,
};
use anyhow::Error;
use http::Response;
use hyper::Body;
use serde::Serialize;
use warp::{http::StatusCode, Rejection, Reply};

pub(super) async fn get_latest_block<Db: DatabaseReader + Clone + Send>(
    db: Db,
) -> Result<Response<Body>, Rejection> {
    let db_result = db.get_latest_block().await;
    format_or_reject_storage_result(db_result)
}

pub(super) async fn get_block_by_hash<Db: DatabaseReader + Clone + Send>(
    hash: String,
    db: Db,
) -> Result<Response<Body>, Rejection> {
    check_hash_is_correct_format(&hash)?;
    let db_result = db.get_block_by_hash(&hash).await;
    format_or_reject_storage_result(db_result)
}

pub(super) async fn get_block_by_height<Db: DatabaseReader + Clone + Send>(
    height: u64,
    db: Db,
) -> Result<Response<Body>, Rejection> {
    let db_result = db.get_block_by_height(height).await;
    format_or_reject_storage_result(db_result)
}

pub(super) async fn get_transaction_by_identifier<Db: DatabaseReader + Clone + Send>(
    transaction_type: TransactionTypeIdFilter,
    hash: String,
    db: Db,
) -> Result<Response<Body>, Rejection> {
    check_hash_is_correct_format(&hash)?;
    let db_result = db
        .get_transaction_aggregate_by_identifier(&transaction_type.into(), &hash)
        .await;
    format_or_reject_storage_result(db_result)
}

pub(super) async fn get_transaction_accepted_by_hash<Db: DatabaseReader + Clone + Send>(
    transaction_type: TransactionTypeIdFilter,
    hash: String,
    db: Db,
) -> Result<Response<Body>, Rejection> {
    check_hash_is_correct_format(&hash)?;
    let db_result = db
        .get_transaction_accepted_by_hash(&transaction_type.into(), &hash)
        .await;
    format_or_reject_storage_result(db_result)
}

pub(super) async fn get_transaction_processed_by_hash<Db: DatabaseReader + Clone + Send>(
    transaction_type: TransactionTypeIdFilter,
    hash: String,
    db: Db,
) -> Result<impl Reply, Rejection> {
    check_hash_is_correct_format(&hash)?;
    let db_result = db
        .get_transaction_processed_by_hash(&transaction_type.into(), &hash)
        .await;
    format_or_reject_storage_result(db_result)
}

pub(super) async fn get_transaction_expired_by_hash<Db: DatabaseReader + Clone + Send>(
    transaction_type: TransactionTypeIdFilter,
    hash: String,
    db: Db,
) -> Result<impl Reply, Rejection> {
    check_hash_is_correct_format(&hash)?;
    let db_result = db
        .get_transaction_expired_by_hash(&transaction_type.into(), &hash)
        .await;
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
) -> Result<Response<Body>, Rejection>
where
    T: Serialize,
{
    match storage_result {
        Ok(data) => {
            let json = warp::reply::json(&data);
            Ok(warp::reply::with_status(json, StatusCode::OK).into_response())
        }
        Err(req_err) => Err(warp::reject::custom(StorageError(req_err))),
    }
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
