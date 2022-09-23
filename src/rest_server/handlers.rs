use serde::Serialize;
use warp::{http::StatusCode, Rejection, Reply};

use super::errors::StorageError;
use crate::types::database::{DatabaseReader, DatabaseRequestError};

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

pub(super) async fn get_latest_deploy<Db: DatabaseReader + Clone + Send>(
    db: Db,
) -> Result<impl Reply, Rejection> {
    let db_result = db.get_latest_deploy_aggregate().await;
    format_or_reject_storage_result(db_result)
}

pub(super) async fn get_deploy_by_hash<Db: DatabaseReader + Clone + Send>(
    hash: String,
    db: Db,
) -> Result<impl Reply, Rejection> {
    let db_result = db.get_deploy_aggregate_by_hash(&hash).await;
    format_or_reject_storage_result(db_result)
}

pub(super) async fn get_deploy_accepted_by_hash<Db: DatabaseReader + Clone + Send>(
    hash: String,
    db: Db,
) -> Result<impl Reply, Rejection> {
    let db_result = db.get_deploy_accepted_by_hash(&hash).await;
    format_or_reject_storage_result(db_result)
}

pub(super) async fn get_deploy_processed_by_hash<Db: DatabaseReader + Clone + Send>(
    hash: String,
    db: Db,
) -> Result<impl Reply, Rejection> {
    let db_result = db.get_deploy_processed_by_hash(&hash).await;
    format_or_reject_storage_result(db_result)
}

pub(super) async fn get_deploy_expired_by_hash<Db: DatabaseReader + Clone + Send>(
    hash: String,
    db: Db,
) -> Result<impl Reply, Rejection> {
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
    let db_result = db.get_finality_signatures_by_block(&block_hash).await;
    format_or_reject_storage_result(db_result)
}

fn format_or_reject_storage_result<T>(
    storage_result: Result<T, DatabaseRequestError>,
) -> Result<impl Reply, Rejection>
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
