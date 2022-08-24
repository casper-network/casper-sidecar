use crate::sqlite_db::{DatabaseRequestError, SqliteDb};
use crate::utils::{resolve_address, ListeningError};
use anyhow::Error;
use serde::Serialize;
use std::convert::Infallible;
use std::future::Future;
use std::path::PathBuf;
use tokio::sync::oneshot;
use tracing::{error, info, warn};
use warp::http::StatusCode;
use warp::{Rejection, Reply};

mod filters {
    use crate::rest_server::{handle_rejection, handlers, InvalidPath};
    use crate::sqlite_db::DatabaseReader;
    use std::convert::Infallible;
    use warp::Filter;

    pub(super) fn combined_filters<Db: DatabaseReader + Clone + Send + Sync>(
        db: Db,
    ) -> impl Filter<Extract = impl warp::Reply, Error = Infallible> + Clone {
        root_filter()
            .or(root_and_invalid_path())
            .or(block_filters(db.clone()))
            .or(deploy_filters(db.clone()))
            .or(step_by_era(db.clone()))
            .or(fault_by_public_key(db))
            .recover(handle_rejection)
    }

    pub(super) fn root_filter(
    ) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        warp::path::end().and_then(|| async {
            Err::<String, warp::Rejection>(warp::reject::custom(InvalidPath))
        })
    }

    pub fn root_and_invalid_path(
    ) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        warp::path::param()
            .and(warp::path::end())
            .and_then(|_param: String| async {
                Err::<String, warp::Rejection>(warp::reject::custom(InvalidPath))
            })
    }

    pub(super) fn block_filters<Db: DatabaseReader + Clone + Send + Sync>(
        db: Db,
    ) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        latest_block(db.clone())
            .or(block_by_hash(db.clone()))
            .or(block_by_height(db))
    }

    pub(super) fn deploy_filters<Db: DatabaseReader + Clone + Send + Sync>(
        db: Db,
    ) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        latest_deploy(db.clone())
            .or(deploy_by_hash(db.clone()))
            .or(deploy_accepted_by_hash(db.clone()))
            .or(deploy_processed_by_hash(db.clone()))
            .or(deploy_expired_by_hash(db))
    }

    pub(super) fn latest_block<Db: DatabaseReader + Clone + Send + Sync>(
        db: Db,
    ) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        warp::path!("block")
            .and(warp::get())
            .and(with_db(db))
            .and_then(handlers::get_latest_block)
    }

    pub(super) fn block_by_hash<Db: DatabaseReader + Clone + Send + Sync>(
        db: Db,
    ) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        warp::path!("block" / String)
            .and(warp::get())
            .and(with_db(db))
            .and_then(handlers::get_block_by_hash)
    }

    pub(super) fn block_by_height<Db: DatabaseReader + Clone + Send + Sync>(
        db: Db,
    ) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        warp::path!("block" / u64)
            .and(warp::get())
            .and(with_db(db))
            .and_then(handlers::get_block_by_height)
    }

    pub(super) fn latest_deploy<Db: DatabaseReader + Clone + Send + Sync>(
        db: Db,
    ) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        warp::path!("deploy")
            .and(warp::get())
            .and(with_db(db))
            .and_then(handlers::get_latest_deploy)
    }

    pub(super) fn deploy_by_hash<Db: DatabaseReader + Clone + Send + Sync>(
        db: Db,
    ) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        warp::path!("deploy" / String)
            .and(warp::get())
            .and(with_db(db))
            .and_then(handlers::get_deploy_by_hash)
    }

    pub(super) fn deploy_accepted_by_hash<Db: DatabaseReader + Clone + Send + Sync>(
        db: Db,
    ) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        warp::path!("deploy" / "accepted" / String)
            .and(warp::get())
            .and(with_db(db))
            .and_then(handlers::get_deploy_accepted_by_hash)
    }

    pub(super) fn deploy_processed_by_hash<Db: DatabaseReader + Clone + Send + Sync>(
        db: Db,
    ) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        warp::path!("deploy" / "processed" / String)
            .and(warp::get())
            .and(with_db(db))
            .and_then(handlers::get_deploy_processed_by_hash)
    }

    pub(super) fn deploy_expired_by_hash<Db: DatabaseReader + Clone + Send + Sync>(
        db: Db,
    ) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        warp::path!("deploy" / "expired" / String)
            .and(warp::get())
            .and(with_db(db))
            .and_then(handlers::get_deploy_expired_by_hash)
    }

    pub(super) fn step_by_era<Db: DatabaseReader + Clone + Send + Sync>(
        db: Db,
    ) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        warp::path!("step" / u64)
            .and(warp::get())
            .and(with_db(db))
            .and_then(handlers::get_step_by_era)
    }

    pub(super) fn fault_by_public_key<Db: DatabaseReader + Clone + Send + Sync>(
        db: Db,
    ) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        warp::path!("fault" / String)
            .and(warp::get())
            .and(with_db(db))
            .and_then(handlers::get_fault_by_public_key)
    }

    fn with_db<Db: DatabaseReader + Clone + Send>(
        db: Db,
    ) -> impl Filter<Extract = (Db,), Error = Infallible> + Clone {
        warp::any().map(move || db.clone())
    }
}

mod handlers {
    use crate::rest_server::serialize_or_reject_storage_result;
    use crate::sqlite_db::DatabaseReader;
    use warp::{Rejection, Reply};

    pub(super) async fn get_latest_block<Db: DatabaseReader + Clone + Send>(
        db: Db,
    ) -> Result<impl Reply, Rejection> {
        let db_result = db.get_latest_block().await;
        serialize_or_reject_storage_result(db_result)
    }

    pub(super) async fn get_block_by_hash<Db: DatabaseReader + Clone + Send>(
        hash: String,
        db: Db,
    ) -> Result<impl Reply, Rejection> {
        let db_result = db.get_block_by_hash(&hash).await;
        serialize_or_reject_storage_result(db_result)
    }

    pub(super) async fn get_block_by_height<Db: DatabaseReader + Clone + Send>(
        height: u64,
        db: Db,
    ) -> Result<impl Reply, Rejection> {
        let db_result = db.get_block_by_height(height).await;
        serialize_or_reject_storage_result(db_result)
    }

    pub(super) async fn get_latest_deploy<Db: DatabaseReader + Clone + Send>(
        db: Db,
    ) -> Result<impl Reply, Rejection> {
        let db_result = db.get_latest_deploy().await;
        serialize_or_reject_storage_result(db_result)
    }

    pub(super) async fn get_deploy_by_hash<Db: DatabaseReader + Clone + Send>(
        hash: String,
        db: Db,
    ) -> Result<impl Reply, Rejection> {
        let db_result = db.get_deploy_by_hash(&hash).await;
        serialize_or_reject_storage_result(db_result)
    }

    pub(super) async fn get_deploy_accepted_by_hash<Db: DatabaseReader + Clone + Send>(
        hash: String,
        db: Db,
    ) -> Result<impl Reply, Rejection> {
        let db_result = db.get_deploy_accepted_by_hash(&hash).await;
        serialize_or_reject_storage_result(db_result)
    }

    pub(super) async fn get_deploy_processed_by_hash<Db: DatabaseReader + Clone + Send>(
        hash: String,
        db: Db,
    ) -> Result<impl Reply, Rejection> {
        let db_result = db.get_deploy_processed_by_hash(&hash).await;
        serialize_or_reject_storage_result(db_result)
    }

    pub(super) async fn get_deploy_expired_by_hash<Db: DatabaseReader + Clone + Send>(
        hash: String,
        db: Db,
    ) -> Result<impl Reply, Rejection> {
        let db_result = db.get_deploy_expired_by_hash(&hash).await;
        serialize_or_reject_storage_result(db_result)
    }

    pub(super) async fn get_step_by_era<Db: DatabaseReader + Clone + Send>(
        era_id: u64,
        db: Db,
    ) -> Result<impl Reply, Rejection> {
        let db_result = db.get_step_by_era(era_id).await;
        serialize_or_reject_storage_result(db_result)
    }

    pub(super) async fn get_fault_by_public_key<Db: DatabaseReader + Clone + Send>(
        public_key: String,
        db: Db,
    ) -> Result<impl Reply, Rejection> {
        let db_result = db.get_fault_by_public_key(&public_key).await;
        serialize_or_reject_storage_result(db_result)
    }
}

pub async fn run_server(
    db_path: PathBuf,
    ip_address: String,
    port: u16,
) -> Result<impl Future<Output = ()> + Sized, Error> {
    let db = SqliteDb::new_read_only(&db_path)?;

    let api = filters::combined_filters(db);

    let addr_string = format!("{}:{}", ip_address, port);
    let bind_address = resolve_address(&addr_string)?;
    info!(message = "starting REST server", address = %bind_address);

    let (_shutdown_sender, shutdown_receiver) = oneshot::channel::<()>();

    let (listening_address, server_with_shutdown) = warp::serve(api)
        .try_bind_with_graceful_shutdown(bind_address, async {
            shutdown_receiver.await.ok();
        })
        .map_err(|error| ListeningError::Listen {
            address: bind_address,
            error: Box::new(error),
        })?;
    info!(address=%listening_address, "started REST server");

    Ok(server_with_shutdown)
}

fn serialize_or_reject_storage_result<T>(
    storage_result: Result<T, DatabaseRequestError>,
) -> Result<impl Reply, Rejection>
where
    T: Serialize,
{
    match storage_result {
        Ok(data) => match serde_json::to_string_pretty(&data) {
            Ok(pretty_json) => {
                Ok(warp::reply::with_status(pretty_json, StatusCode::OK).into_response())
            }
            Err(err) => Err(warp::reject::custom(SerializationError(err))),
        },
        Err(err) => Err(warp::reject::custom(StorageError(err))),
    }
}

#[derive(Serialize)]
struct ApiError {
    code: u16,
    message: String,
}

#[derive(Debug)]
struct InvalidPath;
impl warp::reject::Reject for InvalidPath {}

#[derive(Debug)]
struct SerializationError(serde_json::error::Error);
impl warp::reject::Reject for SerializationError {}

#[derive(Debug)]
struct StorageError(DatabaseRequestError);
impl warp::reject::Reject for StorageError {}

async fn handle_rejection(err: Rejection) -> Result<impl Reply, Infallible> {
    let code;
    let message;

    if err.is_not_found() {
        code = StatusCode::NOT_FOUND;
        message = String::from("No results");
    } else if let Some(SerializationError(err)) = err.find() {
        code = StatusCode::INTERNAL_SERVER_ERROR;
        message = format!("Serialization Error: {}", err);
    } else if let Some(StorageError(err)) = err.find() {
        match err {
            DatabaseRequestError::DBConnectionFailed(err) => {
                warn!(message = format!("Sqlite DB error processing request: {}", err).as_str());
                code = StatusCode::INTERNAL_SERVER_ERROR;
                message = format!("Failed to connect to DB instance: {}", err)
            }
            DatabaseRequestError::NotFound => {
                code = StatusCode::NOT_FOUND;
                message = "Query returned no data".to_string();
            }
            DatabaseRequestError::InvalidParam(err) => {
                code = StatusCode::BAD_REQUEST;
                message = format!("Invalid parameter in query: {}", err)
            }
            DatabaseRequestError::Serialisation(err) => {
                code = StatusCode::INTERNAL_SERVER_ERROR;
                message = format!("Error deserializing returned data: {}", err)
            }
            DatabaseRequestError::Unhandled(err) => {
                code = StatusCode::INTERNAL_SERVER_ERROR;
                message = format!("Unhandled error occurred in storage: {}", err)
            }
        }
    } else if let Some(InvalidPath) = err.find() {
        code = StatusCode::BAD_REQUEST;
        message = String::from("Invalid request path provided");
    } else if err.find::<warp::reject::MethodNotAllowed>().is_some() {
        code = StatusCode::METHOD_NOT_ALLOWED;
        message = "Method not allowed".to_string();
    } else {
        error!("Unhandled REST Server Error: {:?}", err);
        code = StatusCode::INTERNAL_SERVER_ERROR;
        message = "Unhandled error - you've found a bug!".to_string();
    }

    let json = warp::reply::json(&ApiError {
        code: code.as_u16(),
        message,
    });

    Ok(warp::reply::with_status(json, code))
}

#[cfg(test)]
mod tests {
    use crate::rest_server::filters;
    use crate::sqlite_db::AggregateDeployInfo;
    use crate::{sqlite_db, DeployProcessed, Fault, Step};
    use bytes::Bytes;
    use casper_node::types::{Block, Deploy};
    use casper_types::AsymmetricType;
    use http::{Response, StatusCode};
    use std::path::Path;
    use warp::test::request;

    const NOT_STORED_HASH: &str =
        "0bcd71363b01c1c147c1603d2cc945930dcceecd869275beeee61dfc83b27a2c";
    const NOT_STORED_HEIGHT: u64 = 2304;
    const NOT_STORED_STEP_ERA_ID: u64 = 2304;

    const STORED_BLOCK_HASH: &str =
        "d17f834afbc8a675e42f0ee3af9aed5e6759a79a3bc4bd4bd7acc3c8d9332b5a";
    const STORED_BLOCK_HEIGHT: u64 = 23;
    const STORED_DEPLOY_HASH: &str =
        "5e1f28c650e371ae912616ea409e438123a7f0fcceaa1b67d543c7c9d181288d";
    const STORED_STEP_ERA_ID: u64 = 3;
    const STORED_FAULT_PUBLIC_KEY: &str =
        "01a9bde8aee48a1a47d00c0892f5a25a2cda366d7df14147827e42920e48842785";

    const VALID_PUBLIC_KEY: &str =
        "01a601840126a0363a6048bfcbb0492ab5a313a1a19dc4c695650d8f3b51302703";
    const INVALID_HASH: &str = "notablockhash";

    async fn get_response_from_path(path: &str) -> Response<Bytes> {
        let db_path = Path::new("src/testing/");
        let db = sqlite_db::SqliteDb::new(db_path).unwrap();

        let api = filters::combined_filters(db);

        request().path(path).reply(&api).await
    }

    #[tokio::test]
    async fn root_should_return_400() {
        let response = get_response_from_path("/").await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn root_with_invalid_path_should_return_400() {
        let response = get_response_from_path("/notblockordeploy").await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn block_root_should_return_valid_data() {
        let response = get_response_from_path("/block").await;
        assert!(response.status().is_success());
        let body = response.into_body();
        let value = serde_json::from_slice::<Block>(&body);
        assert!(value.is_ok());
    }

    #[tokio::test]
    async fn block_by_hash_should_return_valid_data() {
        let path = format!("/block/{}", STORED_BLOCK_HASH);
        let response = get_response_from_path(&path).await;
        assert!(response.status().is_success());
        let body = response.into_body();
        let value = serde_json::from_slice::<Block>(&body);
        assert!(value.is_ok());
        assert_eq!(
            hex::encode(value.unwrap().hash().inner()),
            STORED_BLOCK_HASH
        );
    }

    #[tokio::test]
    async fn block_by_height_should_return_valid_data() {
        let path = format!("/block/{}", STORED_BLOCK_HEIGHT);
        let response = get_response_from_path(&path).await;
        assert!(response.status().is_success());
        let body = response.into_body();
        let value = serde_json::from_slice::<Block>(&body);
        assert!(value.is_ok());
        assert_eq!(value.unwrap().height(), STORED_BLOCK_HEIGHT);
    }

    #[tokio::test]
    async fn deploy_root_should_return_valid_data() {
        let response = get_response_from_path("/deploy").await;
        assert!(response.status().is_success());
        let body = response.into_body();
        let value = serde_json::from_slice::<AggregateDeployInfo>(&body);
        assert!(value.is_ok());
    }

    #[tokio::test]
    async fn deploy_by_hash_should_return_valid_data() {
        let path = format!("/deploy/{}", STORED_DEPLOY_HASH);
        let response = get_response_from_path(&path).await;
        assert!(response.status().is_success());
        let body = response.into_body();
        let value = serde_json::from_slice::<AggregateDeployInfo>(&body);
        assert!(value.is_ok());
        assert_eq!(value.unwrap().deploy_hash, STORED_DEPLOY_HASH);
    }

    #[tokio::test]
    async fn deploy_accepted_by_hash_should_return_valid_data() {
        let path = format!("/deploy/accepted/{}", STORED_DEPLOY_HASH);
        let response = get_response_from_path(&path).await;
        assert!(response.status().is_success());
        let body = response.into_body();
        let value = serde_json::from_slice::<Deploy>(&body);
        assert!(value.is_ok());
        assert_eq!(hex::encode(value.unwrap().id()), STORED_DEPLOY_HASH);
    }

    #[tokio::test]
    async fn deploy_processed_by_hash_should_return_valid_data() {
        let path = format!("/deploy/processed/{}", STORED_DEPLOY_HASH);
        let response = get_response_from_path(&path).await;
        assert!(response.status().is_success());
        let body = response.into_body();
        let value = serde_json::from_slice::<DeployProcessed>(&body);
        assert!(value.is_ok());
        assert_eq!(hex::encode(*value.unwrap().deploy_hash), STORED_DEPLOY_HASH);
    }

    #[tokio::test]
    async fn deploy_expired_by_hash_should_return_valid_data() {
        let path = format!("/deploy/expired/{}", STORED_DEPLOY_HASH);
        let response = get_response_from_path(&path).await;
        assert!(response.status().is_success());
        let body = response.into_body();
        let value = serde_json::from_slice::<bool>(&body);
        assert!(value.is_ok());
        assert_eq!(value.unwrap(), false);
    }

    #[tokio::test]
    async fn step_by_era_should_return_valid_data() {
        let path = format!("/step/{}", STORED_STEP_ERA_ID);
        let response = get_response_from_path(&path).await;
        assert!(response.status().is_success());
        let body = response.into_body();
        let value = serde_json::from_slice::<Step>(&body);
        assert!(value.is_ok());
        assert_eq!(value.unwrap().era_id.value(), STORED_STEP_ERA_ID);
    }

    #[tokio::test]
    async fn fault_should_return_valid_data() {
        let path = format!("/fault/{}", STORED_FAULT_PUBLIC_KEY);
        let response = get_response_from_path(&path).await;
        assert!(response.status().is_success());
        let body = response.into_body();
        let value = serde_json::from_slice::<Fault>(&body);
        assert!(value.is_ok());
        assert_eq!(value.unwrap().public_key.to_hex(), STORED_FAULT_PUBLIC_KEY);
    }

    #[tokio::test]
    async fn block_by_invalid_hash_should_return_400() {
        let path = format!("/block/{}", INVALID_HASH);
        let response = get_response_from_path(&path).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn block_by_hash_of_not_stored_should_return_404() {
        let path = format!("/block/{}", NOT_STORED_HASH);
        let response = get_response_from_path(&path).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn block_by_height_of_not_stored_should_return_404() {
        let path = format!("/block/{}", NOT_STORED_HEIGHT);
        let response = get_response_from_path(&path).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn deploy_by_hash_of_not_stored_should_return_404() {
        let path = format!("/deploy/{}", NOT_STORED_HASH);
        let response = get_response_from_path(&path).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn deploy_accepted_by_hash_of_not_stored_should_return_404() {
        let path = format!("/deploy/accepted/{}", NOT_STORED_HASH);
        let response = get_response_from_path(&path).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn deploy_processed_by_hash_of_not_stored_should_return_404() {
        let path = format!("/deploy/processed/{}", NOT_STORED_HASH);
        let response = get_response_from_path(&path).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn deploy_expired_by_hash_of_not_stored_should_return_404() {
        let path = format!("/deploy/expired/{}", NOT_STORED_HASH);
        let response = get_response_from_path(&path).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn deploy_by_hash_of_invalid_should_return_400() {
        let path = format!("/deploy/{}", INVALID_HASH);
        let response = get_response_from_path(&path).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn deploy_accepted_by_hash_of_invalid_should_return_400() {
        let path = format!("/deploy/accepted/{}", INVALID_HASH);
        let response = get_response_from_path(&path).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn deploy_processed_by_hash_of_invalid_should_return_400() {
        let path = format!("/deploy/processed/{}", INVALID_HASH);
        let response = get_response_from_path(&path).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn deploy_expired_by_hash_of_invalid_should_return_400() {
        let path = format!("/deploy/expired/{}", INVALID_HASH);
        let response = get_response_from_path(&path).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn step_by_era_of_not_stored_should_return_404() {
        let path = format!("/step/{}", NOT_STORED_STEP_ERA_ID);
        let response = get_response_from_path(&path).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn fault_of_not_stored_should_return_404() {
        let path = format!("/fault/{}", VALID_PUBLIC_KEY);
        let response = get_response_from_path(&path).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn fault_of_invalid_public_key_should_return_400() {
        let path = format!("/fault/notapublickey");
        let response = get_response_from_path(&path).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
}
