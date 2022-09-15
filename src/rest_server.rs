use std::convert::Infallible;
use std::path::PathBuf;

use anyhow::Error;
use serde::Serialize;
use tracing::error;
use warp::http::StatusCode;
use warp::{Rejection, Reply};

use crate::{database::DatabaseRequestError, sqlite_db::SqliteDb, utils::resolve_address};

mod filters {
    use std::convert::Infallible;

    use warp::Filter;

    use crate::{
        database::DatabaseReader,
        rest_server::{handle_rejection, handlers, InvalidPath},
    };

    pub(super) fn combined_filters<Db: DatabaseReader + Clone + Send + Sync>(
        db: Db,
    ) -> impl Filter<Extract = impl warp::Reply, Error = Infallible> + Clone {
        root_filter()
            .or(root_and_invalid_path())
            .or(block_filters(db.clone()))
            .or(deploy_filters(db.clone()))
            .or(step_by_era(db.clone()))
            .or(faults_by_public_key(db.clone()))
            .or(faults_by_era(db.clone()))
            .or(finality_signatures_by_block(db))
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

    pub(super) fn faults_by_public_key<Db: DatabaseReader + Clone + Send + Sync>(
        db: Db,
    ) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        warp::path!("fault" / String)
            .and(warp::get())
            .and(with_db(db))
            .and_then(handlers::get_faults_by_public_key)
    }

    pub(super) fn faults_by_era<Db: DatabaseReader + Clone + Send + Sync>(
        db: Db,
    ) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        warp::path!("fault" / u64)
            .and(warp::get())
            .and(with_db(db))
            .and_then(handlers::get_faults_by_era)
    }

    pub(super) fn finality_signatures_by_block<Db: DatabaseReader + Clone + Send + Sync>(
        db: Db,
    ) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        warp::path!("signatures" / String)
            .and(warp::get())
            .and(with_db(db))
            .and_then(handlers::get_finality_signatures_by_block)
    }

    fn with_db<Db: DatabaseReader + Clone + Send>(
        db: Db,
    ) -> impl Filter<Extract = (Db,), Error = Infallible> + Clone {
        warp::any().map(move || db.clone())
    }
}

mod handlers {
    use warp::{Rejection, Reply};

    use crate::{database::DatabaseReader, rest_server::format_or_reject_storage_result};

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
        let db_result = db.get_deploy_by_hash_aggregate(&hash).await;
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
}

pub async fn run_server(db_path: PathBuf, ip_address: String, port: u16) -> Result<(), Error> {
    let db = SqliteDb::new_read_only(&db_path)?;

    let api = filters::combined_filters(db);

    let address = format!("{}:{}", ip_address, port);
    let socket_address = resolve_address(&address)?;

    warp::serve(api).run(socket_address).await;

    Ok(())
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
    use std::path::Path;

    use bytes::Bytes;
    use http::{Response, StatusCode};
    use warp::test::request;

    use casper_node::types::FinalitySignature as FinSig;
    use casper_types::AsymmetricType;

    use crate::{
        database::AggregateDeployInfo,
        rest_server::filters,
        sqlite_db::SqliteDb,
        types::sse_events::{BlockAdded, DeployAccepted, DeployProcessed, Fault, Step},
    };

    const NOT_STORED_HASH: &str =
        "0bcd71363b01c1c147c1603d2cc945930dcceecd869275beeee61dfc83b27a2c";
    const NOT_STORED_HEIGHT: u64 = 2304;
    const NOT_STORED_STEP_ERA_ID: u64 = 2304;

    const STORED_BLOCK_HASH: &str =
        "a83aee61fddc50b039bc2879cc19fc206db59ec673d41fdd0facc627f800d515";
    const STORED_BLOCK_HEIGHT: u64 = 49;
    const STORED_ACCEPTED_PROCESSED_DEPLOY_HASH: &str =
        "ce0e3468df750d3b4297f1510e6c0770a355374d70ac040c5ea7e72680d1b785";
    const STORED_EXPIRED_DEPLOY_HASH: &str =
        "339980e662fed93e6d54f5832208ca75cb1a219c6efabb2e442681ba74c11c3d";
    const STORED_STEP_ERA_ID: u64 = 5;
    const STORED_FAULT_PUBLIC_KEY: &str =
        "014d3f346385e22857737177c27326affbe696f621536ba14c73c0b81b9e94e61c";
    const STORED_FAULT_ERA_ID: u64 = 16;
    const STORED_BLOCK_HASH_FOR_FIN_SIGS: &str =
        "227b4c2896247dded47ca9e5924d5a6dcc8c398a9ea10d52f076cf61bbb428c3";

    const VALID_PUBLIC_KEY: &str =
        "01a601840126a0363a6048bfcbb0492ab5a313a1a19dc4c695650d8f3b51302703";
    const INVALID_HASH: &str = "notablockhash";

    async fn get_response_from_path(path: &str) -> Response<Bytes> {
        let db_path = Path::new("src/testing/");
        let db = SqliteDb::new(
            db_path,
            "sqlite_database.db3".to_string(),
            "127.0.0.1".to_string(),
        )
        .await
        .unwrap();

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
        serde_json::from_slice::<BlockAdded>(&body).unwrap();
    }

    #[tokio::test]
    async fn block_by_hash_should_return_valid_data() {
        let path = format!("/block/{}", STORED_BLOCK_HASH);
        let response = get_response_from_path(&path).await;
        assert!(response.status().is_success());
        let body = response.into_body();
        let value = serde_json::from_slice::<BlockAdded>(&body);
        assert!(value.is_ok());
        assert_eq!(value.unwrap().hex_encoded_hash(), STORED_BLOCK_HASH);
    }

    #[tokio::test]
    async fn block_by_height_should_return_valid_data() {
        let path = format!("/block/{}", STORED_BLOCK_HEIGHT);
        let response = get_response_from_path(&path).await;
        assert!(response.status().is_success());
        let body = response.into_body();
        let value = serde_json::from_slice::<BlockAdded>(&body);
        assert!(value.is_ok());
        assert_eq!(value.unwrap().get_height(), STORED_BLOCK_HEIGHT);
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
        let path = format!("/deploy/{}", STORED_ACCEPTED_PROCESSED_DEPLOY_HASH);
        let response = get_response_from_path(&path).await;
        assert!(response.status().is_success());
        let body = response.into_body();
        let value = serde_json::from_slice::<AggregateDeployInfo>(&body);
        assert!(value.is_ok());
        assert_eq!(
            value.unwrap().deploy_hash,
            STORED_ACCEPTED_PROCESSED_DEPLOY_HASH
        );
    }

    #[tokio::test]
    async fn deploy_accepted_by_hash_should_return_valid_data() {
        let path = format!("/deploy/accepted/{}", STORED_ACCEPTED_PROCESSED_DEPLOY_HASH);
        let response = get_response_from_path(&path).await;
        assert!(response.status().is_success());
        let body = response.into_body();
        let value = serde_json::from_slice::<DeployAccepted>(&body);
        assert!(value.is_ok());
        assert_eq!(
            value.unwrap().hex_encoded_hash(),
            STORED_ACCEPTED_PROCESSED_DEPLOY_HASH
        );
    }

    #[tokio::test]
    async fn deploy_processed_by_hash_should_return_valid_data() {
        let path = format!(
            "/deploy/processed/{}",
            STORED_ACCEPTED_PROCESSED_DEPLOY_HASH
        );
        let response = get_response_from_path(&path).await;
        assert!(response.status().is_success());
        let body = response.into_body();
        let value = serde_json::from_slice::<DeployProcessed>(&body);
        assert!(value.is_ok());
        assert_eq!(
            value.unwrap().hex_encoded_hash(),
            STORED_ACCEPTED_PROCESSED_DEPLOY_HASH
        );
    }

    #[tokio::test]
    async fn deploy_expired_by_hash_should_return_valid_data() {
        let path = format!("/deploy/expired/{}", STORED_EXPIRED_DEPLOY_HASH);
        let response = get_response_from_path(&path).await;
        assert!(response.status().is_success());
        let body = response.into_body();
        let value = serde_json::from_slice::<bool>(&body);
        assert!(value.unwrap());
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
    async fn fault_by_public_key_should_return_valid_data() {
        let path = format!("/fault/{}", STORED_FAULT_PUBLIC_KEY);
        let response = get_response_from_path(&path).await;
        assert!(response.status().is_success());
        let body = response.into_body();
        let value = serde_json::from_slice::<Vec<Fault>>(&body);
        assert!(value.is_ok());
        assert_eq!(
            value.unwrap().get(0).unwrap().public_key.to_hex(),
            STORED_FAULT_PUBLIC_KEY
        );
    }

    #[tokio::test]
    async fn fault_by_era_should_return_valid_data() {
        let path = format!("/fault/{}", STORED_FAULT_ERA_ID);
        let response = get_response_from_path(&path).await;
        assert!(response.status().is_success());
        let body = response.into_body();
        let value = serde_json::from_slice::<Vec<Fault>>(&body);
        assert!(value.is_ok());
        assert_eq!(
            value.unwrap().get(0).unwrap().era_id.value(),
            STORED_FAULT_ERA_ID
        );
    }

    #[tokio::test]
    async fn finality_signatures_by_block_should_return_valid_data() {
        let path = format!("/signatures/{}", STORED_BLOCK_HASH_FOR_FIN_SIGS);
        let response = get_response_from_path(&path).await;
        assert!(response.status().is_success());
        let body = response.into_body();
        let value = serde_json::from_slice::<Vec<FinSig>>(&body);
        assert!(value.is_ok());
        assert_eq!(
            hex::encode(value.unwrap().get(0).unwrap().block_hash.inner()),
            STORED_BLOCK_HASH_FOR_FIN_SIGS
        );
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
    async fn fault_by_public_key_of_not_stored_should_return_404() {
        let path = format!("/fault/{}", VALID_PUBLIC_KEY);
        let response = get_response_from_path(&path).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn fault_by_era_of_not_stored_should_return_404() {
        let path = format!("/fault/{}", 30);
        let response = get_response_from_path(&path).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn fault_by_invalid_public_key_should_return_400() {
        let path = "/fault/notapublickey".to_string();
        let response = get_response_from_path(&path).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn finality_signature_of_not_stored_should_return_404() {
        let path = format!("/signatures/{}", NOT_STORED_HASH);
        let response = get_response_from_path(&path).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn finality_signature_by_invalid_block_hash_should_return_400() {
        let path = format!("/signatures/{}", INVALID_HASH);
        let response = get_response_from_path(&path).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn should_have_correct_content_type() {
        let response = get_response_from_path("/block").await;
        assert_eq!(
            response.headers().get("content-type").unwrap(),
            "application/json"
        );
    }
}
