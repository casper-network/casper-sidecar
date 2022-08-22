use crate::sqlite_db::{DatabaseRequestError, SqliteDb};
use crate::utils::resolve_address;
use anyhow::Error;
use serde::Serialize;
use std::convert::Infallible;
use std::path::PathBuf;
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

pub async fn run_server(db_path: PathBuf, ip_address: String, port: u16) -> Result<(), Error> {
    let db = SqliteDb::new_read_only(&db_path)?;

    let api = filters::combined_filters(db);

    let addr_string = format!("{}:{}", ip_address, port);
    let bind_address = utils::resolve_address(&addr_string)?;
    info!(message = "starting REST server", address = %bind_address);

    warp::serve(api)
        .try_bind(bind_address).await;

    Ok(())
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
    use crate::sqlite_db;
    use bytes::Bytes;
    use http::{Response, StatusCode};
    use std::path::Path;
    use warp::test::request;

    // These are the codes that would be expected for a request to a valid path.
    const RESP_CODES_FOR_VALID_PATH: [StatusCode; 2] = [StatusCode::OK, StatusCode::NOT_FOUND];

    async fn get_response_from_path(path: &str) -> Response<Bytes> {
        let db_path = Path::new("target/storage");
        let db = sqlite_db::SqliteDb::new(db_path).unwrap();

        let api = filters::combined_filters(db);

        request().path(path).reply(&api).await
    }

    #[tokio::test]
    async fn valid_paths_should_respond_with_correct_status() {
        let valid_paths = [
            "/block",
            "/block/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "/block/100",
            "/deploy",
            "/deploy/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "/deploy/accepted/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "/deploy/processed/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "/deploy/expired/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "/step/1",
            "/fault/publickey",
        ];

        for path in valid_paths {
            let response = get_response_from_path(path).await;
            assert!(RESP_CODES_FOR_VALID_PATH.contains(&response.status()));
        }
    }

    #[tokio::test]
    async fn invalid_paths_should_respond_with_correct_status() {
        let invalid_paths = ["/", "/foo"];

        for path in invalid_paths {
            let response = get_response_from_path(path).await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        }
    }

    #[tokio::test]
    async fn invalid_params_should_respond_with_correct_status() {
        let invalid_paths = [
            "/block/notahexencodedblockhash",
            "/deploy/notahexencodedblockhash",
            "/deploy/accepted/notahexencodedblockhash",
            "/deploy/processed/notahexencodedblockhash",
            "/deploy/expired/notahexencodedblockhash",
        ];

        for path in invalid_paths {
            let response = get_response_from_path(path).await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            assert!(!response.body().is_empty());
        }
    }
}
