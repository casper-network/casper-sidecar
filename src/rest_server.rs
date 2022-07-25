use crate::sqlite_db::SqliteDb;
use anyhow::Error;
use std::convert::Infallible;
use std::path::PathBuf;
use tracing::error;
use warp::http::StatusCode;
use warp::{Rejection, Reply};
use serde::Serialize;

mod filters {
    use std::convert::Infallible;
    use warp::Filter;
    use crate::rest_server::{handle_rejection, handlers, InvalidPath};
    use crate::sqlite_db::DatabaseReader;

    pub fn combined_filters<Db: DatabaseReader + Clone + Send + Sync>(db: Db) -> impl Filter<Extract = impl warp::Reply, Error = Infallible> + Clone {
        root_filter()
            .or(root_and_invalid_path())
            .or(block_filters(db.clone()))
            .or(deploy_filters(db.clone()))
            .or(step_by_era(db.clone()))
            .or(fault_by_public_key(db))
            .recover(handle_rejection)
    }

    pub fn root_filter() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        warp::path::end()
            .and_then(|| {
                async {
                    Err::<String, warp::Rejection>(warp::reject::custom(InvalidPath))
                }
            })
    }

    pub fn root_and_invalid_path() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        warp::path::param()
            .and(warp::path::end())
            .and_then(|_param: String| {
                async {
                    Err::<String, warp::Rejection>(warp::reject::custom(InvalidPath))
                }
            })
    }

    pub fn block_filters<Db: DatabaseReader + Clone + Send + Sync>(db: Db) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        latest_block(db.clone())
            .or(block_by_hash(db.clone()))
            .or(block_by_height(db))
    }

    pub fn deploy_filters<Db: DatabaseReader + Clone + Send + Sync>(db: Db) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        latest_deploy(db.clone())
            .or(deploy_by_hash(db.clone()))
            .or(deploy_accepted_by_hash(db.clone()))
            .or(deploy_processed_by_hash(db.clone()))
            .or(deploy_expired_by_hash(db))
    }

    pub(super) fn latest_block<Db: DatabaseReader + Clone + Send + Sync>(db: Db) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        warp::path!("block")
            .and(warp::get())
            .and(with_db(db))
            .and_then(handlers::get_latest_block)
    }

    pub(super) fn block_by_hash<Db: DatabaseReader + Clone + Send + Sync>(db: Db) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        warp::path!("block" / String)
            .and(warp::get())
            .and(with_db(db))
            .and_then(handlers::get_block_by_hash)
    }

    pub(super) fn block_by_height<Db: DatabaseReader + Clone + Send + Sync>(db: Db) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        warp::path!("block" / u64)
            .and(warp::get())
            .and(with_db(db))
            .and_then(handlers::get_block_by_height)
    }

    pub(super) fn latest_deploy<Db: DatabaseReader + Clone + Send + Sync>(db: Db) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        warp::path!("deploy")
            .and(warp::get())
            .and(with_db(db))
            .and_then(handlers::get_latest_deploy)
    }

    pub(super) fn deploy_by_hash<Db: DatabaseReader + Clone + Send + Sync>(db: Db) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        warp::path!("deploy" / String)
            .and(warp::get())
            .and(with_db(db))
            .and_then(handlers::get_deploy_by_hash)
    }

    pub(super) fn deploy_accepted_by_hash<Db: DatabaseReader + Clone + Send + Sync>(db: Db) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        warp::path!("deploy" / "accepted" / String)
            .and(warp::get())
            .and(with_db(db))
            .and_then(handlers::get_deploy_accepted_by_hash)
    }

    pub(super) fn deploy_processed_by_hash<Db: DatabaseReader + Clone + Send + Sync>(db: Db) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        warp::path!("deploy" / "processed" / String)
            .and(warp::get())
            .and(with_db(db))
            .and_then(handlers::get_deploy_processed_by_hash)
    }

    pub(super) fn deploy_expired_by_hash<Db: DatabaseReader + Clone + Send + Sync>(db: Db) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        warp::path!("deploy" / "expired" / String)
            .and(warp::get())
            .and(with_db(db))
            .and_then(handlers::get_deploy_expired_by_hash)
    }

    pub(super) fn step_by_era<Db: DatabaseReader + Clone + Send + Sync>(db: Db) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        warp::path!("step" / u64)
            .and(warp::get())
            .and(with_db(db))
            .and_then(handlers::get_step_by_era)
    }

    pub(super) fn fault_by_public_key<Db: DatabaseReader + Clone + Send + Sync>(db: Db) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        warp::path!("fault" / String)
            .and(warp::get())
            .and(with_db(db))
            .and_then(handlers::get_fault_by_public_key)
    }

    fn with_db<Db: DatabaseReader + Clone + Send>(db: Db) -> impl Filter<Extract=(Db,), Error=Infallible> + Clone {
        warp::any().map(move || db.clone())
    }
}

mod handlers {
    use warp::{Rejection, Reply};
    use crate::rest_server::serialize_or_reject_storage_result;
    use crate::sqlite_db::DatabaseReader;

    pub(super) async fn get_latest_block<Db: DatabaseReader + Clone + Send>(db: Db) ->  Result<impl Reply, Rejection> {
        let db_result = db.get_latest_block().await;
        serialize_or_reject_storage_result(db_result)
    }

    pub(super) async fn get_block_by_hash<Db: DatabaseReader + Clone + Send>(hash: String, db: Db) -> Result<impl Reply, Rejection> {
        let db_result = db.get_block_by_hash(&hash).await;
        serialize_or_reject_storage_result(db_result)
    }

    pub(super) async fn get_block_by_height<Db: DatabaseReader + Clone + Send>(height: u64, db: Db) -> Result<impl Reply, Rejection> {
        let db_result = db.get_block_by_height(height).await;
        serialize_or_reject_storage_result(db_result)
    }

    pub(super) async fn get_latest_deploy<Db: DatabaseReader + Clone + Send>(db: Db) ->  Result<impl Reply, Rejection> {
        let db_result = db.get_latest_deploy().await;
        serialize_or_reject_storage_result(db_result)
    }

    pub(super) async fn get_deploy_by_hash<Db: DatabaseReader + Clone + Send>(hash: String, db: Db) -> Result<impl Reply, Rejection> {
        let db_result = db.get_deploy_by_hash(&hash).await;
        serialize_or_reject_storage_result(db_result)
    }

    pub(super) async fn get_deploy_accepted_by_hash<Db: DatabaseReader + Clone + Send>(hash: String, db: Db) -> Result<impl Reply, Rejection> {
        let db_result = db.get_deploy_accepted_by_hash(&hash).await;
        serialize_or_reject_storage_result(db_result)
    }

    pub(super) async fn get_deploy_processed_by_hash<Db: DatabaseReader + Clone + Send>(hash: String, db: Db) -> Result<impl Reply, Rejection> {
        let db_result = db.get_deploy_processed_by_hash(&hash).await;
        serialize_or_reject_storage_result(db_result)
    }

    pub(super) async fn get_deploy_expired_by_hash<Db: DatabaseReader + Clone + Send>(hash: String, db: Db) -> Result<impl Reply, Rejection> {
        let db_result = db.get_deploy_expired_by_hash(&hash).await;
        serialize_or_reject_storage_result(db_result)
    }

    pub(super) async fn get_step_by_era<Db: DatabaseReader + Clone + Send>(era_id: u64, db: Db) -> Result<impl Reply, Rejection> {
        let db_result = db.get_step_by_era(era_id).await;
        serialize_or_reject_storage_result(db_result)
    }

    pub(super) async fn get_fault_by_public_key<Db: DatabaseReader + Clone + Send>(public_key: String, db: Db) -> Result<impl Reply, Rejection> {
        let db_result = db.get_fault_by_public_key(&public_key).await;
        serialize_or_reject_storage_result(db_result)
    }
}

pub async fn run_server(db_path: PathBuf, port: u16) -> Result<(), Error> {
    let db = SqliteDb::new_read_only(&db_path)?;

    let api = filters::combined_filters(db);

    warp::serve(api).run(([127, 0, 0, 1], port)).await;

    Ok(())
}

fn serialize_or_reject_storage_result<T>(storage_result: Result<T, Error>) -> Result<impl Reply, Rejection>
    where T: Serialize
{
    match storage_result {
        Ok(data) => match serde_json::to_string_pretty(&data) {
            Ok(pretty_json) => {
                Ok(warp::reply::with_status(
                    pretty_json,
                    StatusCode::OK
                ).into_response())
            },
            Err(err) => Err(warp::reject::custom(SerializationError(err)))
        },
        Err(err) => Err(warp::reject::custom(StorageError(err)))
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
struct StorageError(Error);
impl warp::reject::Reject for StorageError {}


async fn handle_rejection(err: Rejection) -> Result<impl Reply, Infallible> {
    let code;
    let message;

    if err.is_not_found() {
        code = StatusCode::NOT_FOUND;
        message = String::from("No results");
    } else if let Some(InvalidPath) = err.find() {
        code = StatusCode::BAD_REQUEST;
        message = String::from("Invalid request path provided");
    } else if let Some(SerializationError(err)) = err.find() {
        code = StatusCode::INTERNAL_SERVER_ERROR;
        message = format!("Serialization Error: {}", err);
    } else if let Some(StorageError(err)) = err.find() {
        match err.to_string().as_str() {
            "Query returned no rows" => {
                code = StatusCode::NOT_FOUND;
                message = "No results found for query".to_string();
            }
            _ => {
                code = StatusCode::INTERNAL_SERVER_ERROR;
                message = format!("Storage Error: {}", err);
            }
        }
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
    use bytes::Bytes;
    use http::{StatusCode, Response};
    use warp::test::request;
    use crate::rest_server::filters;
    use crate::testing::test_database::MockDatabase;

    // These are the codes that would be expected for a request to a valid path.
    const RESP_CODES_FOR_VALID_REQ: [StatusCode; 3] = [
        StatusCode::OK,
        StatusCode::NOT_FOUND,
        StatusCode::BAD_REQUEST
    ];

    async fn get_response_from_path(path: &str) -> Response<Bytes> {
        let db = MockDatabase;

        let api = filters::combined_filters(db);

        request()
            .path(path)
            .reply(&api)
            .await
    }

    #[tokio::test]
    async fn valid_paths_should_respond_with_correct_status() {
        let valid_paths = [
            "/block",
            "/block/hash",
            "/block/height",
            "/deploy",
            "/deploy/hash",
            "/deploy/accepted/hash",
            "/deploy/processed/hash",
            "/deploy/expired/hash",
            "/step/era",
            "/fault/publickey"
        ];

        for path in valid_paths {
            let response = get_response_from_path(path).await;
            assert!(RESP_CODES_FOR_VALID_REQ.contains(&response.status()));
            assert!(!response.body().is_empty())
        }
    }
}
