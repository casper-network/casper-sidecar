use crate::sqlite_db::ReadOnlyDatabase;
use anyhow::Error;
use std::convert::Infallible;
use std::path::PathBuf;
use tracing::{error, info};
use warp::http::StatusCode;
use warp::{Filter, path, Rejection, Reply};
use serde::Serialize;

pub async fn start_server(db_path: PathBuf, port: u16) -> Result<(), Error> {
    let storage = ReadOnlyDatabase::new(&db_path)?;

    // GET / - return proper paths
    let root = path::end()
        .and_then(move || {
            async move {
                // todo I had to set the OK type to String to keep it happy - there is probably a better way to satisfy the constraint that it needs to impl Reply
                Result::<String ,Rejection>::Err(warp::reject::custom(InvalidPath))
            }
    });

    // GET /:String - checks for "block" or "deploy" routes
    let cloned_storage = storage.clone();
    let root_with_param =
        path::param()
            .and(path::end())
            .and_then(move |root_path: String| {
                let cloned_storage = cloned_storage.clone();
                async move {
                    get_latest(root_path.as_str(), cloned_storage).await
                }
            });

    let block_root = warp::path("block");
    let deploy_root = warp::path("deploy");
    let hash = warp::path("hash");
    let height = warp::path("height");

    // GET /block/hash/:String
    let cloned_storage = storage.clone();
    let block_by_hash =
        block_root
            .and(hash.and(path::param()))
            .and_then(move |hash: String| {
                let cloned_storage = cloned_storage.clone();
                async move {
                    let storage_result = cloned_storage.get_block_by_hash(hash.as_str()).await;
                    serialize_or_reject_storage_result(storage_result)
                }
            });

    // GET /block/height/:u64
    let cloned_storage = storage.clone();
    let block_by_height =
        block_root
            .and(height.and(path::param()))
            .and_then(move |height: u64| {
                let cloned_storage = cloned_storage.clone();
                async move {
                    let storage_result = cloned_storage.get_block_by_height(height).await;
                    serialize_or_reject_storage_result(storage_result)
                }
            });

    // GET /deploy/:String
    let cloned_storage = storage.clone();
    let deploy_by_hash =
        deploy_root
            .and(path::param())
            .and_then(move |hash: String| {
                let cloned_storage = cloned_storage.clone();
                async move {
                    let storage_result = cloned_storage.get_deploy_by_hash(&hash).await;
                    serialize_or_reject_storage_result(storage_result)
                }
            });

    // GET /deploy/accepted/:String
    let cloned_storage = storage.clone();
    let deploy_accepted_by_hash =
        deploy_root
            .and(path("accepted"))
            .and(path::param())
            .and_then(move |hash: String| {
                let cloned_storage = cloned_storage.clone();
                async move {
                    let storage_result = cloned_storage.get_deploy_accepted_by_hash(&hash).await;
                    serialize_or_reject_storage_result(storage_result)
                }
            });

    // GET /deploy/processed/:String
    let cloned_storage = storage.clone();
    let deploy_processed_by_hash =
        deploy_root
            .and(path("processed"))
            .and(path::param())
            .and_then(move |hash: String| {
                let cloned_storage = cloned_storage.clone();
                async move {
                    let storage_result = cloned_storage.get_deploy_processed_by_hash(&hash).await;
                    serialize_or_reject_storage_result(storage_result)
                }
            });

    // GET /step/:u64
    let cloned_storage = storage.clone();
    let step_by_era = warp::path("step")
        .and(path::param())
        .and_then(move |era_id: u64| {
            let cloned_storage = cloned_storage.clone();
            async move {
                let storage_result = cloned_storage.get_step_by_era(era_id).await;
                serialize_or_reject_storage_result(storage_result)
            }
        });

    // GET /fault/public_key/:String
    let cloned_storage = storage.clone();
    let fault_by_public_key = warp::path("fault")
        .and(warp::path("public_key"))
        .and(path::param())
        .and_then(move |public_key: String| {
            let cloned_storage = cloned_storage.clone();
            async move {
                let storage_result = cloned_storage.get_fault_by_public_key(&public_key).await;
                serialize_or_reject_storage_result(storage_result)
            }
        });

    let routes = warp::get().and(
        root.or(root_with_param)
            .or(block_by_hash)
            .or(block_by_height)
            .or(deploy_by_hash)
            .or(deploy_accepted_by_hash)
            .or(deploy_processed_by_hash)
            .or(step_by_era)
            .or(fault_by_public_key)
            .recover(handle_rejection),
    );

    info!(message = "REST server starting", port=port);

    warp::serve(routes).run(([127, 0, 0, 1], port)).await;

    Ok(())
}

async fn get_latest(item: &str, storage: ReadOnlyDatabase) -> Result<String, Rejection> {
    match item {
        "block" => {
            let storage_result = storage.get_latest_block().await;
            serialize_or_reject_storage_result(storage_result)
        },
        "deploy" => {
            let storage_result = storage.get_latest_deploy().await;
            serialize_or_reject_storage_result(storage_result)

        },
        _ => Err(warp::reject::custom(InvalidPath))
    }
}

fn serialize_or_reject_storage_result<T>(storage_result: Result<T, Error>) -> Result<String, Rejection>
    where T: Serialize
{
    match storage_result {
        Ok(data) => match serde_json::to_string_pretty(&data) {
            Ok(pretty_json) => Ok(pretty_json),
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
        // todo this shouldn't always be a 500. Storage will also return an Err if the record isn't present which should be a 404.
        code = StatusCode::INTERNAL_SERVER_ERROR;
        message = format!("Storage Error: {}", err);
    } else if let Some(_) = err.find::<warp::reject::MethodNotAllowed>() {
        code = StatusCode::METHOD_NOT_ALLOWED;
        message = String::from("Method not allowed");
    } else {
        error!("Unhandled REST Server Error: {:?}", err);
        code = StatusCode::INTERNAL_SERVER_ERROR;
        message = String::from("Unhandled error - you've found a bug!");
    }

    let json = warp::reply::json(&ApiError {
        code: code.as_u16(),
        message: message.into(),
    });

    Ok(warp::reply::with_status(json, code))
}
