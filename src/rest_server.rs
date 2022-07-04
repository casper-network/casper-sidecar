use crate::sqlite_db::ReadOnlyDatabase;
use anyhow::Error;
use std::convert::Infallible;
use std::path::PathBuf;
use tracing::{error, info};
use warp::http::StatusCode;
use warp::{Filter, Rejection, Reply};
use serde::Serialize;

pub async fn start_server(db_path: PathBuf, port: u16) -> Result<(), Error> {
    let storage = ReadOnlyDatabase::new(&db_path)?;

    // GET / - return proper paths
    let root = warp::path::end()
        .and_then(move || {
            async move {
                // todo I had to set the OK type to String to keep it happy - there is probably a better way to satisfy the constraint that it needs to impl Reply
                Result::<String ,Rejection>::Err(warp::reject::custom(InvalidPath))
            }
    });

    // GET /:String - checks for "block" or "deploy" routes
    let cloned_storage = storage.clone();
    let root_with_param =
        warp::path::param()
            .and(warp::path::end())
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
            .and(hash.and(warp::path::param()))
            .and_then(move |hash: String| {
                let cloned_storage = cloned_storage.clone();
                async move {
                    let serialized = cloned_storage.get_block_by_hash(hash.as_str()).await;
                    match serialized {
                        Ok(block) => match serde_json::to_string_pretty(&block) {
                            Ok(pretty_json) => Ok(pretty_json),
                            Err(err) => Err(warp::reject::custom(SerializationError(err)))
                        },
                        Err(err) => Err(warp::reject::custom(StorageError(err)))
                    }
                }
            });

    // GET /block/height/:u64
    let cloned_storage = storage.clone();
    let block_by_height =
        block_root
            .and(height.and(warp::path::param()))
            .and_then(move |height: u64| {
                let cloned_storage = cloned_storage.clone();
                async move {
                    let serialized = cloned_storage.get_block_by_height(height).await;
                    match serialized {
                        Ok(block) => match serde_json::to_string_pretty(&block) {
                            Ok(pretty_json) => Ok(pretty_json),
                            Err(err) => Err(warp::reject::custom(SerializationError(err)))
                        },
                        Err(err) => Err(warp::reject::custom(StorageError(err)))
                    }
                }
            });

    // GET /deploy/hash/:String
    let cloned_storage = storage.clone();
    let deploy_by_hash =
        deploy_root
            .and(hash.and(warp::path::param()))
            .and_then(move |hash: String| {
                let cloned_storage = cloned_storage.clone();
                async move {
                    let serialized = cloned_storage.get_deploy_by_hash(hash.as_str()).await;
                    match serialized {
                        Ok(deploy) => match serde_json::to_string_pretty(&deploy) {
                            Ok(pretty_json) => Ok(pretty_json),
                            Err(err) => Err(warp::reject::custom(SerializationError(err)))
                        },
                        Err(err) => Err(warp::reject::custom(StorageError(err)))
                    }
                }
            });

    // GET /step/:u64
    let cloned_storage = storage.clone();
    let step_by_era = warp::path("step")
        .and(warp::path::param())
        .and_then(move |era_id: u64| {
            let cloned_storage = cloned_storage.clone();
            async move {
                let serialized = cloned_storage.get_step_by_era(era_id).await;
                match serialized {
                    Ok(step) => match serde_json::to_string_pretty(&step) {
                        Ok(pretty_json) => Ok(pretty_json),
                        Err(err) => Err(warp::reject::custom(SerializationError(err)))
                    },
                    Err(err) => Err(warp::reject::custom(StorageError(err)))
                }
            }
        });

    // GET /fault/public_key/:String
    let cloned_storage = storage.clone();
    let fault_by_public_key = warp::path("fault")
        .and(warp::path("public_key"))
        .and(warp::path::param())
        .and_then(move |public_key: String| {
            let cloned_storage = cloned_storage.clone();
            async move {
                let serialized = cloned_storage.get_fault_by_public_key(&public_key).await;
                match serialized {
                    Ok(fault) => match serde_json::to_string_pretty(&fault) {
                        Ok(pretty_json) => Ok(pretty_json),
                        Err(err) => Err(warp::reject::custom(SerializationError(err)))
                    },
                    Err(err) => Err(warp::reject::custom(StorageError(err)))
                }
            }
        });

    let routes = warp::get().and(
        root.or(root_with_param)
            .or(block_by_hash)
            .or(block_by_height)
            .or(deploy_by_hash)
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
            return match storage.get_latest_block().await {
                Ok(block) => match serde_json::to_string_pretty(&block) {
                    Ok(pretty_json) => Ok(pretty_json),
                    Err(err) => Err(warp::reject::custom(SerializationError(err)))
                },
                Err(err) => Err(warp::reject::custom(StorageError(err)))
            }
        },
        "deploy" => {
            return match storage.get_latest_deploy().await {
                Ok(aggregate_deploy_info) => match serde_json::to_string_pretty(&aggregate_deploy_info) {
                    Ok(pretty_json) => Ok(pretty_json),
                    Err(err) => Err(warp::reject::custom(SerializationError(err)))
                }
                Err(err) => Err(warp::reject::custom(StorageError(err)))
            }
        },
        _ => Err(warp::reject::custom(InvalidPath))
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
