use crate::sqlite_db::ReadOnlyDatabase;
use crate::types::structs::{DeployProcessed, Step};
use anyhow::Error;
use casper_node::types::Block;
use serde::Serialize;
use std::convert::Infallible;
use std::path::PathBuf;
use tracing::{error, info};
use warp::http::StatusCode;
use warp::{Filter, Rejection, Reply};

pub async fn start_server(db_path: PathBuf, port: u16) -> Result<(), Error> {
    let storage = ReadOnlyDatabase::new(&db_path)?;

    // GET / - return proper paths
    let root = warp::path::end().map(|| {
        "SIDECAR :: Server: Invalid Path /, please try calling the 'block' or 'deploy' API\n"
            .to_string()
    });

    // GET /:String - checks for "block" or "deploy" routes
    let cloned_storage = storage.clone();
    let root_with_param =
        warp::path::param()
            .and(warp::path::end())
            .and_then(move |root_path: String| {
                let cloned_storage = cloned_storage.clone();
                async move {
                    let response = match root_path.as_str() {
                        "block" | "deploy" => get_latest(root_path.as_str(), cloned_storage).await,
                        _ => invalid_path().await,
                    };
                    match response {
                        Ok(resp) => Ok(resp),
                        Err(_e) => Err(warp::reject::not_found()),
                    }
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
                    let serialised = cloned_storage.get_block_by_hash(hash.as_str()).await;
                    match serialised {
                        Ok(x) => Ok(format!("{:#?}\n", serde_json::from_str::<Block>(&x).unwrap())),
                        Err(_e) => Err(warp::reject::not_found()),
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
                    let serialised = cloned_storage.get_block_by_height(height).await;
                    match serialised {
                        Ok(x) => Ok(format!("{:#?}\n", serde_json::from_str::<Block>(&x).unwrap())),
                        Err(_e) => Err(warp::reject::not_found()),
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
                    let bytes = cloned_storage.get_deploy_by_hash(hash.as_str()).await;
                    match bytes {
                        Ok(x) => Ok(format!(
                            "{:#?}\n",
                            serde_json::from_str::<DeployProcessed>(&x).unwrap()
                        )),
                        Err(_e) => Err(warp::reject::not_found()),
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
                let bytes = cloned_storage.get_step_by_era(era_id).await;
                match bytes {
                    Ok(x) => Ok(format!("{:#?}\n", serde_json::from_str::<Step>(&x).unwrap())),
                    Err(_e) => Err(warp::reject::not_found()),
                }
            }
        });

    let routes = warp::get().and(
        root.or(root_with_param)
            .or(block_by_hash)
            .or(block_by_height)
            .or(deploy_by_hash)
            .or(step_by_era)
            .recover(handle_rejection),
    );

    info!(message = "REST server starting", port=port);

    warp::serve(routes).run(([127, 0, 0, 1], port)).await;

    Ok(())
}

async fn get_latest(item: &str, storage: ReadOnlyDatabase) -> Result<String, Rejection> {
    let bytes = match item {
        "block" => storage.get_latest_block().await,
        "deploy" => storage.get_latest_deploy().await,
        _ => Err(Error::msg("Invalid item")),
    };
    match bytes {
        Ok(x) => {
            if item == "block" {
                Ok(format!("{:#?}\r\nThis is the {path} API, calling /{path} returns the latest {path} as above.\nPlease try calling /{path}/hash/:String or /{path}/height/:u64\n", serde_json::from_str::<Block>(&x), path=item))
            } else if item == "deploy" {
                Ok(format!("{:#?}\r\nThis is the {path} API, calling /{path} returns the latest {path} as above.\nPlease try calling /{path}/hash/:String or /{path}/height/:u64\n", serde_json::from_str::<DeployProcessed>(&x), path=item))
            } else {
                Err(warp::reject::not_found())
            }
        }
        Err(_e) => Err(warp::reject::not_found()),
    }
}

async fn invalid_path() -> Result<String, Rejection> {
    Ok("SIDECAR :: Server: Invalid path provided. Please try calling the 'block' or 'deploy' API.\n".to_string())
}

#[derive(Serialize)]
struct ApiError {
    code: u16,
    message: String,
}

async fn handle_rejection(err: Rejection) -> Result<impl Reply, Infallible> {
    let code;
    let message;

    if err.is_not_found() {
        code = StatusCode::NOT_FOUND;
        message = String::from("Not found");
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
