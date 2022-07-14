use crate::sqlite_db::{SqliteDb, DatabaseReader};
use anyhow::Error;
use std::convert::Infallible;
use std::path::PathBuf;
use tracing::{error, info};
use warp::http::StatusCode;
use warp::{Filter, path, Rejection, Reply};
use serde::Serialize;
use serde_json::json;

mod filters {
    use std::convert::Infallible;
    use warp::Filter;
    use crate::rest_server::handlers;
    use crate::sqlite_db::DatabaseReader;

    pub fn block_filters<Db: DatabaseReader + Clone + Send>(db: Db) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        block_by_hash(db.clone())
    }

    fn block_by_hash<Db: DatabaseReader + Clone + Send>(db: Db) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        warp::path!("block" / String)
            .and(warp::get())
            .and(with_db(db.clone()))
            .and_then(handlers::get_block_by_hash)
    }

    fn with_db<Db: DatabaseReader + Clone + Send>(db: Db) -> impl Filter<Extract=(Db,), Error=Infallible> + Clone {
        warp::any().map(move || db.clone())
    }
}

mod handlers {
    use std::convert::Infallible;
    use http::StatusCode;
    use warp::Reply;
    use crate::sqlite_db::DatabaseReader;

    pub async fn get_block_by_hash<Db>(hash: String, db: Db) -> Result<impl Reply, Infallible>
        where
            Db: DatabaseReader + Clone + Send
    {
        match db.get_block_by_hash(&hash).await {
            Ok(res) => Ok(warp::reply::with_status(format!("OK: Hello: {}", res.to_string()), StatusCode::OK).into_response()),
            Err(err) => Ok(warp::reply::with_status(format!("Err: Hello: {}", err.to_string()), StatusCode::OK).into_response())
        }
    }
}

#[cfg(test)]
pub(super) mod test_fixtures {
    use std::path::Path;
    use anyhow::Error;
    use async_trait::async_trait;
    use casper_node::types::{Block, Deploy};
    use crate::{DeployProcessed, Fault, Step};
    use crate::sqlite_db::{AggregateDeployInfo, DatabaseReader};

    #[derive(Clone)]
    pub struct MockDatabase;

    impl MockDatabase {
        pub fn new(path: &Path) -> MockDatabase {
            MockDatabase
        }
    }

    #[async_trait]
    impl DatabaseReader for MockDatabase {
        async fn get_latest_block(&self) -> Result<Block, Error> {
            Err(Error::msg("!"))
        }

        async fn get_block_by_height(&self, height: u64) -> Result<Block, Error> {
            Err(Error::msg("todo!"))
        }

        async fn get_block_by_hash(&self, hash: &str) -> Result<Block, Error> {
            Err(Error::msg("todo!"))
        }

        async fn get_latest_deploy(&self) -> Result<AggregateDeployInfo, Error> {
            Err(Error::msg("todo!"))
        }

        async fn get_deploy_by_hash(&self, hash: &str) -> Result<AggregateDeployInfo, Error> {
            Err(Error::msg("todo!"))
        }

        async fn get_deploy_accepted_by_hash(&self, hash: &str) -> Result<Deploy, Error> {
            Err(Error::msg("todo!"))
        }

        async fn get_deploy_processed_by_hash(&self, hash: &str) -> Result<DeployProcessed, Error> {
            Err(Error::msg("todo!"))
        }

        async fn get_deploy_expired_by_hash(&self, hash: &str) -> Result<bool, Error> {
            Err(Error::msg("todo!"))
        }

        async fn get_step_by_era(&self, era_id: u64) -> Result<Step, Error> {
            Err(Error::msg("todo!"))
        }

        async fn get_fault_by_public_key(&self, public_key: &str) -> Result<Fault, Error> {
            Err(Error::msg("todo!"))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use http::StatusCode;
    use warp::test::request;
    use crate::rest_server::filters;
    use crate::rest_server::test_fixtures::MockDatabase;

    #[tokio::test]
    async fn check_block_endpoint() {
        let db_path = Path::new("stubbed");
        let db = MockDatabase::new(db_path);

        let api = filters::block_filters(db);

        let resp = request()
            .path("/block/George")
            .reply(&api)
            .await;

        assert_eq!(resp.status(), StatusCode::OK)
    }
}















pub async fn start_server(db_path: PathBuf, port: u16) -> Result<(), Error> {
    let storage = SqliteDb::new_read_only(&db_path)?;

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

    // GET /deploy/expired/:String
    let cloned_storage = storage.clone();
    let deploy_expired_by_hash =
        deploy_root
            .and(path("expired"))
            .and(path::param())
            .and_then(move |hash: String| {
                let cloned_storage = cloned_storage.clone();
                async move {
                    match cloned_storage.get_deploy_expired_by_hash(&hash).await {
                        Ok(expired_status) => Ok(json!({"expired": expired_status}).to_string()),
                        Err(err) => Err(warp::reject::custom(StorageError(err)))
                    }
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
            .or(deploy_expired_by_hash)
            .or(step_by_era)
            .or(fault_by_public_key)
            .recover(handle_rejection),
    );

    info!(message = "REST server starting", port=port);

    warp::serve(routes).run(([127, 0, 0, 1], port)).await;

    Ok(())
}

async fn get_latest(item: &str, storage: SqliteDb) -> Result<String, Rejection> {
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
        match err.to_string().as_str() {
            "Query returned no rows" => {
                code = StatusCode::NOT_FOUND;
                message = format!("No results found for query");
            }
            _ => {
                code = StatusCode::INTERNAL_SERVER_ERROR;
                message = format!("Storage Error: {}", err);
            }
        }
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
