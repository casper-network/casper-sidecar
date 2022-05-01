use crate::sqlite_db::ReadOnlyDatabase;
use crate::types::structs::{DeployProcessed, Step};
use crate::kv_store::{KVStore, RocksDB};
use anyhow::Error;
use casper_node::types::Block;
use casper_types::{AsymmetricType, PublicKey, account::AccountHash};
use serde::Serialize;
use std::convert::Infallible;
use std::fmt::{Display, Formatter};
use std::path::Path;
use casper_types::bytesrepr::FromBytes;
use tracing::{error, info, warn};
use warp::http::StatusCode;
use warp::{reject, Filter, Rejection, Reply};

pub async fn start_server(db_path: &Path, kv_path: &Path, port: u16) -> Result<(), Error> {
    let storage = ReadOnlyDatabase::new(&db_path)?;
    let kv_store = RocksDB::connect_read_only(kv_path)?;

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

    let balance_root = warp::path("balance");

    // GET /balance/health-check
    let cloned_kv_store = kv_store.clone();
    let balance_health_check = balance_root
        .and(warp::path("health-check"))
        .and_then(move || {
            let cloned_kv_store = cloned_kv_store.clone();
            async move {
                match cloned_kv_store.find("health-check") {
                    Ok(status) => match status {
                        None => Err(warp::reject::not_found()),
                        Some(status) => Ok(format!("Status: {}\n", status)),
                    },
                    Err(_) => Err(warp::reject::not_found()),
                }
            }
        });

    // GET /balance/key/:string
    let cloned_kv_store = kv_store.clone();
    let balance_by_pub_key_hex = balance_root
        .and(warp::path("key"))
        .and(warp::path::param())
        .and_then(move |pub_key_hex: String| {
            let cloned_kv_store = cloned_kv_store.clone();
            async move {
                if !vec!["01", "02"].contains(&&pub_key_hex[0..2])
                    && (pub_key_hex.len() == 64 || pub_key_hex.len() == 66)
                {
                    return Err(warp::reject::custom(ExpectedKeyGotHash));
                }
                match PublicKey::from_hex(pub_key_hex) {
                    Ok(public_key) => {
                        let account_hash = AccountHash::from(&public_key);
                        let hash_hex = hex::encode(account_hash.value());
                        let purse_query_key = format!("purse-of-{}", &hash_hex);
                        let purse_uref = match cloned_kv_store.find(&purse_query_key) {
                            Ok(opt_uref) => match opt_uref {
                                None => return Err(warp::reject::not_found()),
                                Some(uref) => uref
                            }
                            Err(_) => return Err(warp::reject::not_found()),
                        };
                        match cloned_kv_store.find(&purse_uref) {
                            Ok(balance) => match balance {
                                Some(balance_string) => Ok(format!(
                                    "Balance Request\n\tAccountHash: {}\n\tBalance: {}\n",
                                    hex::encode(account_hash.value()),
                                    balance_string
                                )),
                                None => Err(reject::custom(BalanceNotFound)),
                            },
                            Err(_) => Err(warp::reject::custom(BalanceNotFound)),
                        }
                    }
                    Err(_) => Err(warp::reject::not_found()),
                }
            }
        });

    // GET /balance/hash/:string
    let cloned_kv_store = kv_store.clone();
    let balance_by_account_hash = balance_root
        .and(warp::path("hash"))
        .and(warp::path::param())
        .and_then(move |account_hash: String| {
            let cloned_kv_store = cloned_kv_store.clone();
            async move {
                match hex::decode(&account_hash) {
                    Ok(x) => match AccountHash::from_bytes(&x) {
                            Ok(hash) => {
                                assert_eq!(cloned_kv_store.find("health-check").unwrap().unwrap(), "healthy");
                                let purse_query_key = format!("purse-of-{}", hex::encode(&hash.0.value()));
                                let purse_uref = match cloned_kv_store.find(&purse_query_key) {
                                    Ok(opt_uref) => match opt_uref {
                                        None => {
                                            warn!("No uref found!");
                                            return Err(warp::reject::not_found())
                                        },
                                        Some(uref) => uref
                                    }
                                    Err(_) => return Err(warp::reject::not_found()),
                                };
                                match cloned_kv_store.find(&purse_uref) {
                                    Ok(balance) => {
                                        match balance {
                                            Some(balance_string) => Ok(format!(
                                                "Balance Request\n\tAccountHash: {}\n\tBalance: {}\n",
                                                account_hash, balance_string
                                            )),
                                            None => Err(reject::custom(BalanceNotFound)),
                                        }
                                    },
                                    Err(_) => Err(warp::reject::not_found()),
                                }
                            },
                            Err(_) => Err(warp::reject::not_found())
                        },
                    Err(_) => Err(warp::reject::not_found())
                }
            }
        });

    let cloned_kv_store = kv_store.clone();
    let raw_purse_query = balance_root
        .and(warp::path("purse"))
        .and(warp::path::param())
        .and_then(move |purse_uref: String| {
            let cloned_kv_store = cloned_kv_store.clone();
            async move {
                match cloned_kv_store.find(&purse_uref) {
                    Ok(maybe_balance) => match maybe_balance {
                        None => Err(warp::reject::not_found()),
                        Some(balance) => Ok(balance)
                    },
                    Err(_) => Err(warp::reject::not_found())
                }
            }
        });

    let routes = warp::get().and(
        root.or(root_with_param)
            .or(block_by_hash)
            .or(block_by_height)
            .or(deploy_by_hash)
            .or(step_by_era)
            .or(balance_health_check)
            .or(balance_by_pub_key_hex)
            .or(balance_by_account_hash)
            .or(raw_purse_query)
            .recover(handle_rejection),
    );

    println!("\n\tSidecar running on localhost:{}\n\tTry querying for a block by hash: /block/<block hash>\n", port);

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

#[derive(Debug)]
struct BalanceNotFound;

impl reject::Reject for BalanceNotFound {}

impl Display for BalanceNotFound {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Unable to find a balance for the given key/hash")
    }
}

#[derive(Debug)]
struct ExpectedKeyGotHash;

impl reject::Reject for ExpectedKeyGotHash {}

impl Display for ExpectedKeyGotHash {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "It looks like you might have used an account hash instead of a public key. Please try querying the /balance/hash/ endpoint instead.")
    }
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
    } else if let Some(BalanceNotFound) = err.find() {
        code = StatusCode::NOT_FOUND;
        message = format!("{}", BalanceNotFound);
    } else if let Some(ExpectedKeyGotHash) = err.find() {
        code = StatusCode::BAD_REQUEST;
        message = format!("{}", ExpectedKeyGotHash);
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
