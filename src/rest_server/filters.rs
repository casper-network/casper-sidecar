use std::convert::Infallible;

use warp::Filter;

use super::{
    errors::{handle_rejection, InvalidPath},
    handlers,
};
use crate::types::database::DatabaseReader;

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
    warp::path::end()
        .and_then(|| async { Err::<String, warp::Rejection>(warp::reject::custom(InvalidPath)) })
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
