use super::{
    errors::{handle_rejection, InvalidPath},
    handlers,
    requests::ListDeploysRequest,
};
use crate::types::database::DatabaseReader;
use std::convert::Infallible;
use warp::Filter;

/// Helper function to specify available filters.
/// Input: the database with data to be filtered.
/// Return: the filtered data.
pub(super) fn combined_filters<Db: DatabaseReader + Clone + Send + Sync>(
    db: Db,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Infallible> + Clone {
    root_filter()
        .or(root_and_invalid_path())
        .or(block_filters(db.clone()))
        .or(deploy_filters(db.clone()))
        .or(step_by_era(db.clone()))
        .or(faults_by_public_key(db.clone()))
        .or(faults_by_era(db.clone()))
        .or(finality_signatures_by_block(db))
        .or(metrics_filter())
        .recover(handle_rejection)
}

/// Handle the case where no filter URL was specified after the root address (HOST:PORT).
/// Return: a message that an invalid path was provided.
/// Example: curl http://127.0.0.1:18888
/// {"code":400,"message":"Invalid request path provided"}
fn root_filter() -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::path::end()
        .and_then(|| async { Err::<String, warp::Rejection>(warp::reject::custom(InvalidPath)) })
}

/// Handle the case where an invalid path was provided.
/// Return: a message that an invalid path was provided.
/// Example: curl http://127.0.0.1:18888/other
/// {"code":400,"message":"Invalid request path provided"}
fn root_and_invalid_path(
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::path::param().and_then(|_param: String| async {
        Err::<String, warp::Rejection>(warp::reject::custom(InvalidPath))
    })
}

/// Helper function to specify available filters for block information.
/// Input: the database with data to be filtered.
/// Return: the filtered data.
fn block_filters<Db: DatabaseReader + Clone + Send + Sync>(
    db: Db,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    latest_block(db.clone())
        .or(block_by_hash(db.clone()))
        .or(block_by_height(db))
}

/// Helper function to specify available filters for deploy information.
/// Input: the database with data to be filtered.
/// Return: the filtered data.
fn deploy_filters<Db: DatabaseReader + Clone + Send + Sync>(
    db: Db,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    deploy_by_hash(db.clone())
        .or(deploy_accepted_by_hash(db.clone()))
        .or(deploy_processed_by_hash(db.clone()))
        .or(deploy_expired_by_hash(db.clone()))
        .or(list_deploys(db))
}

/// Return information about the last block added to the linear chain.
/// Input: the database with data to be filtered.
/// Return: data about the latest block.
/// Path URL: block
/// Example: curl http://127.0.0.1:18888/block
fn latest_block<Db: DatabaseReader + Clone + Send + Sync>(
    db: Db,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::path!("block")
        .and(warp::get())
        .and(with_db(db))
        .and_then(handlers::get_latest_block)
}

/// Return information about a block given its block hash.
/// Input: the database with data to be filtered.
/// Return: data about the block specified.
/// Path URL: block/<block-hash>
/// Example: curl http://127.0.0.1:18888/block/c0292d8408e9d83d1aaceadfbeb25dc38cda36bcb91c3d403a0deb594dc3d63f
fn block_by_hash<Db: DatabaseReader + Clone + Send + Sync>(
    db: Db,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::path!("block" / String)
        .and(warp::get())
        .and(with_db(db))
        .and_then(handlers::get_block_by_hash)
}

/// Return information about a block given a specific block height.
/// Input: the database with data to be filtered.
/// Return: data about the block requested.
/// Path URL: block/<block-height>
/// Example: curl http://127.0.0.1:18888/block/630151
fn block_by_height<Db: DatabaseReader + Clone + Send + Sync>(
    db: Db,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::path!("block" / u64)
        .and(warp::get())
        .and(with_db(db))
        .and_then(handlers::get_block_by_height)
}

/// Return an aggregate of the different states for the given deploy. This is a synthetic JSON not emitted by the node.
/// The output differs depending on the deploy's status, which changes over time as the deploy goes through its lifecycle.
/// Input: the database with data to be filtered.
/// Return: data about the deploy specified.
/// Path URL: deploy/<deploy-hash>
/// Example: curl http://127.0.0.1:18888/deploy/f01544d37354c5f9b2c4956826d32f8e44198f94fb6752e87f422fe3071ab58a
fn deploy_by_hash<Db: DatabaseReader + Clone + Send + Sync>(
    db: Db,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::path!("deploy" / String)
        .and(warp::get())
        .and(with_db(db))
        .and_then(handlers::get_deploy_by_hash)
}

/// Return a paginated list of deploy aggregates. See also #deploy_by_hash
/// Input: the database with data to be filtered.
/// Return: data about the deploy specified.
/// Path URL: deploy
/// Example: curl http://127.0.0.1:18888/deploy?exclude_expired=true&exclude_not_processed=true&limit=20&offset=150&sort_column=block_timestamp&sort_order=asc
pub(crate) fn list_deploys<Db: DatabaseReader + Clone + Send + Sync>(
    db: Db,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("deploys")
        .and(warp::post())
        .and(list_deploy_request_body())
        .and(with_db(db))
        .and_then(handlers::list_deploys)
}

/// Return information about an accepted deploy given its deploy hash.
/// Input: the database with data to be filtered.
/// Return: data about the accepted deploy.
/// Path URL: deploy/accepted/<deploy-hash>
/// Example: curl http://127.0.0.1:18888/deploy/accepted/f01544d37354c5f9b2c4956826d32f8e44198f94fb6752e87f422fe3071ab58a
fn deploy_accepted_by_hash<Db: DatabaseReader + Clone + Send + Sync>(
    db: Db,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::path!("deploy" / "accepted" / String)
        .and(warp::get())
        .and(with_db(db))
        .and_then(handlers::get_deploy_accepted_by_hash)
}

/// Return information about a deploy that expired given its deploy hash.
/// Input: the database with data to be filtered.
/// Return: data about the expired deploy.
/// Path URL: deploy/expired/<deploy-hash>
/// Example: curl http://127.0.0.1:18888/deploy/expired/e03544d37354c5f9b2c4956826d32f8e44198f94fb6752e87f422fe3071ab58a
fn deploy_expired_by_hash<Db: DatabaseReader + Clone + Send + Sync>(
    db: Db,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::path!("deploy" / "expired" / String)
        .and(warp::get())
        .and(with_db(db))
        .and_then(handlers::get_deploy_expired_by_hash)
}

/// Return information about a deploy that was processed given its deploy hash.
/// Input: the database with data to be filtered.
/// Return: data about the processed deploy.
/// Path URL: deploy/processed/<deploy-hash>
/// Example: curl http://127.0.0.1:18888/deploy/processed/f08944d37354c5f9b2c4956826d32f8e44198f94fb6752e87f422fe3071ab77a
fn deploy_processed_by_hash<Db: DatabaseReader + Clone + Send + Sync>(
    db: Db,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::path!("deploy" / "processed" / String)
        .and(warp::get())
        .and(with_db(db))
        .and_then(handlers::get_deploy_processed_by_hash)
}

/// Return the faults associated with a validator's public key.
/// Input: the database with data to be filtered.
/// Return: faults caused by the validator specified.
/// Path URL: faults/<public-key>
/// Example: curl http://127.0.0.1:18888/faults/01a601840126a0363a6048bfcbb0492ab5a313a1a19dc4c695650d8f3b51302703
fn faults_by_public_key<Db: DatabaseReader + Clone + Send + Sync>(
    db: Db,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::path!("faults" / String)
        .and(warp::get())
        .and(with_db(db))
        .and_then(handlers::get_faults_by_public_key)
}

/// Return the faults associated with an era given a valid era identifier.
/// Input: the database with data to be filtered.
/// Return: fault information for a given era.
/// Path URL: faults/<era-ID>
/// Example: curl http://127.0.0.1:18888/faults/2304
fn faults_by_era<Db: DatabaseReader + Clone + Send + Sync>(
    db: Db,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::path!("faults" / u64)
        .and(warp::get())
        .and(with_db(db))
        .and_then(handlers::get_faults_by_era)
}

/// Return the finality signatures in a block given its block hash.
/// Input: the database with data to be filtered.
/// Return: the finality signatures for the block specified.
/// Path URL: signatures/<block-hash>
/// Example: curl http://127.0.0.1:18888/signatures/c0292d8408e9d83d1aaceadfbeb25dc38cda36bcb91c3d403a0deb594dc3d63f
fn finality_signatures_by_block<Db: DatabaseReader + Clone + Send + Sync>(
    db: Db,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::path!("signatures" / String)
        .and(warp::get())
        .and(with_db(db))
        .and_then(handlers::get_finality_signatures_by_block)
}

/// Return the step event emitted at the end of an era, given a valid era identifier.
/// Input: the database with data to be filtered.
/// Return: the step event for a given era.
/// Path URL: step/<era-ID>
/// Example: curl http://127.0.0.1:18888/step/2304
fn step_by_era<Db: DatabaseReader + Clone + Send + Sync>(
    db: Db,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::path!("step" / u64)
        .and(warp::get())
        .and(with_db(db))
        .and_then(handlers::get_step_by_era)
}

fn metrics_filter() -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::path!("metrics")
        .and(warp::get())
        .and_then(handlers::metrics_handler)
}

/// Helper function to extract data from a database
fn with_db<Db: DatabaseReader + Clone + Send>(
    db: Db,
) -> impl Filter<Extract = (Db,), Error = Infallible> + Clone {
    warp::any().map(move || db.clone())
}

fn list_deploy_request_body(
) -> impl Filter<Extract = (ListDeploysRequest,), Error = warp::Rejection> + Clone {
    warp::body::content_length_limit(1024 * 32).and(warp::body::json())
}
