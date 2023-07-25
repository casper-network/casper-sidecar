use super::{errors::handle_rejection, handlers, openapi::build_open_api_filters};
use crate::{
    types::database::DatabaseReader,
    utils::{root_filter, InvalidPath},
};
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
        .or(build_open_api_filters())
        .recover(handle_rejection)
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
        .or(deploy_expired_by_hash(db))
}

/// Return information about the last block added to the linear chain.
/// Input: the database with data to be filtered.
/// Return: data about the latest block.
/// Path URL: block
/// Example: curl http://127.0.0.1:18888/block
#[utoipa::path(
    get,
    path = "/block",
    responses(
        (status = 200, description = "latest stored block", body = BlockAdded)
    )
)]
pub fn latest_block<Db: DatabaseReader + Clone + Send + Sync>(
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
#[utoipa::path(
    get,
    path = "/block/{block_hash}",
    params(
        ("block_hash" = String, Path, description = "Base64 encoded block hash of requested block")
    ),
    responses(
        (status = 200, description = "fetch latest stored block", body = BlockAdded)
    )
)]
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
#[utoipa::path(
    get,
    path = "/block/{height}",
    params(
        ("height" = u32, Path, description = "Height of the requested block")
    ),
    responses(
        (status = 200, description = "fetch latest stored block", body = BlockAdded)
    )
)]
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
#[utoipa::path(
    get,
    path = "/deploy/{deploy_hash}",
    params(
        ("deploy_hash" = String, Path, description = "Base64 encoded deploy hash of requested deploy")
    ),
    responses(
        (status = 200, description = "fetch aggregate data for deploy events", body = DeployAggregate)
    )
)]
fn deploy_by_hash<Db: DatabaseReader + Clone + Send + Sync>(
    db: Db,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::path!("deploy" / String)
        .and(warp::get())
        .and(with_db(db))
        .and_then(handlers::get_deploy_by_hash)
}

/// Return information about an accepted deploy given its deploy hash.
/// Input: the database with data to be filtered.
/// Return: data about the accepted deploy.
/// Path URL: deploy/accepted/<deploy-hash>
/// Example: curl http://127.0.0.1:18888/deploy/accepted/f01544d37354c5f9b2c4956826d32f8e44198f94fb6752e87f422fe3071ab58a
#[utoipa::path(
    get,
    path = "/deploy/accepted/{deploy_hash}",
    params(
        ("deploy_hash" = String, Path, description = "Base64 encoded deploy hash of requested deploy accepted")
    ),
    responses(
        (status = 200, description = "fetch stored deploy", body = DeployAccepted)
    )
)]
fn deploy_accepted_by_hash<Db: DatabaseReader + Clone + Send + Sync>(
    db: Db,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::path!("deploy" / "accepted" / String)
        .and(warp::get())
        .and(with_db(db))
        .and_then(handlers::get_deploy_accepted_by_hash)
}

#[utoipa::path(
    get,
    path = "/deploy/expired/{deploy_hash}",
    params(
        ("deploy_hash" = String, Path, description = "Base64 encoded deploy hash of requested deploy expired")
    ),
    responses(
        (status = 200, description = "fetch stored deploy", body = DeployExpired)
    )
)]
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

#[utoipa::path(
    get,
    path = "/deploy/processed/{deploy_hash}",
    params(
        ("deploy_hash" = String, Path, description = "Base64 encoded deploy hash of requested deploy processed")
    ),
    responses(
        (status = 200, description = "fetch stored deploy", body = DeployProcessed)
    )
)]
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

#[utoipa::path(
    get,
    path = "/faults/{public_key}",
    params(
        ("public_key" = String, Path, description = "Base64 encoded validator's public key")
    ),
    responses(
        (status = 200, description = "faults associated with a validator's public key", body = [Fault])
    )
)]
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

#[utoipa::path(
    get,
    path = "/faults/{era}",
    params(
        ("era" = String, Path, description = "Era identifier")
    ),
    responses(
        (status = 200, description = "faults associated with an era ", body = [Fault])
    )
)]
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

#[utoipa::path(
    get,
    path = "/signatures/{block_hash}",
    params(
        ("block_hash" = String, Path, description = "Base64 encoded block hash of requested block")
    ),
    responses(
        (status = 200, description = "finality signatures in a block", body = [FinalitySignature])
    )
)]
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

#[utoipa::path(
    get,
    path = "/step/{era_id}",
    params(
        ("era_id" = String, Path, description = "Era id")
    ),
    responses(
        (status = 200, description = "step event emitted at the end of an era", body = Step)
    )
)]
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

/// Helper function to extract data from a database
fn with_db<Db: DatabaseReader + Clone + Send>(
    db: Db,
) -> impl Filter<Extract = (Db,), Error = Infallible> + Clone {
    warp::any().map(move || db.clone())
}
