use super::{
    errors::handle_rejection, handlers, openapi::build_open_api_filters, status::status_filters,
};
use crate::{
    types::database::{DatabaseReader, TransactionTypeId},
    utils::{root_filter, InvalidPath},
};
use std::{convert::Infallible, str::FromStr};
use warp::Filter;

pub enum TransactionTypeIdFilter {
    Deploy,
    Version1,
}

impl From<TransactionTypeIdFilter> for TransactionTypeId {
    fn from(val: TransactionTypeIdFilter) -> Self {
        match val {
            TransactionTypeIdFilter::Deploy => TransactionTypeId::Deploy,
            TransactionTypeIdFilter::Version1 => TransactionTypeId::Version1,
        }
    }
}

impl FromStr for TransactionTypeIdFilter {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "deploy" => Ok(TransactionTypeIdFilter::Deploy),
            "version1" => Ok(TransactionTypeIdFilter::Version1),
            _ => Err(format!("Invalid transaction type id: {}", s)),
        }
    }
}

/// Helper function to specify available filters.
/// Input: the database with data to be filtered.
/// Return: the filtered data.
pub(super) fn combined_filters<Db: DatabaseReader + Clone + Send + Sync>(
    db: Db,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Infallible> + Clone {
    root_filter()
        .or(root_and_invalid_path())
        .or(block_filters(db.clone()))
        .or(transaction_filters(db.clone()))
        .or(step_by_era(db.clone()))
        .or(faults_by_public_key(db.clone()))
        .or(faults_by_era(db.clone()))
        .or(finality_signatures_by_block(db))
        .or(build_open_api_filters())
        .or(status_filters())
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

/// Helper function to specify available filters for transaction information.
/// Input: the database with data to be filtered.
/// Return: the filtered data.
fn transaction_filters<Db: DatabaseReader + Clone + Send + Sync>(
    db: Db,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    transaction_by_hash(db.clone())
        .or(transaction_accepted_by_hash(db.clone()))
        .or(transaction_processed_by_hash(db.clone()))
        .or(transaction_expired_by_hash(db))
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

/// Return an aggregate of the different states for the given transaction. This is a synthetic JSON not emitted by the node.
/// The output differs depending on the transaction's status, which changes over time as the transaction goes through its lifecycle.
/// Input: the database with data to be filtered.
/// Return: data about the transaction specified.
/// Path URL: transaction/<transaction-hash>
/// Example: curl http://127.0.0.1:18888/transaction/f01544d37354c5f9b2c4956826d32f8e44198f94fb6752e87f422fe3071ab58a
#[utoipa::path(
    get,
    path = "/transaction/{transaction_hash}",
    params(
        ("transaction_hash" = String, Path, description = "Base64 encoded transaction hash of requested transaction")
    ),
    responses(
        (status = 200, description = "fetch aggregate data for transaction events", body = TreansactionAggregate)
    )
)]
fn transaction_by_hash<Db: DatabaseReader + Clone + Send + Sync>(
    db: Db,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::path!("transaction" / TransactionTypeIdFilter / String)
        .and(warp::get())
        .and(with_db(db))
        .and_then(handlers::get_transaction_by_identifier)
}

/// Return information about an accepted transaction given its transaction hash.
/// Input: the database with data to be filtered.
/// Return: data about the accepted transaction.
/// Path URL: transaction/accepted/<transaction-hash>
/// Example: curl http://127.0.0.1:18888/transaction/accepted/f01544d37354c5f9b2c4956826d32f8e44198f94fb6752e87f422fe3071ab58a
#[utoipa::path(
    get,
    path = "/transaction/accepted/{transaction_hash}",
    params(
        ("transaction_hash" = String, Path, description = "Base64 encoded transaction hash of requested transaction accepted")
    ),
    responses(
        (status = 200, description = "fetch stored transaction", body = TransactionAccepted)
    )
)]
fn transaction_accepted_by_hash<Db: DatabaseReader + Clone + Send + Sync>(
    db: Db,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::path!("transaction" / "accepted" / TransactionTypeIdFilter / String)
        .and(warp::get())
        .and(with_db(db))
        .and_then(handlers::get_transaction_accepted_by_hash)
}

#[utoipa::path(
    get,
    path = "/transaction/expired/{transaction_hash}",
    params(
        ("transaction_hash" = String, Path, description = "Base64 encoded transaction hash of requested transaction expired")
    ),
    responses(
        (status = 200, description = "fetch stored transaction", body = TransactionExpired)
    )
)]
/// Return information about a transaction that expired given its transaction hash.
/// Input: the database with data to be filtered.
/// Return: data about the expired transaction.
/// Path URL: transaction/expired/<transaction-hash>
/// Example: curl http://127.0.0.1:18888/transaction/expired/e03544d37354c5f9b2c4956826d32f8e44198f94fb6752e87f422fe3071ab58a
fn transaction_expired_by_hash<Db: DatabaseReader + Clone + Send + Sync>(
    db: Db,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::path!("transaction" / "expired" / TransactionTypeIdFilter / String)
        .and(warp::get())
        .and(with_db(db))
        .and_then(handlers::get_transaction_expired_by_hash)
}

#[utoipa::path(
    get,
    path = "/transaction/processed/{transaction_hash}",
    params(
        ("transaction_hash" = String, Path, description = "Base64 encoded transaction hash of requested transaction processed")
    ),
    responses(
        (status = 200, description = "fetch stored transaction", body = TransactionProcessed)
    )
)]
/// Return information about a transaction that was processed given its transaction hash.
/// Input: the database with data to be filtered.
/// Return: data about the processed transaction.
/// Path URL: transaction/processed/<transaction-hash>
/// Example: curl http://127.0.0.1:18888/transaction/processed/f08944d37354c5f9b2c4956826d32f8e44198f94fb6752e87f422fe3071ab77a
fn transaction_processed_by_hash<Db: DatabaseReader + Clone + Send + Sync>(
    db: Db,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::path!("transaction" / "processed" / TransactionTypeIdFilter / String)
        .and(warp::get())
        .and(with_db(db))
        .and_then(handlers::get_transaction_processed_by_hash)
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
