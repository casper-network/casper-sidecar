//! Testing utilities.
//!
//! Contains various parts and components to aid writing tests and simulations using the
//! `casper-node` library.
pub(crate) mod test_clock;
mod test_rng;
#[cfg(test)]
use casper_execution_engine::core::engine_state::ExecutableDeployItem;
#[cfg(test)]
use casper_hashing::Digest;
#[cfg(test)]
use casper_types::testing::TestRng;
#[cfg(test)]
use casper_types::{runtime_args, TimeDiff, Timestamp, U512, RuntimeArgs, SecretKey};
#[cfg(test)]
use casper_node::types::{Deploy, DeployHash};
#[cfg(test)]
use rand::{Rng, RngCore};

/// Creates a test deploy created at given instant and with given ttl.
#[cfg(test)]
pub(crate) fn create_test_deploy(
    created_ago: TimeDiff,
    ttl: TimeDiff,
    now: Timestamp,
    test_rng: &mut TestRng,
) -> Deploy {
    random_deploy_with_timestamp_and_ttl(
        test_rng,
        now - created_ago,
        ttl
    )
}

/// Creates a random deploy that is considered expired.
#[cfg(test)]
pub(crate) fn create_expired_deploy(now: Timestamp, test_rng: &mut TestRng) -> Deploy {
    create_test_deploy(
        TimeDiff::from_seconds(20),
        TimeDiff::from_seconds(10),
        now,
        test_rng,
    )
}

/// Generates a random instance but using the specified `timestamp` and `ttl`.
#[cfg(test)]
fn random_deploy_with_timestamp_and_ttl(
    rng: &mut TestRng,
    timestamp: Timestamp,
    ttl: TimeDiff,
) -> Deploy {
    let gas_price = rng.gen_range(1..100);

    let dependencies = vec![
        DeployHash::new(Digest::hash(rng.next_u64().to_le_bytes())),
        DeployHash::new(Digest::hash(rng.next_u64().to_le_bytes())),
        DeployHash::new(Digest::hash(rng.next_u64().to_le_bytes())),
    ];
    let chain_name = String::from("casper-example");

    // We need "amount" in order to be able to get correct info via `deploy_info()`.
    let payment_args = runtime_args! {
            "amount" => U512::from(10i8),
        };
    let payment = ExecutableDeployItem::StoredContractByName {
        name: String::from("casper-example"),
        entry_point: String::from("example-entry-point"),
        args: payment_args,
    };

    let session = rng.gen();

    let secret_key = SecretKey::random(rng);

    Deploy::new(
        timestamp,
        ttl,
        gas_price,
        dependencies,
        chain_name,
        payment,
        session,
        &secret_key,
        None,
    )
}
