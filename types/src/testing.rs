//! Testing utilities.
//!
//! Contains various parts and components to aid writing tests and simulations using the
//! `casper-node` library.

#[cfg(feature = "sse-data-testing")]
use casper_types::{testing::TestRng, Deploy, TimeDiff, Timestamp, Transaction};
#[cfg(test)]
use casper_types::{BlockHash, Digest, PublicKey};
#[cfg(feature = "sse-data-testing")]
use rand::Rng;
#[cfg(test)]
use serde_json::Value;

#[cfg(feature = "sse-data-testing")]
/// Creates a test deploy created at given instant and with given ttl.
pub fn create_test_transaction(
    created_ago: TimeDiff,
    ttl: TimeDiff,
    now: Timestamp,
    test_rng: &mut TestRng,
) -> Transaction {
    use casper_types::{TransactionV1, MINT_LANE_ID};

    if test_rng.gen() {
        Transaction::Deploy(Deploy::random_with_timestamp_and_ttl(
            test_rng,
            now - created_ago,
            ttl,
        ))
    } else {
        let timestamp = now - created_ago;
        let transaction = TransactionV1::random_with_lane_and_timestamp_and_ttl(
            test_rng,
            MINT_LANE_ID,
            Some(timestamp),
            Some(ttl),
        );
        Transaction::V1(transaction)
    }
}

#[cfg(feature = "sse-data-testing")]
/// Creates a random deploy that is considered expired.
pub fn create_expired_transaction(now: Timestamp, test_rng: &mut TestRng) -> Transaction {
    create_test_transaction(
        TimeDiff::from_seconds(20),
        TimeDiff::from_seconds(10),
        now,
        test_rng,
    )
}

#[cfg(test)]
pub fn parse_public_key(arg: &str) -> PublicKey {
    serde_json::from_value(Value::String(arg.to_string())).unwrap()
}

#[cfg(test)]
pub fn parse_block_hash(arg: &str) -> BlockHash {
    serde_json::from_value(Value::String(arg.to_string())).unwrap()
}

#[cfg(test)]
pub fn parse_digest(arg: &str) -> Digest {
    serde_json::from_value(Value::String(arg.to_string())).unwrap()
}
