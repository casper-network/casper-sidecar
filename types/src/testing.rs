//! Testing utilities.
//!
//! Contains various parts and components to aid writing tests and simulations using the
//! `casper-node` library.

#[cfg(feature = "sse-data-testing")]
use casper_types::{
    testing::TestRng, Deploy, TimeDiff, Timestamp, Transaction, TransactionV1Builder,
};
#[cfg(test)]
use casper_types::{BlockHash, Digest, PublicKey};
#[cfg(feature = "sse-data-testing")]
use rand::Rng;

#[cfg(feature = "sse-data-testing")]
/// Creates a test deploy created at given instant and with given ttl.
pub fn create_test_transaction(
    created_ago: TimeDiff,
    ttl: TimeDiff,
    now: Timestamp,
    test_rng: &mut TestRng,
) -> Transaction {
    if test_rng.gen() {
        Transaction::Deploy(Deploy::random_with_timestamp_and_ttl(
            test_rng,
            now - created_ago,
            ttl,
        ))
    } else {
        let timestamp = now - created_ago;
        let transaction = TransactionV1Builder::new_random(test_rng)
            .with_timestamp(timestamp)
            .with_ttl(ttl)
            .build()
            .unwrap();
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
    let escaped = format!("\"{}\"", arg);
    serde_json::from_str(&escaped).unwrap()
}

#[cfg(test)]
pub fn parse_block_hash(arg: &str) -> BlockHash {
    let escaped = format!("\"{}\"", arg);
    serde_json::from_str(&escaped).unwrap()
}

#[cfg(test)]
pub fn parse_digest(arg: &str) -> Digest {
    let escaped = format!("\"{}\"", arg);
    serde_json::from_str(&escaped).unwrap()
}
