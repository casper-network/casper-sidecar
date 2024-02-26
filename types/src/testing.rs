//! Testing utilities.
//!
//! Contains various parts and components to aid writing tests and simulations using the
//! `casper-node` library.

use casper_types::{
    testing::TestRng, Deploy, TimeDiff, Timestamp, Transaction, TransactionV1Builder,
};
use rand::Rng;

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

/// Creates a random deploy that is considered expired.
pub fn create_expired_transaction(now: Timestamp, test_rng: &mut TestRng) -> Transaction {
    create_test_transaction(
        TimeDiff::from_seconds(20),
        TimeDiff::from_seconds(10),
        now,
        test_rng,
    )
}
