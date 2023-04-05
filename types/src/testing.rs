//! Testing utilities.
//!
//! Contains various parts and components to aid writing tests and simulations using the
//! `casper-node` library.

#[cfg(any(feature = "sse-data-testing", test))]
use casper_node::types::Deploy;
use crate::test_rng::TestRng;
use casper_types::{TimeDiff, Timestamp};

/// Creates a test deploy created at given instant and with given ttl.
pub fn create_test_deploy(
    created_ago: TimeDiff,
    ttl: TimeDiff,
    now: Timestamp,
    test_rng: &mut TestRng,
) -> Deploy {
    Deploy::random_with_timestamp_and_ttl(test_rng, now - created_ago, ttl)
}

/// Creates a random deploy that is considered expired.
pub fn create_expired_deploy(now: Timestamp, test_rng: &mut TestRng) -> Deploy {
    create_test_deploy(
        TimeDiff::from_seconds(20),
        TimeDiff::from_seconds(10),
        now,
        test_rng,
    )
}
