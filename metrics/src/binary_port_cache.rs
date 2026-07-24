use std::sync::LazyLock;

use prometheus::{IntCounter, IntCounterVec, Opts};

use super::REGISTRY;

static HANDLE_CALLS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    let counter = IntCounterVec::new(
        Opts::new(
            "binary_port_cache_handle_calls",
            "Number of times the binary port cache handled an SSE event, split by event kind",
        ),
        &["handler"],
    )
    .unwrap();
    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("cannot register metric");
    counter
});

static NEW_ENTRIES: LazyLock<IntCounterVec> = LazyLock::new(|| {
    let counter = IntCounterVec::new(
        Opts::new(
            "binary_port_cache_new_entries",
            "Number of new BlockWithSignaturesBuiltInFlight cache entries created for \
             previously-unknown blocks, split by the event that triggered them",
        ),
        &["source"],
    )
    .unwrap();
    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("cannot register metric");
    counter
});

static BLOCKS_FINALIZED: LazyLock<IntCounter> = LazyLock::new(|| {
    let counter = IntCounter::new(
        "binary_port_cache_blocks_finalized",
        "Number of blocks upgraded from NotSureBlock to BlockWithSignatures because enough \
         accumulated finality signatures cleared the finality threshold",
    )
    .expect("binary_port_cache_blocks_finalized metric can't be created");
    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("cannot register metric");
    counter
});

static BLOCKS_FETCHED_FROM_NODE: LazyLock<IntCounter> = LazyLock::new(|| {
    let counter = IntCounter::new(
        "binary_port_cache_blocks_fetched_from_node",
        "Number of BlockWithSignatures cache entries created directly from a node binary port \
         fetch, rather than assembled from SSE events",
    )
    .expect("binary_port_cache_blocks_fetched_from_node metric can't be created");
    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("cannot register metric");
    counter
});

static CLIENT_LOOKUPS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    let counter = IntCounterVec::new(
        Opts::new(
            "binary_port_cache_client_lookups",
            "Outcome of CachingNodeClient's cache lookups before falling back to the node, \
             split by which cache/resource was checked and whether it was a hit or a miss",
        ),
        &["resource", "outcome"],
    )
    .unwrap();
    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("cannot register metric");
    counter
});

/// Records an invocation of one of `HeedBinaryPortCache`'s `handle_*` SSE event handlers.
pub fn inc_handle_call(handler: &str) {
    HANDLE_CALLS.with_label_values(&[handler]).inc();
}

/// Records a brand new `BlockWithSignaturesBuiltInFlight` cache entry being created for a block
/// the cache didn't have any prior knowledge of, triggered by `source` (e.g. `"block_added"` or
/// `"finality_signature"`).
pub fn inc_new_entry(source: &str) {
    NEW_ENTRIES.with_label_values(&[source]).inc();
}

/// Records a `NotSureBlock` being upgraded to `BlockWithSignatures` once its accumulated
/// finality signatures clear the finality threshold.
pub fn inc_blocks_finalized() {
    BLOCKS_FINALIZED.inc();
}

/// Records a `BlockWithSignatures` cached directly from a node binary port fetch (the
/// read-through cache-aside populate on a miss), as opposed to being assembled from SSE events.
pub fn inc_blocks_fetched_from_node() {
    BLOCKS_FETCHED_FROM_NODE.inc();
}

/// Records the outcome of `CachingNodeClient` checking a cache for `resource` (e.g.
/// `"block_with_signatures"`, `"block_header"`, `"transaction_with_execution_info"`, or
/// `"latest_block"` for the in-memory "latest known block" cache) before falling back to the
/// node.
pub fn record_cache_lookup(resource: &str, hit: bool) {
    let outcome = if hit { "hit" } else { "miss" };
    CLIENT_LOOKUPS.with_label_values(&[resource, outcome]).inc();
}
