use sea_query::BlobSize;

pub mod block_added;
pub mod deploy_accepted;
pub mod deploy_event;
pub mod deploy_expired;
pub mod deploy_processed;
pub mod event_log;
pub mod event_type;
pub mod fault;
pub mod finality_signature;
pub mod step;

// The `Integer` type only supports up to i64 which means a big enough u64 overflows causing a type mismatch error.
// The `BigUnsigned` type tries to convert the given `u64` into an `i64` under the hood so it has the same problem as above.
// to resolve this we can use a length constrained `Blob` to represent a `u64`.
pub const U64_BLOB_SIZE: BlobSize = BlobSize::Blob(Some(22));
