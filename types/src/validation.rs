use thiserror::Error;

use casper_hashing::Digest;
use casper_types::bytesrepr;

use crate::block::{self, Block, BlockHash};

/// Error that can be returned when validating data returned from a JSON-RPC method.
#[derive(Error, Debug)]
pub enum ValidateResponseError {
    /// Failed to marshall value.
    #[error("failed to marshall value {0}")]
    BytesRepr(bytesrepr::Error),

    /// Error from serde.
    #[error(transparent)]
    Serde(#[from] serde_json::Error),

    /// Failed to parse JSON.
    #[error("validate response failed to parse")]
    ValidateResponseFailedToParse,

    /// The body hash in the header is not the same as the hash of the body of the block.
    #[error(
        "block header has incorrect body hash. \
         actual block body hash: {actual_block_body_hash}, \
         block: {block}"
    )]
    BodyHashMismatch {
        /// The `Block` with the `BlockHeader` with the incorrect block body hash.
        block: Box<Block>,
        /// The actual hash of the block's `BlockBody`.
        actual_block_body_hash: Digest,
    },

    /// The block's hash is not the same as the header's hash.
    #[error(
        "block has incorrect block hash. \
         actual block body hash: {actual_block_header_hash}, \
         block: {block}"
    )]
    BlockHashMismatch {
        /// The `Block` with the incorrect `BlockHeaderHash`
        block: Box<Block>,
        /// The actual hash of the block's `BlockHeader`
        actual_block_header_hash: BlockHash,
    },

    /// Serialized value not contained in proof.
    #[error("serialized value not contained in proof")]
    SerializedValueNotContainedInProof,

    /// No block in response.
    #[error("no block in response")]
    NoBlockInResponse,

    /// Block hash requested does not correspond to response.
    #[error("block hash requested does not correspond to response")]
    UnexpectedBlockHash,

    /// Block height was not as requested.
    #[error("block height was not as requested")]
    UnexpectedBlockHeight,

    /// An invalid combination of state identifier and block header response
    #[error("invalid combination of state identifier and block header in response")]
    InvalidGlobalStateResponse,
}

impl From<bytesrepr::Error> for ValidateResponseError {
    fn from(e: bytesrepr::Error) -> Self {
        ValidateResponseError::BytesRepr(e)
    }
}
