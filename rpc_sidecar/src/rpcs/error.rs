use crate::node_client::Error as NodeClientError;
use casper_json_rpc::Error as RpcError;
use casper_types::{
    AvailableBlockRange, BlockIdentifier, DeployHash, KeyFromStrError, KeyTag, TransactionHash,
    URefFromStrError,
};

use super::{ErrorCode, ErrorData};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("request for {0} has failed: {1}")]
    NodeRequest(&'static str, NodeClientError),
    #[error("no block found for the provided identifier")]
    NoBlockFound(Option<BlockIdentifier>, AvailableBlockRange),
    #[error("no transaction for hash {0}")]
    NoTransactionWithHash(TransactionHash),
    #[error("no deploy for hash {0}")]
    NoDeployWithHash(DeployHash),
    #[error("found a transaction when searching for a deploy")]
    FoundTransactionInsteadOfDeploy,
    #[error("value was not found in the global state")]
    GlobalStateEntryNotFound,
    #[error("the requested purse URef was invalid: {0}")]
    InvalidPurseURef(URefFromStrError),
    #[error("the requested purse balance could not be parsed")]
    InvalidPurseBalance,
    #[error("the requested main purse was invalid")]
    InvalidMainPurse,
    #[error("the requested account info could not be parsed")]
    InvalidAccountInfo,
    #[error("the provided dictionary key was invalid: {0}")]
    InvalidDictionaryKey(KeyFromStrError),
    #[error("the provided dictionary key points at an unexpected type: {0}")]
    InvalidTypeUnderDictionaryKey(String),
    #[error("the provided dictionary key doesn't exist")]
    DictionaryKeyNotFound,
    #[error("the provided dictionary name doesn't exist")]
    DictionaryNameNotFound,
    #[error("the provided dictionary value is {0} instead of a URef")]
    DictionaryValueIsNotAUref(KeyTag),
    #[error("the provided dictionary key could not be parsed: {0}")]
    DictionaryKeyCouldNotBeParsed(String),
    #[error("the transaction was invalid: {0}")]
    InvalidTransaction(String),
    #[error("the deploy was invalid: {0}")]
    InvalidDeploy(String),
    #[error("the auction bids were invalid")]
    InvalidAuctionBids,
    #[error("the auction contract was invalid")]
    InvalidAuctionContract,
    #[error("the auction validators were invalid")]
    InvalidAuctionValidators,
    #[error("speculative execution returned nothing")]
    SpecExecReturnedNothing,
}

impl Error {
    fn code(&self) -> ErrorCode {
        match self {
            Error::NoBlockFound(_, _) => ErrorCode::NoSuchBlock,
            Error::NoTransactionWithHash(_) => ErrorCode::NoSuchTransaction,
            Error::NoDeployWithHash(_) => ErrorCode::NoSuchDeploy,
            Error::FoundTransactionInsteadOfDeploy => ErrorCode::VariantMismatch,
            Error::NodeRequest(_, NodeClientError::UnknownStateRootHash) => {
                ErrorCode::NoSuchStateRoot
            }
            Error::GlobalStateEntryNotFound => ErrorCode::QueryFailed,
            Error::NodeRequest(_, NodeClientError::QueryFailedToExecute) => {
                ErrorCode::QueryFailedToExecute
            }
            Error::NodeRequest(_, NodeClientError::FunctionIsDisabled) => {
                ErrorCode::FunctionIsDisabled
            }
            Error::InvalidPurseURef(_) => ErrorCode::FailedToParseGetBalanceURef,
            Error::InvalidPurseBalance => ErrorCode::FailedToGetBalance,
            Error::InvalidAccountInfo => ErrorCode::NoSuchAccount,
            Error::InvalidDictionaryKey(_) => ErrorCode::FailedToParseQueryKey,
            Error::InvalidMainPurse => ErrorCode::NoSuchMainPurse,
            Error::InvalidTypeUnderDictionaryKey(_)
            | Error::DictionaryKeyNotFound
            | Error::DictionaryNameNotFound
            | Error::DictionaryValueIsNotAUref(_)
            | Error::DictionaryKeyCouldNotBeParsed(_) => ErrorCode::FailedToGetDictionaryURef,
            Error::InvalidTransaction(_) => ErrorCode::InvalidTransaction,
            Error::NodeRequest(_, NodeClientError::SpecExecutionFailed(_))
            | Error::InvalidDeploy(_)
            | Error::SpecExecReturnedNothing => ErrorCode::InvalidDeploy,
            Error::InvalidAuctionBids
            | Error::InvalidAuctionContract
            | Error::InvalidAuctionValidators => ErrorCode::InvalidAuctionState,
            Error::NodeRequest(_, _) => ErrorCode::NodeRequestFailed,
        }
    }
}

impl From<Error> for RpcError {
    fn from(value: Error) -> Self {
        match value {
            Error::NoBlockFound(_, available_block_range) => RpcError::new(
                value.code(),
                ErrorData::MissingBlockOrStateRoot {
                    message: value.to_string(),
                    available_block_range,
                },
            ),
            _ => RpcError::new(value.code(), value.to_string()),
        }
    }
}
