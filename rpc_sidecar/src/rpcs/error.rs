use crate::node_client::{Error as NodeClientError, InvalidTransactionOrDeploy};
use casper_json_rpc::{Error as RpcError, ReservedErrorCode, RpcErrorCode};
use http::StatusCode;
use casper_types::{
    AvailableBlockRange, BlockIdentifier, DeployHash, KeyTag, TransactionHash, URefFromStrError,
    bytesrepr,
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
    #[error("the provided dictionary key was invalid: {0}")]
    InvalidDictionaryKey(String),
    #[error("the provided dictionary key points at an unexpected type: {0}")]
    InvalidTypeUnderDictionaryKey(String),
    #[error("the provided dictionary key doesn't exist")]
    DictionaryKeyNotFound,
    #[error("the provided dictionary name doesn't exist")]
    DictionaryNameNotFound,
    #[error("the requested main purse was not found")]
    MainPurseNotFound,
    #[error("the requested account was not found")]
    AccountNotFound,
    #[error("the requested addressable entity was not found")]
    AddressableEntityNotFound,
    #[error("the requested reward was not found")]
    RewardNotFound,
    #[error("the requested account has been migrated to an addressable entity")]
    AccountMigratedToEntity,
    #[error("the provided dictionary value is {0} instead of a URef")]
    DictionaryValueIsNotAUref(KeyTag),
    #[error("the provided dictionary key could not be parsed: {0}")]
    DictionaryKeyCouldNotBeParsed(String),
    #[error("the transaction was invalid: {0}")]
    InvalidTransaction(InvalidTransactionOrDeploy),
    #[error("the deploy was invalid: {0}")]
    InvalidDeploy(InvalidTransactionOrDeploy),
    #[error("the requested purse balance could not be parsed")]
    InvalidPurseBalance,
    #[error("the requested account info could not be parsed")]
    InvalidAccountInfo,
    #[error("the requested addressable entity could not be parsed")]
    InvalidAddressableEntity,
    #[error("the auction state was invalid")]
    InvalidAuctionState,
    #[error("the named keys were invalid: {0}")]
    InvalidNamedKeys(String),
    #[error("the entry points were invalid: {0}")]
    InvalidEntryPoints(String),
    #[error("speculative execution returned nothing")]
    SpecExecReturnedNothing,
    #[error("unexpected bytesrepr failure: {0}")]
    BytesreprFailure(bytesrepr::Error),
}

impl Error {
    fn code(&self) -> Option<ErrorCode> {
        match self {
            Error::NoBlockFound(_, _) => Some(ErrorCode::NoSuchBlock),
            Error::NoTransactionWithHash(_) => Some(ErrorCode::NoSuchTransaction),
            Error::NoDeployWithHash(_) => Some(ErrorCode::NoSuchDeploy),
            Error::FoundTransactionInsteadOfDeploy => Some(ErrorCode::VariantMismatch),
            Error::NodeRequest(_, NodeClientError::UnknownStateRootHash) => {
                Some(ErrorCode::NoSuchStateRoot)
            }
            Error::GlobalStateEntryNotFound => Some(ErrorCode::QueryFailed),
            Error::NodeRequest(_, NodeClientError::QueryFailedToExecute) => {
                Some(ErrorCode::QueryFailedToExecute)
            }
            Error::NodeRequest(_, NodeClientError::FunctionIsDisabled) => {
                Some(ErrorCode::FunctionIsDisabled)
            }
            Error::NodeRequest(_, NodeClientError::SwitchBlockNotFound) => {
                Some(ErrorCode::SwitchBlockNotFound)
            }
            Error::NodeRequest(_, NodeClientError::SwitchBlockParentNotFound) => {
                Some(ErrorCode::SwitchBlockParentNotFound)
            }
            Error::NodeRequest(_, NodeClientError::UnsupportedRewardsV1Request) => {
                Some(ErrorCode::UnsupportedRewardsV1Request)
            }
            Error::NodeRequest(_, NodeClientError::PurseNotFound) => Some(ErrorCode::PurseNotFound),
            Error::InvalidPurseURef(_) => Some(ErrorCode::FailedToParseGetBalanceURef),
            Error::InvalidDictionaryKey(_) => Some(ErrorCode::FailedToParseQueryKey),
            Error::MainPurseNotFound => Some(ErrorCode::NoMainPurse),
            Error::AccountNotFound => Some(ErrorCode::NoSuchAccount),
            Error::AddressableEntityNotFound => Some(ErrorCode::NoSuchAddressableEntity),
            Error::RewardNotFound => Some(ErrorCode::NoRewardsFound),
            Error::AccountMigratedToEntity => Some(ErrorCode::AccountMigratedToEntity),
            Error::InvalidTypeUnderDictionaryKey(_)
            | Error::DictionaryKeyNotFound
            | Error::DictionaryNameNotFound
            | Error::DictionaryValueIsNotAUref(_)
            | Error::DictionaryKeyCouldNotBeParsed(_) => Some(ErrorCode::FailedToGetDictionaryURef),
            Error::InvalidTransaction(_) => Some(ErrorCode::InvalidTransaction),
            Error::NodeRequest(_, NodeClientError::SpecExecutionFailed(_))
            | Error::InvalidDeploy(_)
            | Error::SpecExecReturnedNothing => Some(ErrorCode::InvalidDeploy),
            Error::NodeRequest(_, _) => Some(ErrorCode::NodeRequestFailed),
            Error::InvalidPurseBalance => Some(ErrorCode::FailedToGetBalance),
            Error::InvalidAccountInfo
            | Error::InvalidAddressableEntity
            | Error::InvalidAuctionState
            | Error::InvalidNamedKeys(_)
            | Error::InvalidEntryPoints(_)
            | Error::BytesreprFailure(_) => None,
        }
    }
}

impl From<Error> for RpcError {
    fn from(value: Error) -> Self {
        match value {
            Error::NoBlockFound(_, available_block_range) => RpcError::new(
                ErrorCode::NoSuchBlock,
                ErrorData::MissingBlockOrStateRoot {
                    message: value.to_string(),
                    available_block_range,
                },
            ),
            // The node's own binary port rejected this request as throttled (it stays reachable
            // over the same connection - see `NodeClient::Error::RequestThrottled` - this just
            // means it's telling us to back off). Surface it as a real HTTP 429 rather than
            // burying it in a `200 OK` JSON-RPC envelope, so clients, proxies, and load balancers
            // that only look at the HTTP status still see the throttling.
            Error::NodeRequest(_, NodeClientError::RequestThrottled) => {
                RpcError::new(RpcErrorCode::RequestThrottled, value.to_string())
                    .with_http_status_override(StatusCode::TOO_MANY_REQUESTS.as_u16())
            }
            _ => match value.code() {
                Some(code) => RpcError::new(code, value.to_string()),
                None => RpcError::new(ReservedErrorCode::InternalError, value.to_string()),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// When the node's own binary port throttles a request, the sidecar must surface a genuine
    /// HTTP 429 - not just a JSON-RPC error object wrapped in a `200 OK` - so that anything
    /// watching only the HTTP status (a proxy, a load balancer, a naive client) still observes the
    /// throttling.
    #[test]
    fn node_request_throttled_maps_to_http_429() {
        let error = Error::NodeRequest("some_method", NodeClientError::RequestThrottled);

        let rpc_error = RpcError::from(error);

        assert_eq!(rpc_error.code(), RpcErrorCode::RequestThrottled as i64);
        assert_eq!(
            rpc_error.http_status_override(),
            Some(StatusCode::TOO_MANY_REQUESTS.as_u16())
        );
    }
}
