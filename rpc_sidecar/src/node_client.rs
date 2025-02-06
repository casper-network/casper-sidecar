use crate::{config::ExponentialBackoffConfig, encode_request, NodeClientConfig};
use anyhow::Error as AnyhowError;
use async_trait::async_trait;
use casper_binary_port::{
    AccountInformation, AddressableEntityInformation, BalanceResponse, BinaryMessage,
    BinaryMessageCodec, BinaryResponse, BinaryResponseAndRequest, Command, CommandHeader,
    ConsensusValidatorChanges, ContractInformation, DictionaryItemIdentifier,
    DictionaryQueryResult, EntityIdentifier, EraIdentifier, ErrorCode, GetRequest,
    GetTrieFullResult, GlobalStateEntityQualifier, GlobalStateQueryResult, GlobalStateRequest,
    InformationRequest, InformationRequestTag, KeyPrefix, NodeStatus, PackageIdentifier,
    PayloadEntity, PurseIdentifier, RecordId, ResponseType, RewardResponse,
    SpeculativeExecutionResult, TransactionWithExecutionInfo, ValueWithProof,
};
use casper_types::{
    bytesrepr::{self, FromBytes, ToBytes},
    contracts::ContractPackage,
    system::auction::DelegatorKind,
    AvailableBlockRange, BlockHash, BlockHeader, BlockIdentifier, BlockWithSignatures,
    ChainspecRawBytes, Digest, GlobalStateIdentifier, Key, KeyTag, Package, Peers, PublicKey,
    StoredValue, Transaction, TransactionHash, Transfer,
};
use futures::{Future, SinkExt, StreamExt};
use metrics::rpc::{
    inc_disconnect, observe_reconnect_time, register_mismatched_id, register_timeout,
};
use serde::de::DeserializeOwned;
use std::{
    convert::{TryFrom, TryInto},
    fmt::{self, Display, Formatter},
    net::{IpAddr, SocketAddr},
    sync::{
        atomic::{AtomicU16, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{
    net::TcpStream,
    sync::{futures::Notified, RwLock, RwLockWriteGuard},
};
use tokio_util::codec::Framed;
use tracing::{error, field, info, warn};

const MAX_MISMATCHED_ID_RETRIES: u8 = 100;
#[cfg(not(test))]
const INITIAL_REQUEST_ID: u16 = 0;
#[cfg(test)]
const INITIAL_REQUEST_ID: u16 = 1;

#[async_trait]
pub trait NodeClient: Send + Sync {
    async fn send_request(&self, req: Command) -> Result<BinaryResponseAndRequest, Error>;

    async fn read_record(
        &self,
        record_id: RecordId,
        key: &[u8],
    ) -> Result<BinaryResponseAndRequest, Error> {
        let get = GetRequest::Record {
            record_type_tag: record_id.into(),
            key: key.to_vec(),
        };
        self.send_request(Command::Get(get)).await
    }

    async fn read_info(&self, req: InformationRequest) -> Result<BinaryResponseAndRequest, Error> {
        let get = req.try_into().expect("should always be able to convert");
        self.send_request(Command::Get(get)).await
    }

    async fn query_global_state(
        &self,
        state_identifier: Option<GlobalStateIdentifier>,
        base_key: Key,
        path: Vec<String>,
    ) -> Result<Option<GlobalStateQueryResult>, Error> {
        let req = GlobalStateRequest::new(
            state_identifier,
            GlobalStateEntityQualifier::Item { base_key, path },
        );
        let resp = self
            .send_request(Command::Get(GetRequest::State(Box::new(req))))
            .await?;
        parse_response::<GlobalStateQueryResult>(&resp.into())
    }

    async fn query_global_state_by_tag(
        &self,
        state_identifier: Option<GlobalStateIdentifier>,
        key_tag: KeyTag,
    ) -> Result<Vec<StoredValue>, Error> {
        let get = GlobalStateRequest::new(
            state_identifier,
            GlobalStateEntityQualifier::AllItems { key_tag },
        );
        let resp = self
            .send_request(Command::Get(GetRequest::State(Box::new(get))))
            .await?;
        parse_response::<Vec<StoredValue>>(&resp.into())?.ok_or(Error::EmptyEnvelope)
    }

    async fn query_global_state_by_prefix(
        &self,
        state_identifier: Option<GlobalStateIdentifier>,
        key_prefix: KeyPrefix,
    ) -> Result<Vec<StoredValue>, Error> {
        let get = GlobalStateRequest::new(
            state_identifier,
            GlobalStateEntityQualifier::ItemsByPrefix { key_prefix },
        );
        let resp = self
            .send_request(Command::Get(GetRequest::State(Box::new(get))))
            .await?;
        parse_response::<Vec<StoredValue>>(&resp.into())?.ok_or(Error::EmptyEnvelope)
    }

    async fn read_balance(
        &self,
        state_identifier: Option<GlobalStateIdentifier>,
        purse_identifier: PurseIdentifier,
    ) -> Result<BalanceResponse, Error> {
        let get = GlobalStateRequest::new(
            state_identifier,
            GlobalStateEntityQualifier::Balance { purse_identifier },
        );
        let resp = self
            .send_request(Command::Get(GetRequest::State(Box::new(get))))
            .await?;
        parse_response::<BalanceResponse>(&resp.into())?.ok_or(Error::EmptyEnvelope)
    }

    async fn read_trie_bytes(&self, trie_key: Digest) -> Result<Option<Vec<u8>>, Error> {
        let resp = self
            .send_request(Command::Get(GetRequest::Trie { trie_key }))
            .await?;
        let res = parse_response::<GetTrieFullResult>(&resp.into())?.ok_or(Error::EmptyEnvelope)?;
        Ok(res.into_inner().map(<Vec<u8>>::from))
    }

    async fn query_dictionary_item(
        &self,
        state_identifier: Option<GlobalStateIdentifier>,
        identifier: DictionaryItemIdentifier,
    ) -> Result<Option<DictionaryQueryResult>, Error> {
        let get = GlobalStateRequest::new(
            state_identifier,
            GlobalStateEntityQualifier::DictionaryItem { identifier },
        );
        let resp = self
            .send_request(Command::Get(GetRequest::State(Box::new(get))))
            .await?;
        parse_response::<DictionaryQueryResult>(&resp.into())
    }

    async fn try_accept_transaction(&self, transaction: Transaction) -> Result<(), Error> {
        let request = Command::TryAcceptTransaction { transaction };
        let response = self.send_request(request).await?;

        if response.is_success() {
            return Ok(());
        } else {
            return Err(Error::from_error_code(response.error_code()));
        }
    }

    async fn exec_speculatively(
        &self,
        transaction: Transaction,
    ) -> Result<SpeculativeExecutionResult, Error> {
        let request = Command::TrySpeculativeExec { transaction };
        let resp = self.send_request(request).await?;
        parse_response::<SpeculativeExecutionResult>(&resp.into())?.ok_or(Error::EmptyEnvelope)
    }

    async fn read_block_transfers(&self, hash: BlockHash) -> Result<Option<Vec<Transfer>>, Error> {
        let key = hash.to_bytes().expect("should always serialize a digest");
        let resp = self.read_record(RecordId::Transfer, &key).await?;
        parse_response_bincode::<Vec<Transfer>>(&resp.into())
    }

    async fn read_block_header(
        &self,
        block_identifier: Option<BlockIdentifier>,
    ) -> Result<Option<BlockHeader>, Error> {
        let resp = self
            .read_info(InformationRequest::BlockHeader(block_identifier))
            .await?;
        parse_response::<BlockHeader>(&resp.into())
    }

    async fn read_block_with_signatures(
        &self,
        block_identifier: Option<BlockIdentifier>,
    ) -> Result<Option<BlockWithSignatures>, Error> {
        let resp = self
            .read_info(InformationRequest::BlockWithSignatures(block_identifier))
            .await?;
        parse_response::<BlockWithSignatures>(&resp.into())
    }

    async fn read_transaction_with_execution_info(
        &self,
        hash: TransactionHash,
        with_finalized_approvals: bool,
    ) -> Result<Option<TransactionWithExecutionInfo>, Error> {
        let resp = self
            .read_info(InformationRequest::Transaction {
                hash,
                with_finalized_approvals,
            })
            .await?;
        parse_response::<TransactionWithExecutionInfo>(&resp.into())
    }

    async fn read_peers(&self) -> Result<Peers, Error> {
        let resp = self.read_info(InformationRequest::Peers).await?;
        parse_response::<Peers>(&resp.into())?.ok_or(Error::EmptyEnvelope)
    }

    async fn read_available_block_range(&self) -> Result<AvailableBlockRange, Error> {
        let resp = self
            .read_info(InformationRequest::AvailableBlockRange)
            .await?;
        parse_response::<AvailableBlockRange>(&resp.into())?.ok_or(Error::EmptyEnvelope)
    }

    async fn read_chainspec_bytes(&self) -> Result<ChainspecRawBytes, Error> {
        let resp = self
            .read_info(InformationRequest::ChainspecRawBytes)
            .await?;
        parse_response::<ChainspecRawBytes>(&resp.into())?.ok_or(Error::EmptyEnvelope)
    }

    async fn read_validator_changes(&self) -> Result<ConsensusValidatorChanges, Error> {
        let resp = self
            .read_info(InformationRequest::ConsensusValidatorChanges)
            .await?;
        parse_response::<ConsensusValidatorChanges>(&resp.into())?.ok_or(Error::EmptyEnvelope)
    }

    async fn read_latest_switch_block_header(&self) -> Result<Option<BlockHeader>, Error> {
        let resp = self
            .read_info(InformationRequest::LatestSwitchBlockHeader)
            .await?;
        parse_response::<BlockHeader>(&resp.into())
    }

    async fn read_node_status(&self) -> Result<NodeStatus, Error> {
        let resp = self.read_info(InformationRequest::NodeStatus).await?;
        parse_response::<NodeStatus>(&resp.into())?.ok_or(Error::EmptyEnvelope)
    }

    async fn read_reward(
        &self,
        era_identifier: Option<EraIdentifier>,
        validator: PublicKey,
        delegator: Option<PublicKey>,
    ) -> Result<Option<RewardResponse>, Error> {
        let validator = validator.into();
        let delegator = delegator.map(|delegator| Box::new(DelegatorKind::PublicKey(delegator)));
        let resp = self
            .read_info(InformationRequest::Reward {
                era_identifier,
                validator,
                delegator,
            })
            .await?;
        parse_response::<RewardResponse>(&resp.into())
    }

    async fn read_package(
        &self,
        state_identifier: Option<GlobalStateIdentifier>,
        identifier: PackageIdentifier,
    ) -> Result<Option<PackageResponse>, Error> {
        let get = InformationRequest::Package {
            state_identifier,
            identifier,
        };
        let resp = self.read_info(get).await?;
        match resp.response().returned_data_type_tag() {
            Some(type_tag) if type_tag == ResponseType::ContractPackageWithProof as u8 => Ok(
                parse_response::<ValueWithProof<ContractPackage>>(&resp.into())?
                    .map(PackageResponse::ContractPackage),
            ),
            _ => Ok(parse_response::<ValueWithProof<Package>>(&resp.into())?
                .map(PackageResponse::Package)),
        }
    }

    async fn read_entity(
        &self,
        state_identifier: Option<GlobalStateIdentifier>,
        identifier: EntityIdentifier,
        include_bytecode: bool,
    ) -> Result<Option<EntityResponse>, Error> {
        let get = InformationRequest::Entity {
            state_identifier,
            identifier,
            include_bytecode,
        };
        let resp = self.read_info(get).await?;
        match resp.response().returned_data_type_tag() {
            Some(type_tag) if type_tag == ResponseType::ContractInformation as u8 => {
                Ok(parse_response::<ContractInformation>(&resp.into())?
                    .map(EntityResponse::Contract))
            }
            Some(type_tag) if type_tag == ResponseType::AccountInformation as u8 => Ok(
                parse_response::<AccountInformation>(&resp.into())?.map(EntityResponse::Account),
            ),
            _ => Ok(
                parse_response::<AddressableEntityInformation>(&resp.into())?
                    .map(EntityResponse::Entity),
            ),
        }
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]

pub enum InvalidTransactionOrDeploy {
    ///The deploy had an invalid chain name
    #[error("The deploy had an invalid chain name")]
    DeployChainName,
    ///Deploy dependencies are no longer supported
    #[error("The dependencies for this transaction are no longer supported")]
    DeployDependenciesNoLongerSupported,
    ///The deploy sent to the network had an excessive size
    #[error("The deploy had an excessive size")]
    DeployExcessiveSize,
    ///The deploy sent to the network had an excessive time to live
    #[error("The deploy had an excessive time to live")]
    DeployExcessiveTimeToLive,
    ///The deploy sent to the network had a timestamp referencing a time that has yet to occur.
    #[error("The deploys timestamp is in the future")]
    DeployTimestampInFuture,
    ///The deploy sent to the network had an invalid body hash
    #[error("The deploy had an invalid body hash")]
    DeployBodyHash,
    ///The deploy sent to the network had an invalid deploy hash i.e. the provided deploy hash
    /// didn't match the derived deploy hash
    #[error("The deploy had an invalid deploy hash")]
    DeployHash,
    ///The deploy sent to the network had an empty approval set
    #[error("The deploy had no approvals")]
    DeployEmptyApprovals,
    ///The deploy sent to the network had an invalid approval
    #[error("The deploy had an invalid approval")]
    DeployApproval,
    ///The deploy sent to the network had an excessive session args length
    #[error("The deploy had an excessive session args length")]
    DeployExcessiveSessionArgsLength,
    ///The deploy sent to the network had an excessive payment args length
    #[error("The deploy had an excessive payment args length")]
    DeployExcessivePaymentArgsLength,
    ///The deploy sent to the network had a missing payment amount
    #[error("The deploy had a missing payment amount")]
    DeployMissingPaymentAmount,
    ///The deploy sent to the network had a payment amount that was not parseable
    #[error("The deploy sent to the network had a payment amount that was unable to be parsed")]
    DeployFailedToParsePaymentAmount,
    ///The deploy sent to the network exceeded the block gas limit
    #[error("The deploy sent to the network exceeded the block gas limit")]
    DeployExceededBlockGasLimit,
    ///The deploy sent to the network was missing a transfer amount
    #[error("The deploy sent to the network was missing a transfer amount")]
    DeployMissingTransferAmount,
    ///The deploy sent to the network had a transfer amount that was unable to be parseable
    #[error("The deploy sent to the network had a transfer amount that was unable to be parsed")]
    DeployFailedToParseTransferAmount,
    ///The deploy sent to the network had a transfer amount that was insufficient
    #[error("The deploy sent to the network had an insufficient transfer amount")]
    DeployInsufficientTransferAmount,
    ///The deploy sent to the network had excessive approvals
    #[error("The deploy sent to the network had excessive approvals")]
    DeployExcessiveApprovals,
    ///The network was unable to calculate the gas limit for the deploy
    #[error("The network was unable to calculate the gas limit associated with the deploy")]
    DeployUnableToCalculateGasLimit,
    ///The network was unable to calculate the gas cost for the deploy
    #[error("The network was unable to calculate the gas cost for the deploy")]
    DeployUnableToCalculateGasCost,
    ///The deploy sent to the network was invalid for an unspecified reason
    #[error("The deploy sent to the network was invalid for an unspecified reason")]
    DeployUnspecified,
    /// The transaction sent to the network had an invalid chain name
    #[error("The transaction sent to the network had an invalid chain name")]
    TransactionChainName,
    /// The transaction sent to the network had an excessive size
    #[error("The transaction sent to the network had an excessive size")]
    TransactionExcessiveSize,
    /// The transaction sent to the network had an excessive time to live
    #[error("The transaction sent to the network had an excessive time to live")]
    TransactionExcessiveTimeToLive,
    /// The transaction sent to the network had a timestamp located in the future.
    #[error("The transaction sent to the network had a timestamp that has not yet occurred")]
    TransactionTimestampInFuture,
    /// The transaction sent to the network had a provided body hash that conflicted with hash
    /// derived by the network
    #[error("The transaction sent to the network had an invalid body hash")]
    TransactionBodyHash,
    /// The transaction sent to the network had a provided hash that conflicted with the hash
    /// derived by the network
    #[error("The transaction sent to the network had an invalid hash")]
    TransactionHash,
    /// The transaction sent to the network had an empty approvals set
    #[error("The transaction sent to the network had no approvals")]
    TransactionEmptyApprovals,
    /// The transaction sent to the network had an invalid approval
    #[error("The transaction sent to the network had an invalid approval")]
    TransactionInvalidApproval,
    /// The transaction sent to the network had excessive args length
    #[error("The transaction sent to the network had excessive args length")]
    TransactionExcessiveArgsLength,
    /// The transaction sent to the network had excessive approvals
    #[error("The transaction sent to the network had excessive approvals")]
    TransactionExcessiveApprovals,
    /// The transaction sent to the network exceeds the block gas limit
    #[error("The transaction sent to the network exceeds the networks block gas limit")]
    TransactionExceedsBlockGasLimit,
    /// The transaction sent to the network had a missing arg
    #[error("The transaction sent to the network was missing an argument")]
    TransactionMissingArg,
    /// The transaction sent to the network had an argument with an unexpected type
    #[error("The transaction sent to the network had an unexpected argument type")]
    TransactionUnexpectedArgType,
    /// The transaction sent to the network had an invalid argument
    #[error("The transaction sent to the network had an invalid argument")]
    TransactionInvalidArg,
    /// The transaction sent to the network had an insufficient transfer amount
    #[error("The transaction sent to the network had an insufficient transfer amount")]
    TransactionInsufficientTransferAmount,
    /// The transaction sent to the network had a custom entry point when it should have a non
    /// custom entry point.
    #[error("The native transaction sent to the network should not have a custom entry point")]
    TransactionEntryPointCannotBeCustom,
    /// The transaction sent to the network had a standard entry point when it must be custom.
    #[error("The non-native transaction sent to the network must have a custom entry point")]
    TransactionEntryPointMustBeCustom,
    /// The transaction sent to the network had empty module bytes
    #[error("The transaction sent to the network had empty module bytes")]
    TransactionEmptyModuleBytes,
    /// The transaction sent to the network had an invalid gas price conversion
    #[error("The transaction sent to the network had an invalid gas price conversion")]
    TransactionGasPriceConversion,
    /// The network was unable to calculate the gas limit for the transaction sent.
    #[error("The network was unable to calculate the gas limit for the transaction sent")]
    TransactionUnableToCalculateGasLimit,
    /// The network was unable to calculate the gas cost for the transaction sent.
    #[error("The network was unable to calculate the gas cost for the transaction sent.")]
    TransactionUnableToCalculateGasCost,
    /// The transaction sent to the network had an invalid pricing mode
    #[error("The transaction sent to the network had an invalid pricing mode")]
    TransactionPricingMode,
    /// The transaction sent to the network was invalid for an unspecified reason
    #[error("The transaction sent to the network was invalid for an unspecified reason")]
    TransactionUnspecified,
    /// The catchall error from a casper node
    #[error("The transaction or deploy sent to the network was invalid for an unspecified reason")]
    TransactionOrDeployUnspecified,
    /// Blockchain is empty
    #[error("blockchain is empty")]
    EmptyBlockchain,
    /// Expected deploy, but got transaction
    #[error("expected deploy, got transaction")]
    ExpectedDeploy,
    /// Expected transaction, but got deploy
    #[error("expected transaction V1, got deploy")]
    ExpectedTransaction,
    /// Transaction has expired
    #[error("transaction has expired")]
    TransactionExpired,
    /// Transactions parameters are missing or incorrect
    #[error("missing or incorrect transaction parameters")]
    MissingOrIncorrectParameters,
    /// No such addressable entity
    #[error("no such addressable entity")]
    NoSuchAddressableEntity,
    // No such contract at hash
    #[error("no such contract at hash")]
    NoSuchContractAtHash,
    /// No such entry point
    #[error("no such entry point")]
    NoSuchEntryPoint,
    /// No such package at hash
    #[error("no such package at hash")]
    NoSuchPackageAtHash,
    /// Invalid entity at version
    #[error("invalid entity at version")]
    InvalidEntityAtVersion,
    /// Disabled entity at version
    #[error("disabled entity at version")]
    DisabledEntityAtVersion,
    /// Missing entity at version
    #[error("missing entity at version")]
    MissingEntityAtVersion,
    /// Invalid associated keys
    #[error("invalid associated keys")]
    InvalidAssociatedKeys,
    /// Insufficient signature weight
    #[error("insufficient signature weight")]
    InsufficientSignatureWeight,
    /// Insufficient balance
    #[error("insufficient balance")]
    InsufficientBalance,
    /// Unknown balance
    #[error("unknown balance")]
    UnknownBalance,
    /// Invalid payment variant for deploy
    #[error("invalid payment variant for deploy")]
    DeployInvalidPaymentVariant,
    /// Missing transfer target for deploy
    #[error("missing transfer target for deploy")]
    DeployMissingTransferTarget,
    /// Missing module bytes for deploy
    #[error("missing module bytes for deploy")]
    DeployMissingModuleBytes,
    /// Entry point cannot be 'call'
    #[error("entry point cannot be 'call'")]
    InvalidTransactionEntryPointCannotBeCall,
    /// Invalid transaction lane
    #[error("invalid transaction lane")]
    InvalidTransactionInvalidTransactionLane,
    #[error("expected named arguments")]
    ExpectedNamedArguments,
    #[error("invalid transaction runtime")]
    InvalidTransactionRuntime,
    #[error("couldn't associate a transaction lane with the transaction")]
    InvalidTransactionNoWasmLaneMatches,
    #[error("entry point must be 'call'")]
    InvalidTransactionEntryPointMustBeCall,
    #[error("One of the payloads field cannot be deserialized")]
    InvalidTransactionCannotDeserializeField,
    #[error("Can't calculate hash of the payload fields")]
    InvalidTransactionCannotCalculateFieldsHash,
    #[error("Unexpected fields in payload")]
    InvalidTransactionUnexpectedFields,
    #[error("expected bytes arguments")]
    InvalidTransactionExpectedBytesArguments,
    #[error("Missing seed field in transaction")]
    InvalidTransactionMissingSeed,
    #[error("Pricing mode not supported")]
    PricingModeNotSupported,
    #[error("Gas limit not supported")]
    InvalidDeployGasLimitNotSupported,
    #[error("Invalid runtime for Transaction::Deploy")]
    InvalidDeployInvalidRuntime,
}

impl From<ErrorCode> for InvalidTransactionOrDeploy {
    fn from(value: ErrorCode) -> Self {
        match value {
            ErrorCode::InvalidDeployChainName => Self::DeployChainName,
            ErrorCode::InvalidDeployDependenciesNoLongerSupported => {
                Self::DeployDependenciesNoLongerSupported
            }
            ErrorCode::InvalidDeployExcessiveSize => Self::DeployExcessiveSize,
            ErrorCode::InvalidDeployExcessiveTimeToLive => Self::DeployExcessiveTimeToLive,
            ErrorCode::InvalidDeployTimestampInFuture => Self::DeployTimestampInFuture,
            ErrorCode::InvalidDeployBodyHash => Self::DeployBodyHash,
            ErrorCode::InvalidDeployHash => Self::DeployHash,
            ErrorCode::InvalidDeployEmptyApprovals => Self::DeployEmptyApprovals,
            ErrorCode::InvalidDeployApproval => Self::DeployApproval,
            ErrorCode::InvalidDeployExcessiveSessionArgsLength => {
                Self::DeployExcessiveSessionArgsLength
            }
            ErrorCode::InvalidDeployExcessivePaymentArgsLength => {
                Self::DeployExcessivePaymentArgsLength
            }
            ErrorCode::InvalidDeployMissingPaymentAmount => Self::DeployMissingPaymentAmount,
            ErrorCode::InvalidDeployFailedToParsePaymentAmount => {
                Self::DeployFailedToParsePaymentAmount
            }
            ErrorCode::InvalidDeployExceededBlockGasLimit => Self::DeployExceededBlockGasLimit,
            ErrorCode::InvalidDeployMissingTransferAmount => Self::DeployMissingTransferAmount,
            ErrorCode::InvalidDeployFailedToParseTransferAmount => {
                Self::DeployFailedToParseTransferAmount
            }
            ErrorCode::InvalidDeployInsufficientTransferAmount => {
                Self::DeployInsufficientTransferAmount
            }
            ErrorCode::InvalidDeployExcessiveApprovals => Self::DeployExcessiveApprovals,
            ErrorCode::InvalidDeployUnableToCalculateGasLimit => {
                Self::DeployUnableToCalculateGasLimit
            }
            ErrorCode::InvalidDeployUnableToCalculateGasCost => {
                Self::DeployUnableToCalculateGasCost
            }
            ErrorCode::InvalidDeployUnspecified => Self::DeployUnspecified,
            ErrorCode::InvalidTransactionChainName => Self::TransactionChainName,
            ErrorCode::InvalidTransactionExcessiveSize => Self::TransactionExcessiveSize,
            ErrorCode::InvalidTransactionExcessiveTimeToLive => {
                Self::TransactionExcessiveTimeToLive
            }
            ErrorCode::InvalidTransactionTimestampInFuture => Self::TransactionTimestampInFuture,
            ErrorCode::InvalidTransactionBodyHash => Self::TransactionBodyHash,
            ErrorCode::InvalidTransactionHash => Self::TransactionHash,
            ErrorCode::InvalidTransactionEmptyApprovals => Self::TransactionEmptyApprovals,
            ErrorCode::InvalidTransactionInvalidApproval => Self::TransactionInvalidApproval,
            ErrorCode::InvalidTransactionExcessiveArgsLength => {
                Self::TransactionExcessiveArgsLength
            }
            ErrorCode::InvalidTransactionExcessiveApprovals => Self::TransactionExcessiveApprovals,
            ErrorCode::InvalidTransactionExceedsBlockGasLimit => {
                Self::TransactionExceedsBlockGasLimit
            }
            ErrorCode::InvalidTransactionMissingArg => Self::TransactionMissingArg,
            ErrorCode::InvalidTransactionUnexpectedArgType => Self::TransactionUnexpectedArgType,
            ErrorCode::InvalidTransactionInvalidArg => Self::TransactionInvalidArg,
            ErrorCode::InvalidTransactionInsufficientTransferAmount => {
                Self::TransactionInsufficientTransferAmount
            }
            ErrorCode::InvalidTransactionEntryPointCannotBeCustom => {
                Self::TransactionEntryPointCannotBeCustom
            }
            ErrorCode::InvalidTransactionEntryPointMustBeCustom => {
                Self::TransactionEntryPointMustBeCustom
            }
            ErrorCode::InvalidTransactionEmptyModuleBytes => Self::TransactionEmptyModuleBytes,
            ErrorCode::InvalidTransactionGasPriceConversion => Self::TransactionGasPriceConversion,
            ErrorCode::InvalidTransactionUnableToCalculateGasLimit => {
                Self::TransactionUnableToCalculateGasLimit
            }
            ErrorCode::InvalidTransactionUnableToCalculateGasCost => {
                Self::TransactionUnableToCalculateGasCost
            }
            ErrorCode::InvalidTransactionPricingMode => Self::TransactionPricingMode,
            ErrorCode::EmptyBlockchain => Self::EmptyBlockchain,
            ErrorCode::ExpectedDeploy => Self::ExpectedDeploy,
            ErrorCode::ExpectedTransaction => Self::ExpectedTransaction,
            ErrorCode::TransactionExpired => Self::TransactionExpired,
            ErrorCode::MissingOrIncorrectParameters => Self::MissingOrIncorrectParameters,
            ErrorCode::NoSuchAddressableEntity => Self::NoSuchAddressableEntity,
            ErrorCode::NoSuchContractAtHash => Self::NoSuchContractAtHash,
            ErrorCode::NoSuchEntryPoint => Self::NoSuchEntryPoint,
            ErrorCode::NoSuchPackageAtHash => Self::NoSuchPackageAtHash,
            ErrorCode::InvalidEntityAtVersion => Self::InvalidEntityAtVersion,
            ErrorCode::DisabledEntityAtVersion => Self::DisabledEntityAtVersion,
            ErrorCode::MissingEntityAtVersion => Self::MissingEntityAtVersion,
            ErrorCode::InvalidAssociatedKeys => Self::InvalidAssociatedKeys,
            ErrorCode::InsufficientSignatureWeight => Self::InsufficientSignatureWeight,
            ErrorCode::InsufficientBalance => Self::InsufficientBalance,
            ErrorCode::UnknownBalance => Self::UnknownBalance,
            ErrorCode::DeployInvalidPaymentVariant => Self::DeployInvalidPaymentVariant,
            ErrorCode::DeployMissingPaymentAmount => Self::DeployMissingPaymentAmount,
            ErrorCode::DeployFailedToParsePaymentAmount => Self::DeployFailedToParsePaymentAmount,
            ErrorCode::DeployMissingTransferTarget => Self::DeployMissingTransferTarget,
            ErrorCode::DeployMissingModuleBytes => Self::DeployMissingModuleBytes,
            ErrorCode::InvalidTransactionEntryPointCannotBeCall => {
                Self::InvalidTransactionEntryPointCannotBeCall
            }
            ErrorCode::InvalidTransactionInvalidTransactionLane => {
                Self::InvalidTransactionInvalidTransactionLane
            }
            ErrorCode::InvalidTransactionUnspecified => Self::TransactionUnspecified,
            ErrorCode::InvalidTransactionOrDeployUnspecified => {
                Self::TransactionOrDeployUnspecified
            }
            ErrorCode::ExpectedNamedArguments => Self::ExpectedNamedArguments,
            ErrorCode::InvalidTransactionRuntime => Self::InvalidTransactionRuntime,
            ErrorCode::InvalidTransactionNoWasmLaneMatches => {
                Self::InvalidTransactionNoWasmLaneMatches
            }
            ErrorCode::InvalidTransactionEntryPointMustBeCall => {
                Self::InvalidTransactionEntryPointMustBeCall
            }
            ErrorCode::InvalidTransactionCannotDeserializeField => {
                Self::InvalidTransactionCannotDeserializeField
            }
            ErrorCode::InvalidTransactionCannotCalculateFieldsHash => {
                Self::InvalidTransactionCannotCalculateFieldsHash
            }
            ErrorCode::InvalidTransactionUnexpectedFields => {
                Self::InvalidTransactionUnexpectedFields
            }
            ErrorCode::InvalidTransactionExpectedBytesArguments => {
                Self::InvalidTransactionExpectedBytesArguments
            }
            ErrorCode::InvalidTransactionMissingSeed => Self::InvalidTransactionMissingSeed,
            ErrorCode::PricingModeNotSupported => Self::PricingModeNotSupported,
            ErrorCode::InvalidDeployGasLimitNotSupported => Self::InvalidDeployGasLimitNotSupported,
            ErrorCode::InvalidDeployInvalidRuntime => Self::InvalidDeployInvalidRuntime,
            _ => Self::TransactionOrDeployUnspecified,
        }
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum Error {
    #[error("request error: {0}")]
    RequestFailed(String),
    #[error("request id mismatch: expected {expected}, got {got}")]
    RequestResponseIdMismatch { expected: u16, got: u16 },
    #[error("failed to get a response with correct id {max} times, giving up")]
    TooManyMismatchedResponses { max: u8 },
    #[error("failed to deserialize the original request provided with the response: {0}")]
    OriginalRequestDeserialization(String),
    #[error("failed to deserialize the envelope of a response")]
    EnvelopeDeserialization,
    #[error("failed to deserialize a response: {0}")]
    Deserialization(String),
    #[error("failed to serialize a request: {0}")]
    Serialization(String),
    #[error("unexpectedly received no response body")]
    NoResponseBody,
    #[error("unexpectedly received an empty envelope")]
    EmptyEnvelope,
    #[error("unexpected payload variant received in the response: {0}")]
    UnexpectedVariantReceived(u8),
    #[error("attempted to use a function that's disabled on the node")]
    FunctionIsDisabled,
    #[error("could not find the provided state root hash")]
    UnknownStateRootHash,
    #[error("the provided global state query failed to execute")]
    QueryFailedToExecute,
    #[error("could not execute the provided transaction: {0}")]
    InvalidTransaction(InvalidTransactionOrDeploy),
    #[error("speculative execution has failed: {0}")]
    SpecExecutionFailed(String),
    #[error("the switch block for the requested era was not found")]
    SwitchBlockNotFound,
    #[error("the parent of the switch block for the requested era was not found")]
    SwitchBlockParentNotFound,
    #[error("cannot serve rewards stored in V1 format")]
    UnsupportedRewardsV1Request,
    #[error("purse was not found for given identifier")]
    PurseNotFound,
    #[error("received an unexpected node error: {message} ({code})")]
    UnexpectedNodeError { message: String, code: u16 },
    #[error("binary protocol version mismatch")]
    CommandHeaderVersionMismatch,
    #[error("request was throttled by the node")]
    RequestThrottled,
    #[error("malformed information request")]
    MalformedInformationRequest,
    #[error("malformed version bytes in command header")]
    TooLittleBytesForRequestHeaderVersion,
    #[error("malformed protocol version")]
    MalformedCommandHeaderVersion,
    #[error("malformed transfer record key")]
    TransferRecordMalformedKey,
    #[error("malformed command header")]
    MalformedCommandHeader,
    #[error("malformed command")]
    MalformedCommand,
    #[error("not found")]
    NotFound,
    #[error("node reported internal error")]
    InternalNodeError,
    #[error("bad request")]
    BadRequest,
    #[error("unsupported request")]
    UnsupportedRequest,
    #[error("dictionary URef not found")]
    DictionaryURefNotFound,
    #[error("no complete blocks")]
    NoCompleteBlocks,
    #[error("gas price tolerance too low")]
    GasPriceToleranceTooLow,
    #[error("received v1 transaction for speculative execution")]
    ReceivedV1Transaction,
    #[error("connection to node lost")]
    ConnectionLost,
}

impl Error {
    fn from_error_code(code: u16) -> Self {
        match ErrorCode::try_from(code) {
            Ok(ErrorCode::FunctionDisabled) => Self::FunctionIsDisabled,
            Ok(ErrorCode::RootNotFound) => Self::UnknownStateRootHash,
            Ok(ErrorCode::FailedQuery) => Self::QueryFailedToExecute,
            Ok(ErrorCode::SwitchBlockNotFound) => Self::SwitchBlockNotFound,
            Ok(ErrorCode::SwitchBlockParentNotFound) => Self::SwitchBlockParentNotFound,
            Ok(ErrorCode::UnsupportedRewardsV1Request) => Self::UnsupportedRewardsV1Request,
            Ok(ErrorCode::CommandHeaderVersionMismatch) => Self::CommandHeaderVersionMismatch,
            Ok(ErrorCode::PurseNotFound) => Self::PurseNotFound,
            Ok(
                err @ (ErrorCode::InvalidDeployChainName
                | ErrorCode::InvalidDeployDependenciesNoLongerSupported
                | ErrorCode::InvalidDeployExcessiveSize
                | ErrorCode::InvalidDeployExcessiveTimeToLive
                | ErrorCode::InvalidDeployTimestampInFuture
                | ErrorCode::InvalidDeployBodyHash
                | ErrorCode::InvalidDeployHash
                | ErrorCode::InvalidDeployEmptyApprovals
                | ErrorCode::InvalidDeployApproval
                | ErrorCode::InvalidDeployExcessiveSessionArgsLength
                | ErrorCode::InvalidDeployExcessivePaymentArgsLength
                | ErrorCode::InvalidDeployMissingPaymentAmount
                | ErrorCode::InvalidDeployFailedToParsePaymentAmount
                | ErrorCode::InvalidDeployExceededBlockGasLimit
                | ErrorCode::InvalidDeployMissingTransferAmount
                | ErrorCode::InvalidDeployFailedToParseTransferAmount
                | ErrorCode::InvalidDeployInsufficientTransferAmount
                | ErrorCode::InvalidDeployExcessiveApprovals
                | ErrorCode::InvalidDeployUnableToCalculateGasLimit
                | ErrorCode::InvalidDeployUnableToCalculateGasCost
                | ErrorCode::InvalidDeployUnspecified
                | ErrorCode::InvalidTransactionChainName
                | ErrorCode::InvalidTransactionExcessiveSize
                | ErrorCode::InvalidTransactionExcessiveTimeToLive
                | ErrorCode::InvalidTransactionTimestampInFuture
                | ErrorCode::InvalidTransactionBodyHash
                | ErrorCode::InvalidTransactionHash
                | ErrorCode::InvalidTransactionEmptyApprovals
                | ErrorCode::InvalidTransactionInvalidApproval
                | ErrorCode::InvalidTransactionExcessiveArgsLength
                | ErrorCode::InvalidTransactionExcessiveApprovals
                | ErrorCode::InvalidTransactionExceedsBlockGasLimit
                | ErrorCode::InvalidTransactionMissingArg
                | ErrorCode::InvalidTransactionUnexpectedArgType
                | ErrorCode::InvalidTransactionInvalidArg
                | ErrorCode::InvalidTransactionInsufficientTransferAmount
                | ErrorCode::InvalidTransactionEntryPointCannotBeCustom
                | ErrorCode::InvalidTransactionEntryPointMustBeCustom
                | ErrorCode::InvalidTransactionEmptyModuleBytes
                | ErrorCode::InvalidTransactionGasPriceConversion
                | ErrorCode::InvalidTransactionUnableToCalculateGasLimit
                | ErrorCode::InvalidTransactionUnableToCalculateGasCost
                | ErrorCode::InvalidTransactionPricingMode
                | ErrorCode::EmptyBlockchain
                | ErrorCode::ExpectedDeploy
                | ErrorCode::ExpectedTransaction
                | ErrorCode::TransactionExpired
                | ErrorCode::MissingOrIncorrectParameters
                | ErrorCode::NoSuchAddressableEntity
                | ErrorCode::NoSuchContractAtHash
                | ErrorCode::NoSuchEntryPoint
                | ErrorCode::NoSuchPackageAtHash
                | ErrorCode::InvalidEntityAtVersion
                | ErrorCode::DisabledEntityAtVersion
                | ErrorCode::MissingEntityAtVersion
                | ErrorCode::InvalidAssociatedKeys
                | ErrorCode::InsufficientSignatureWeight
                | ErrorCode::InsufficientBalance
                | ErrorCode::UnknownBalance
                | ErrorCode::DeployInvalidPaymentVariant
                | ErrorCode::DeployMissingPaymentAmount
                | ErrorCode::DeployFailedToParsePaymentAmount
                | ErrorCode::DeployMissingTransferTarget
                | ErrorCode::DeployMissingModuleBytes
                | ErrorCode::InvalidTransactionEntryPointCannotBeCall
                | ErrorCode::InvalidTransactionInvalidTransactionLane
                | ErrorCode::InvalidTransactionUnspecified
                | ErrorCode::InvalidTransactionOrDeployUnspecified
                | ErrorCode::ExpectedNamedArguments
                | ErrorCode::InvalidTransactionRuntime
                | ErrorCode::InvalidTransactionNoWasmLaneMatches
                | ErrorCode::InvalidTransactionEntryPointMustBeCall
                | ErrorCode::InvalidTransactionCannotDeserializeField
                | ErrorCode::InvalidTransactionCannotCalculateFieldsHash
                | ErrorCode::InvalidTransactionUnexpectedFields
                | ErrorCode::InvalidTransactionExpectedBytesArguments
                | ErrorCode::InvalidTransactionMissingSeed
                | ErrorCode::PricingModeNotSupported
                | ErrorCode::InvalidDeployGasLimitNotSupported
                | ErrorCode::InvalidDeployInvalidRuntime),
            ) => Self::InvalidTransaction(InvalidTransactionOrDeploy::from(err)),
            Ok(ErrorCode::RequestThrottled) => Self::RequestThrottled,
            Ok(ErrorCode::MalformedInformationRequest) => Self::MalformedInformationRequest,
            Ok(ErrorCode::TooLittleBytesForRequestHeaderVersion) => {
                Self::TooLittleBytesForRequestHeaderVersion
            }
            Ok(ErrorCode::MalformedCommandHeaderVersion) => Self::MalformedCommandHeaderVersion,
            Ok(ErrorCode::TransferRecordMalformedKey) => Self::TransferRecordMalformedKey,
            Ok(ErrorCode::MalformedCommandHeader) => Self::MalformedCommandHeader,
            Ok(ErrorCode::MalformedCommand) => Self::MalformedCommand,
            Ok(err @ (ErrorCode::WasmPreprocessing | ErrorCode::InvalidItemVariant)) => {
                Self::SpecExecutionFailed(err.to_string())
            }
            Ok(ErrorCode::NotFound) => Self::NotFound,
            Ok(ErrorCode::InternalError) => Self::InternalNodeError,
            Ok(ErrorCode::BadRequest) => Self::BadRequest,
            Ok(ErrorCode::UnsupportedRequest) => Self::UnsupportedRequest,
            Ok(ErrorCode::DictionaryURefNotFound) => Self::DictionaryURefNotFound,
            Ok(ErrorCode::NoCompleteBlocks) => Self::NoCompleteBlocks,
            Ok(ErrorCode::GasPriceToleranceTooLow) => Self::GasPriceToleranceTooLow,
            Ok(ErrorCode::ReceivedV1Transaction) => Self::ReceivedV1Transaction,
            Ok(ErrorCode::NoError) => {
                //This code shouldn't be passed in an error scenario
                Self::UnexpectedNodeError {
                    message: "Received unexpected 'NoError' error code".to_string(),
                    code: ErrorCode::NoError as u16,
                }
            }
            Err(err) => Self::UnexpectedNodeError {
                message: err.to_string(),
                code,
            },
        }
    }
}

struct Reconnect;

struct Notify<T> {
    inner: tokio::sync::Notify,
    phantom: std::marker::PhantomData<T>,
}

impl<T> Notify<T> {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: tokio::sync::Notify::new(),
            phantom: std::marker::PhantomData,
        })
    }

    fn notified(&self) -> Notified {
        self.inner.notified()
    }

    fn notify_one(&self) {
        self.inner.notify_one()
    }
}

pub struct FramedNodeClient {
    client: Arc<RwLock<Framed<TcpStream, BinaryMessageCodec>>>,
    reconnect: Arc<Notify<Reconnect>>,
    config: NodeClientConfig,
    current_request_id: Arc<AtomicU16>,
}

impl FramedNodeClient {
    pub async fn new(
        config: NodeClientConfig,
        maybe_network_name: Option<String>,
    ) -> Result<
        (
            Arc<Self>,
            impl Future<Output = Result<(), AnyhowError>>,
            impl Future<Output = Result<(), AnyhowError>>,
        ),
        AnyhowError,
    > {
        let stream = Arc::new(RwLock::new(
            Self::connect_with_retries(
                &config.ip_address,
                config.port,
                &config.exponential_backoff,
                config.max_message_size_bytes,
            )
            .await?,
        ));

        let reconnect = Notify::<Reconnect>::new();

        let reconnect_loop =
            Self::reconnect_loop(config.clone(), Arc::clone(&stream), Arc::clone(&reconnect));
        let keepalive_timeout = Duration::from_millis(config.keepalive_timeout_ms);
        let node_client = Arc::new(Self {
            client: Arc::clone(&stream),
            reconnect,
            config,
            current_request_id: AtomicU16::new(INITIAL_REQUEST_ID).into(),
        });
        let keepalive_loop = Self::keepalive_loop(node_client.clone(), keepalive_timeout);

        // validate network name
        if let Some(network_name) = maybe_network_name {
            let node_status = node_client.read_node_status().await?;
            if network_name != node_status.chainspec_name {
                let msg = format!("Network name {} does't match name {network_name} configured for node RPC connection", node_status.chainspec_name);
                error!("{msg}");
                return Err(AnyhowError::msg(msg));
            }
        }

        Ok((node_client, reconnect_loop, keepalive_loop))
    }

    fn next_id(&self) -> u16 {
        self.current_request_id.fetch_add(1, Ordering::Relaxed)
    }

    async fn reconnect_loop(
        config: NodeClientConfig,
        client: Arc<RwLock<Framed<TcpStream, BinaryMessageCodec>>>,
        reconnect: Arc<Notify<Reconnect>>,
    ) -> Result<(), AnyhowError> {
        loop {
            tokio::select! {
                _ = reconnect.notified() => {
                    let mut lock = client.write().await;
                    let new_client = Self::reconnect(&config.clone()).await?;
                    *lock = new_client;
                },
            }
        }
    }

    async fn keepalive_loop(
        client: Arc<dyn NodeClient>,
        keepalive_timeout: Duration,
    ) -> Result<(), AnyhowError> {
        loop {
            tokio::time::sleep(keepalive_timeout).await;
            client
                .send_request(Command::Get(GetRequest::Information {
                    info_type_tag: InformationRequestTag::ProtocolVersion.into(),
                    key: vec![],
                }))
                .await?;
        }
    }

    async fn send_request_internal(
        &self,
        req: &Command,
        client: &mut RwLockWriteGuard<'_, Framed<TcpStream, BinaryMessageCodec>>,
    ) -> Result<BinaryResponseAndRequest, Error> {
        let (request_id, payload) = self.generate_payload(req);

        if let Err(err) = tokio::time::timeout(
            Duration::from_secs(self.config.message_timeout_secs),
            client.send(payload),
        )
        .await
        .map_err(|_| {
            register_timeout("sending_payload");
            Error::RequestFailed("timeout".to_owned())
        })? {
            return Err(Error::RequestFailed(err.to_string()));
        };

        for _ in 0..MAX_MISMATCHED_ID_RETRIES {
            let Ok(maybe_response) = tokio::time::timeout(
                Duration::from_secs(self.config.message_timeout_secs),
                client.next(),
            )
            .await
            else {
                register_timeout("receiving_response");
                return Err(Error::ConnectionLost);
            };

            if let Some(response) = maybe_response {
                let resp = bytesrepr::deserialize_from_slice(
                    response
                        .map_err(|err| Error::RequestFailed(err.to_string()))?
                        .payload(),
                )
                .map_err(|err| {
                    error!(
                        "Error when deserializing binary port envelope: {}",
                        err.to_string()
                    );
                    Error::EnvelopeDeserialization
                })?;
                match validate_response(resp, request_id) {
                    Ok(response) => return Ok(response),
                    Err(err) if matches!(err, Error::RequestResponseIdMismatch { expected, got } if expected > got) =>
                    {
                        // If our expected ID is greater than the one we received, it means we can
                        // try to recover from the situation by reading more responses from the stream.
                        warn!(%err, "received a response with an outdated id, trying another response");
                        register_mismatched_id();
                        continue;
                    }
                    Err(err) => {
                        register_mismatched_id();
                        return Err(err);
                    }
                }
            } else {
                return Err(Error::ConnectionLost);
            }
        }

        Err(Error::TooManyMismatchedResponses {
            max: MAX_MISMATCHED_ID_RETRIES,
        })
    }

    fn generate_payload(&self, req: &Command) -> (u16, BinaryMessage) {
        let next_id = self.next_id();
        (
            next_id,
            BinaryMessage::new(
                encode_request(req, next_id).expect("should always serialize a request"),
            ),
        )
    }

    async fn connect_with_retries(
        ip_address: &IpAddr,
        port: u16,
        backoff_config: &ExponentialBackoffConfig,
        max_message_size_bytes: u32,
    ) -> Result<Framed<TcpStream, BinaryMessageCodec>, AnyhowError> {
        let mut wait = backoff_config.initial_delay_ms;
        let max_attempts = &backoff_config.max_attempts;
        let tcp_socket = SocketAddr::new(*ip_address, port);
        let mut current_attempt = 1;
        loop {
            match TcpStream::connect(tcp_socket).await {
                Ok(stream) => {
                    return Ok(Framed::new(
                        stream,
                        BinaryMessageCodec::new(max_message_size_bytes),
                    ))
                }
                Err(err) => {
                    current_attempt += 1;
                    if *max_attempts < current_attempt {
                        anyhow::bail!(
                            "Couldn't connect to node {} after {} attempts",
                            tcp_socket,
                            current_attempt - 1
                        );
                    }
                    warn!(%err, "failed to connect to node {tcp_socket}, waiting {wait}ms before retrying");
                    tokio::time::sleep(Duration::from_millis(wait)).await;
                    wait = (wait * backoff_config.coefficient).min(backoff_config.max_delay_ms);
                }
            };
        }
    }

    async fn reconnect_internal(
        config: &NodeClientConfig,
    ) -> Result<Framed<TcpStream, BinaryMessageCodec>, AnyhowError> {
        let disconnected_start = Instant::now();
        inc_disconnect();
        error!("node connection closed, will attempt to reconnect");
        let stream = Self::connect_with_retries(
            &config.ip_address,
            config.port,
            &config.exponential_backoff,
            config.max_message_size_bytes,
        )
        .await?;
        info!("connection with the node has been re-established");
        observe_reconnect_time(disconnected_start.elapsed());
        Ok(stream)
    }

    async fn reconnect(
        config: &NodeClientConfig,
    ) -> Result<Framed<TcpStream, BinaryMessageCodec>, AnyhowError> {
        Self::reconnect_internal(config).await
    }
}

#[async_trait]
impl NodeClient for FramedNodeClient {
    async fn send_request(&self, req: Command) -> Result<BinaryResponseAndRequest, Error> {
        let mut client = match tokio::time::timeout(
            Duration::from_secs(self.config.client_access_timeout_secs),
            self.client.write(),
        )
        .await
        {
            Ok(client) => client,
            Err(_) => {
                register_timeout("acquiring_client");
                return Err(Error::ConnectionLost);
            }
        };

        let result = self.send_request_internal(&req, &mut client).await;
        if let Err(err) = &result {
            warn!(
                addr = %self.config.ip_address,
                err = display_error(&err),
                "binary port client handler error"
            );
            // attempt to reconnect in case the node was restarted and connection broke
            client.close().await.ok();
            let ip_address = &self.config.ip_address;

            match tokio::time::timeout(
                Duration::from_secs(self.config.client_access_timeout_secs),
                Self::connect_with_retries(
                    ip_address,
                    self.config.port,
                    &self.config.exponential_backoff,
                    self.config.max_message_size_bytes,
                ),
            )
            .await
            {
                Ok(Ok(new_client)) => {
                    *client = new_client;
                    return self.send_request_internal(&req, &mut client).await;
                }
                Ok(Err(err)) => {
                    warn!(
                        %err,
                        addr = %self.config.ip_address,
                        "binary port client failed to reconnect"
                    );
                    // schedule standard reconnect process with multiple retries
                    // and return a response
                    self.reconnect.notify_one();
                    return Err(Error::ConnectionLost);
                }
                Err(_) => {
                    warn!(
                        %err,
                        addr = %self.config.ip_address,
                        "failed to reestablish connection in timely fashion"
                    );
                    register_timeout("reacquiring_connection");
                    return Err(Error::ConnectionLost);
                }
            }
        }
        result
    }
}

#[derive(Debug)]
pub enum EntityResponse {
    Entity(AddressableEntityInformation),
    Account(AccountInformation),
    Contract(ContractInformation),
}

#[derive(Debug)]
pub enum PackageResponse {
    Package(ValueWithProof<Package>),
    ContractPackage(ValueWithProof<ContractPackage>),
}

fn validate_response(
    resp: BinaryResponseAndRequest,
    expected_id: u16,
) -> Result<BinaryResponseAndRequest, Error> {
    let original_id = match try_parse_request_id(resp.request()) {
        Ok(id) => id,
        Err(e) => {
            error!("Error when decoding original_id: {}", e);
            0
        }
    };
    if original_id != expected_id {
        return Err(Error::RequestResponseIdMismatch {
            expected: expected_id,
            got: original_id,
        });
    }
    Ok(resp)
}

fn try_parse_request_id(request: &[u8]) -> Result<u16, anyhow::Error> {
    if request.len() < 4 {
        anyhow::bail!(
            "Node responded with request that doesn't have enough bytes to read payload length"
        )
    }
    let (payload_length, remainder) =
        u32::from_bytes(request).map_err(|e| anyhow::Error::msg(e.to_string()))?;
    if payload_length != remainder.len() as u32 {
        anyhow::bail!("Node responded with request that has a mismatch in declared bytes vs the payload length")
    }
    let (header, _) = extract_header(remainder)?;
    Ok(header.id())
}

fn extract_header(payload: &[u8]) -> Result<(CommandHeader, &[u8]), anyhow::Error> {
    const BINARY_VERSION_LENGTH_BYTES: usize = std::mem::size_of::<u16>();

    if payload.len() < BINARY_VERSION_LENGTH_BYTES {
        anyhow::bail!("Not enough bytes to read version of the command header");
    }

    let binary_protocol_version = match u16::from_bytes(payload) {
        Ok((binary_protocol_version, _)) => binary_protocol_version,
        Err(_) => {
            anyhow::bail!("Could not read header version from request");
        }
    };

    if binary_protocol_version != CommandHeader::HEADER_VERSION {
        anyhow::bail!("Header version does not meet expectation");
    }

    match CommandHeader::from_bytes(payload) {
        Ok((header, remainder)) => Ok((header, remainder)),
        Err(error) => {
            anyhow::bail!("Malformed CommandHeader definition: {}", error);
        }
    }
}

fn parse_response<A>(resp: &BinaryResponse) -> Result<Option<A>, Error>
where
    A: FromBytes + PayloadEntity,
{
    if resp.is_not_found() {
        return Ok(None);
    }
    if !resp.is_success() {
        return Err(Error::from_error_code(resp.error_code()));
    }
    match resp.returned_data_type_tag() {
        Some(found) if found == u8::from(A::RESPONSE_TYPE) => {
            bytesrepr::deserialize_from_slice(resp.payload())
                .map(Some)
                .map_err(|err| Error::Deserialization(err.to_string()))
        }
        Some(other) => Err(Error::UnexpectedVariantReceived(other)),
        _ => Ok(None),
    }
}

fn parse_response_bincode<A>(resp: &BinaryResponse) -> Result<Option<A>, Error>
where
    A: DeserializeOwned + PayloadEntity,
{
    if resp.is_not_found() {
        return Ok(None);
    }
    if !resp.is_success() {
        return Err(Error::from_error_code(resp.error_code()));
    }
    match resp.returned_data_type_tag() {
        Some(found) if found == u8::from(A::RESPONSE_TYPE) => bincode::deserialize(resp.payload())
            .map(Some)
            .map_err(|err| Error::Deserialization(err.to_string())),
        Some(other) => Err(Error::UnexpectedVariantReceived(other)),
        _ => Ok(None),
    }
}

/// Wraps an error to ensure it gets properly captured by tracing.
pub(crate) fn display_error<'a, T>(err: &'a T) -> field::DisplayValue<ErrFormatter<'a, T>>
where
    T: std::error::Error + 'a,
{
    field::display(ErrFormatter(err))
}

/// An error formatter.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ErrFormatter<'a, T>(pub &'a T);

impl<T> Display for ErrFormatter<'_, T>
where
    T: std::error::Error,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut opt_source: Option<&(dyn std::error::Error)> = Some(self.0);

        while let Some(source) = opt_source {
            write!(f, "{}", source)?;
            opt_source = source.source();

            if opt_source.is_some() {
                f.write_str(": ")?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::testing::{
        get_dummy_request, get_dummy_request_payload, get_port, start_mock_binary_port,
        start_mock_binary_port_responding_with_given_response,
        start_mock_binary_port_responding_with_stored_value,
    };

    use super::*;
    use casper_binary_port::{CommandHeader, ReactorStateName};
    use casper_types::{
        testing::TestRng, BlockSynchronizerStatus, CLValue, ProtocolVersion, TimeDiff, Timestamp,
    };
    use tokio::time::sleep;

    #[tokio::test]
    async fn given_client_and_no_node_should_fail_after_tries() {
        let config = NodeClientConfig::new_with_port_and_retries(1111, 2);
        let res = FramedNodeClient::new(config, None).await;

        assert!(res.is_err());
        let error_message = res.err().unwrap().to_string();

        assert!(error_message.starts_with("Couldn't connect to node"));
        assert!(error_message.ends_with(" after 2 attempts"));
    }

    #[tokio::test]
    async fn given_client_and_node_should_connect_and_do_request() {
        let port = get_port();
        let mut rng = TestRng::new();
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let _mock_server_handle = start_mock_binary_port_responding_with_stored_value(
            port,
            Some(INITIAL_REQUEST_ID),
            None,
            Arc::clone(&shutdown),
        )
        .await;
        let config = NodeClientConfig::new_with_port_and_retries(port, 2);
        let (c, _, _) = FramedNodeClient::new(config, None).await.unwrap();

        let res = query_global_state_for_string_value(&mut rng, &c)
            .await
            .unwrap();

        assert_eq!(res, StoredValue::CLValue(CLValue::from_t("Foo").unwrap()))
    }

    #[tokio::test]
    async fn given_client_should_try_until_node_starts() {
        let mut rng = TestRng::new();
        let port = get_port();
        let shutdown = Arc::new(tokio::sync::Notify::new());
        tokio::spawn(async move {
            sleep(Duration::from_secs(5)).await;
            let _mock_server_handle = start_mock_binary_port_responding_with_stored_value(
                port,
                Some(INITIAL_REQUEST_ID),
                None,
                Arc::clone(&shutdown),
            )
            .await;
        });
        let config = NodeClientConfig::new_with_port_and_retries(port, 5);
        let (client, _, _) = FramedNodeClient::new(config, None).await.unwrap();

        let res = query_global_state_for_string_value(&mut rng, &client)
            .await
            .unwrap();

        assert_eq!(res, StoredValue::CLValue(CLValue::from_t("Foo").unwrap()))
    }

    #[tokio::test]
    async fn should_fail_to_connect_if_network_name_does_not_match() {
        // prepare node status response with network name
        let mut rng = TestRng::new();
        let protocol_version = ProtocolVersion::from_parts(2, 0, 0);
        let value = NodeStatus {
            protocol_version,
            peers: Peers::random(&mut rng),
            build_version: rng.random_string(5..10),
            chainspec_name: "network-1".into(),
            starting_state_root_hash: Digest::random(&mut rng),
            last_added_block_info: None,
            our_public_signing_key: None,
            round_length: None,
            next_upgrade: None,
            uptime: TimeDiff::from_millis(0),
            reactor_state: ReactorStateName::new(rng.random_string(5..10)),
            last_progress: Timestamp::random(&mut rng),
            available_block_range: AvailableBlockRange::random(&mut rng),
            block_sync: BlockSynchronizerStatus::random(&mut rng),
            latest_switch_block_hash: None,
        };
        let response = BinaryResponse::from_value(value);

        // setup mock binary port with node status response
        let port = get_port();
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let request_id = Some(INITIAL_REQUEST_ID);
        let _mock_server_handle = start_mock_binary_port_responding_with_given_response(
            port,
            request_id,
            None,
            Arc::clone(&shutdown),
            response,
        )
        .await;

        let config = NodeClientConfig::new_with_port_and_retries(port, 2);
        let network_name = Some("not-network-1".into());
        let res = FramedNodeClient::new(config, network_name).await;

        assert!(res.is_err());
        let error_message = res.err().unwrap().to_string();

        assert_eq!(error_message, "Network name network-1 does't match name not-network-1 configured for node RPC connection");
    }

    async fn query_global_state_for_string_value(
        rng: &mut TestRng,
        client: &FramedNodeClient,
    ) -> Result<StoredValue, Error> {
        let state_root_hash = Digest::random(rng);
        let base_key = Key::ChecksumRegistry;
        client
            .query_global_state(
                Some(GlobalStateIdentifier::StateRootHash(state_root_hash)),
                base_key,
                vec![],
            )
            .await?
            .ok_or(Error::NoResponseBody)
            .map(|query_res| query_res.into_inner().0)
    }

    #[tokio::test]
    async fn given_client_should_reconnect_to_restarted_node_and_do_request() {
        let port = get_port();
        let mut rng = TestRng::new();
        let shutdown_server = Arc::new(tokio::sync::Notify::new());
        let mock_server_handle = start_mock_binary_port_responding_with_stored_value(
            port,
            Some(INITIAL_REQUEST_ID),
            None,
            Arc::clone(&shutdown_server),
        )
        .await;

        let config = NodeClientConfig::new_with_port(port);
        let (c, reconnect_loop, _) = FramedNodeClient::new(config, None).await.unwrap();

        let scenario = async {
            // Request id = 1
            assert!(query_global_state_for_string_value(&mut rng, &c)
                .await
                .is_ok());

            // shutdown node
            shutdown_server.notify_one();
            let _ = mock_server_handle.await;

            // Request id = 2
            let err = query_global_state_for_string_value(&mut rng, &c)
                .await
                .unwrap_err();
            assert!(matches!(err, Error::ConnectionLost));

            // restart node
            let mock_server_handle = start_mock_binary_port_responding_with_stored_value(
                port,
                Some(INITIAL_REQUEST_ID + 2),
                None,
                Arc::clone(&shutdown_server),
            )
            .await;

            // wait for reconnect loop to do it's business
            tokio::time::sleep(Duration::from_secs(2)).await;

            // Request id = 3
            assert!(query_global_state_for_string_value(&mut rng, &c)
                .await
                .is_ok());

            // restart node between requests
            shutdown_server.notify_one();
            let _ = mock_server_handle.await;
            let _mock_server_handle = start_mock_binary_port_responding_with_stored_value(
                port,
                Some(INITIAL_REQUEST_ID + 4),
                None,
                Arc::clone(&shutdown_server),
            )
            .await;

            // Request id = 4 & 5 (retry)
            assert!(query_global_state_for_string_value(&mut rng, &c)
                .await
                .is_ok());
        };

        tokio::select! {
            _ = scenario => (),
            _ = reconnect_loop => panic!("reconnect loop should not exit"),
        }
    }

    #[tokio::test]
    async fn should_generate_payload_with_incrementing_id() {
        let port = get_port();
        let config = NodeClientConfig::new_with_port(port);
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let _mock_server_handle =
            start_mock_binary_port(port, vec![], 1, Arc::clone(&shutdown)).await;
        let (c, _, _) = FramedNodeClient::new(config, None).await.unwrap();

        let generated_ids: Vec<_> = (INITIAL_REQUEST_ID..INITIAL_REQUEST_ID + 10)
            .map(|_| {
                let (_, binary_message) = c.generate_payload(&get_dummy_request());
                let header = CommandHeader::from_bytes(binary_message.payload())
                    .unwrap()
                    .0;
                header.id()
            })
            .collect();

        assert_eq!(
            generated_ids,
            (INITIAL_REQUEST_ID..INITIAL_REQUEST_ID + 10).collect::<Vec<_>>()
        );
    }

    #[test]
    fn should_reject_mismatched_request_id() {
        let expected_id = 1;
        let actual_id = 2;

        let req = get_dummy_request_payload(Some(actual_id));
        let resp = BinaryResponse::new_empty();
        let resp_and_req = BinaryResponseAndRequest::new(resp, req);

        let result = validate_response(resp_and_req, expected_id);
        assert!(matches!(
            result,
            Err(Error::RequestResponseIdMismatch { expected, got }) if expected == 1 && got == 2
        ));

        let expected_id = 2;
        let actual_id = 1;

        let req = get_dummy_request_payload(Some(actual_id));
        let resp = BinaryResponse::new_empty();
        let resp_and_req = BinaryResponseAndRequest::new(resp, req);

        let result = validate_response(resp_and_req, expected_id);
        assert!(matches!(
            result,
            Err(Error::RequestResponseIdMismatch { expected, got }) if expected == 2 && got == 1
        ));
    }

    #[test]
    fn should_accept_matching_request_id() {
        let expected_id = 1;
        let actual_id = 1;

        let req = get_dummy_request_payload(Some(actual_id));
        let resp = BinaryResponse::new_empty();
        let resp_and_req = BinaryResponseAndRequest::new(resp, req);

        let result = validate_response(resp_and_req, expected_id);
        dbg!(&result);
        assert!(result.is_ok())
    }

    #[tokio::test]
    async fn should_keep_retrying_to_get_response_up_to_the_limit() {
        const LIMIT: u8 = MAX_MISMATCHED_ID_RETRIES - 1;

        let port = get_port();
        let mut rng = TestRng::new();
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let _mock_server_handle = start_mock_binary_port_responding_with_stored_value(
            port,
            Some(0),
            Some(LIMIT),
            Arc::clone(&shutdown),
        )
        .await;
        let config = NodeClientConfig::new_with_port_and_retries(port, 2);
        let (c, _, _) = FramedNodeClient::new(config, None).await.unwrap();

        let res = query_global_state_for_string_value(&mut rng, &c)
            .await
            .unwrap_err();
        // Expect error different than 'TooManyMismatchResponses'
        assert!(!matches!(res, Error::TooManyMismatchedResponses { .. }));
    }

    #[tokio::test]
    async fn should_quit_retrying_to_get_response_over_the_retry_limit() {
        const LIMIT: u8 = MAX_MISMATCHED_ID_RETRIES;

        let port = get_port();
        let mut rng = TestRng::new();
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let _mock_server_handle = start_mock_binary_port_responding_with_stored_value(
            port,
            Some(0),
            Some(LIMIT),
            Arc::clone(&shutdown),
        )
        .await;
        let config = NodeClientConfig::new_with_port_and_retries(port, 2);
        let (c, _, _) = FramedNodeClient::new(config, None).await.unwrap();

        let res = query_global_state_for_string_value(&mut rng, &c)
            .await
            .unwrap_err();
        // Expect 'TooManyMismatchResponses' error
        assert!(matches!(res, Error::TooManyMismatchedResponses { max } if max == LIMIT));
    }

    #[test]
    fn should_map_errors() {
        assert!(matches!(
            Error::from_error_code(ErrorCode::InvalidTransactionOrDeployUnspecified as u16),
            Error::InvalidTransaction(InvalidTransactionOrDeploy::TransactionOrDeployUnspecified)
        ));
        assert!(matches!(
            Error::from_error_code(ErrorCode::ExpectedNamedArguments as u16),
            Error::InvalidTransaction(InvalidTransactionOrDeploy::ExpectedNamedArguments)
        ));
        assert!(matches!(
            Error::from_error_code(ErrorCode::InvalidTransactionRuntime as u16),
            Error::InvalidTransaction(InvalidTransactionOrDeploy::InvalidTransactionRuntime)
        ));
        assert!(matches!(
            Error::from_error_code(ErrorCode::InvalidTransactionNoWasmLaneMatches as u16),
            Error::InvalidTransaction(
                InvalidTransactionOrDeploy::InvalidTransactionNoWasmLaneMatches
            )
        ));
        assert!(matches!(
            Error::from_error_code(ErrorCode::InvalidTransactionEntryPointMustBeCall as u16),
            Error::InvalidTransaction(
                InvalidTransactionOrDeploy::InvalidTransactionEntryPointMustBeCall
            )
        ));
        assert!(matches!(
            Error::from_error_code(ErrorCode::InvalidTransactionCannotDeserializeField as u16),
            Error::InvalidTransaction(
                InvalidTransactionOrDeploy::InvalidTransactionCannotDeserializeField
            )
        ));
        assert!(matches!(
            Error::from_error_code(ErrorCode::InvalidTransactionCannotCalculateFieldsHash as u16),
            Error::InvalidTransaction(
                InvalidTransactionOrDeploy::InvalidTransactionCannotCalculateFieldsHash
            )
        ));
        assert!(matches!(
            Error::from_error_code(ErrorCode::InvalidTransactionUnexpectedFields as u16),
            Error::InvalidTransaction(
                InvalidTransactionOrDeploy::InvalidTransactionUnexpectedFields
            )
        ));
        assert!(matches!(
            Error::from_error_code(ErrorCode::InvalidTransactionExpectedBytesArguments as u16),
            Error::InvalidTransaction(
                InvalidTransactionOrDeploy::InvalidTransactionExpectedBytesArguments
            )
        ));
        assert!(matches!(
            Error::from_error_code(ErrorCode::InvalidTransactionMissingSeed as u16),
            Error::InvalidTransaction(InvalidTransactionOrDeploy::InvalidTransactionMissingSeed)
        ));
        assert!(matches!(
            Error::from_error_code(ErrorCode::PricingModeNotSupported as u16),
            Error::InvalidTransaction(InvalidTransactionOrDeploy::PricingModeNotSupported)
        ));
        assert!(matches!(
            Error::from_error_code(ErrorCode::InvalidDeployGasLimitNotSupported as u16),
            Error::InvalidTransaction(
                InvalidTransactionOrDeploy::InvalidDeployGasLimitNotSupported
            )
        ));
        assert!(matches!(
            Error::from_error_code(ErrorCode::InvalidDeployInvalidRuntime as u16),
            Error::InvalidTransaction(InvalidTransactionOrDeploy::InvalidDeployInvalidRuntime)
        ));
        assert!(matches!(
            Error::from_error_code(ErrorCode::MalformedInformationRequest as u16),
            Error::MalformedInformationRequest
        ));
        assert!(matches!(
            Error::from_error_code(ErrorCode::TooLittleBytesForRequestHeaderVersion as u16),
            Error::TooLittleBytesForRequestHeaderVersion
        ));
        assert!(matches!(
            Error::from_error_code(ErrorCode::MalformedCommandHeaderVersion as u16),
            Error::MalformedCommandHeaderVersion
        ));
        assert!(matches!(
            Error::from_error_code(ErrorCode::TransferRecordMalformedKey as u16),
            Error::TransferRecordMalformedKey
        ));
        assert!(matches!(
            Error::from_error_code(ErrorCode::MalformedCommandHeader as u16),
            Error::MalformedCommandHeader
        ));
        assert!(matches!(
            Error::from_error_code(ErrorCode::MalformedCommand as u16),
            Error::MalformedCommand
        ));
        assert!(matches!(
            Error::from_error_code(ErrorCode::NotFound as u16),
            Error::NotFound
        ));
        assert!(matches!(
            Error::from_error_code(ErrorCode::InternalError as u16),
            Error::InternalNodeError
        ));
        assert!(matches!(
            Error::from_error_code(ErrorCode::BadRequest as u16),
            Error::BadRequest
        ));
        assert!(matches!(
            Error::from_error_code(ErrorCode::UnsupportedRequest as u16),
            Error::UnsupportedRequest
        ));
        assert!(matches!(
            Error::from_error_code(ErrorCode::DictionaryURefNotFound as u16),
            Error::DictionaryURefNotFound
        ));
        assert!(matches!(
            Error::from_error_code(ErrorCode::NoCompleteBlocks as u16),
            Error::NoCompleteBlocks
        ));
        assert!(matches!(
            Error::from_error_code(ErrorCode::GasPriceToleranceTooLow as u16),
            Error::GasPriceToleranceTooLow
        ));
        assert!(matches!(
            Error::from_error_code(ErrorCode::ReceivedV1Transaction as u16),
            Error::ReceivedV1Transaction
        ));
    }
}
