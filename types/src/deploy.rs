use std::collections::BTreeSet;
use std::fmt::{self, Display, Formatter};

use itertools::Itertools;
use serde::{Deserialize, Serialize};
use ed25519_dalek::ExpandedSecretKey;
use k256::ecdsa::{signature::Signer, Signature as Secp256k1Signature};
use thiserror::Error;

use casper_hashing::Digest;

use casper_types::{bytesrepr::{self}, PublicKey, SecretKey, Signature, TimeDiff, Timestamp, U512, RuntimeArgs, runtime_args};

use crate::executable_deploy_item::ExecutableDeployItem;

use casper_types::bytesrepr::ToBytes;
use casper_types::testing::TestRng;
use rand::{Rng, RngCore};

/// The maximum permissible size in bytes of a Deploy when serialized via `ToBytes`.
///
/// Note: this should be kept in sync with the value of `[deploys.max_deploy_size]` in the
/// production chainspec.
pub const MAX_SERIALIZED_SIZE_OF_DEPLOY: u32 = 1_024 * 1_024;

/// Signs the given message using the given key pair.
pub(crate) fn sign<T: AsRef<[u8]>>(
    message: T,
    secret_key: &SecretKey,
    public_key: &PublicKey,
) -> Signature {
    match (secret_key, public_key) {
        (SecretKey::System, PublicKey::System) => {
            panic!("cannot create signature with system keys",)
        }
        (SecretKey::Ed25519(secret_key), PublicKey::Ed25519(public_key)) => {
            let expanded_secret_key = ExpandedSecretKey::from(secret_key);
            let signature = expanded_secret_key.sign(message.as_ref(), public_key);
            Signature::Ed25519(signature)
        }
        (SecretKey::Secp256k1(secret_key), PublicKey::Secp256k1(_public_key)) => {
            let signer = secret_key;
            let signature: Secp256k1Signature = signer
                .try_sign(message.as_ref())
                .expect("should create signature");
            Signature::Secp256k1(signature)
        }
        _ => panic!("secret and public key types must match"),
    }
}

/// A representation of the way in which a deploy failed validation checks.
#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Debug, Error, Serialize)]
pub enum DeployConfigurationFailure {
    /// Invalid chain name.
    #[error("invalid chain name: expected {expected}, got {got}")]
    InvalidChainName {
        /// The expected chain name.
        expected: String,
        /// The received chain name.
        got: String,
    },

    /// Too many dependencies.
    #[error("{got} dependencies exceeds limit of {max_dependencies}")]
    ExcessiveDependencies {
        /// The dependencies limit.
        max_dependencies: u8,
        /// The actual number of dependencies provided.
        got: usize,
    },

    /// Deploy is too large.
    #[error("deploy size too large: {0}")]
    ExcessiveSize(#[from] ExcessiveSizeError),

    /// Excessive time-to-live.
    #[error("time-to-live of {got} exceeds limit of {max_ttl}")]
    ExcessiveTimeToLive {
        /// The time-to-live limit.
        max_ttl: TimeDiff,
        /// The received time-to-live.
        got: TimeDiff,
    },

    /// The provided body hash does not match the actual hash of the body.
    #[error("the provided body hash does not match the actual hash of the body")]
    InvalidBodyHash,

    /// The provided deploy hash does not match the actual hash of the deploy.
    #[error("the provided hash does not match the actual hash of the deploy")]
    InvalidDeployHash,

    /// The deploy has no approvals.
    #[error("the deploy has no approvals")]
    EmptyApprovals,

    /// Invalid approval.
    #[error("the approval at index {index} is invalid: {error_msg}")]
    InvalidApproval {
        /// The index of the approval at fault.
        index: usize,
        /// The approval validation error.
        error_msg: String,
    },

    /// Excessive length of deploy's session args.
    #[error("serialized session code runtime args of {got} exceeds limit of {max_length}")]
    ExcessiveSessionArgsLength {
        /// The byte size limit of session arguments.
        max_length: usize,
        /// The received length of session arguments.
        got: usize,
    },

    /// Excessive length of deploy's payment args.
    #[error("serialized payment code runtime args of {got} exceeds limit of {max_length}")]
    ExcessivePaymentArgsLength {
        /// The byte size limit of payment arguments.
        max_length: usize,
        /// The received length of payment arguments.
        got: usize,
    },

    /// Missing payment "amount" runtime argument.
    #[error("missing payment 'amount' runtime argument ")]
    MissingPaymentAmount,

    /// Failed to parse payment "amount" runtime argument.
    #[error("failed to parse payment 'amount' as U512")]
    FailedToParsePaymentAmount,

    /// The payment amount associated with the deploy exceeds the block gas limit.
    #[error("payment amount of {got} exceeds the block gas limit of {block_gas_limit}")]
    ExceededBlockGasLimit {
        /// Configured block gas limit.
        block_gas_limit: u64,
        /// The payment amount received.
        got: U512,
    },

    /// Missing payment "amount" runtime argument
    #[error("missing transfer 'amount' runtime argument")]
    MissingTransferAmount,

    /// Failed to parse transfer "amount" runtime argument.
    #[error("failed to parse transfer 'amount' as U512")]
    FailedToParseTransferAmount,

    /// Insufficient transfer amount.
    #[error("insufficient transfer amount; minimum: {minimum} attempted: {attempted}")]
    InsufficientTransferAmount {
        /// The minimum transfer amount.
        minimum: U512,
        /// The attempted transfer amount.
        attempted: U512,
    },

    /// The amount of approvals on the deploy exceeds the max_associated_keys limit.
    #[error("number of associated keys {got} exceeds the maximum {max_associated_keys}")]
    ExcessiveApprovals {
        /// Number of approvals on the deploy.
        got: u32,
        /// The chainspec limit for max_associated_keys.
        max_associated_keys: u32,
    },
}

/// Error returned when a Deploy is too large.
#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Debug, Error, Serialize)]
#[error("deploy size of {actual_deploy_size} bytes exceeds limit of {max_deploy_size}")]
pub struct ExcessiveSizeError {
    /// The maximum permitted serialized deploy size, in bytes.
    pub max_deploy_size: u32,
    /// The serialized size of the deploy provided, in bytes.
    pub actual_deploy_size: usize,
}


/// A cryptographic hash uniquely identifying a [`Deploy`].
#[derive(
    Copy, Clone, Default, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize, Debug,
)]
#[serde(deny_unknown_fields)]
pub struct DeployHash(Digest);

impl DeployHash {
    /// Returns a new `DeployHash`.
    pub fn new(digest: Digest) -> Self {
        DeployHash(digest)
    }

    /// Returns a copy of the wrapped `Digest`.
    pub fn inner(&self) -> Digest {
        self.0
    }
}

impl From<Digest> for DeployHash {
    fn from(digest: Digest) -> Self {
        Self(digest)
    }
}


impl From<DeployHash> for Digest {
    fn from(deploy_hash: DeployHash) -> Self {
        deploy_hash.0
    }
}

impl Display for DeployHash {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

impl ToBytes for DeployHash {
    fn to_bytes(&self) -> Result<Vec<u8>, bytesrepr::Error> {
        self.0.to_bytes()
    }

    fn serialized_length(&self) -> usize {
        self.0.serialized_length()
    }
}

impl AsRef<[u8]> for DeployHash {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

/// The header portion of a [`Deploy`].
#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct DeployHeader {
    account: PublicKey,
    timestamp: Timestamp,
    ttl: TimeDiff,
    gas_price: u64,
    body_hash: Digest,
    dependencies: Vec<DeployHash>,
    chain_name: String,
}

impl DeployHeader {
    /// Returns the account within which the deploy will be run.
    pub fn account(&self) -> &PublicKey {
        &self.account
    }

    /// Returns the deploy creation timestamp.
    pub fn timestamp(&self) -> Timestamp {
        self.timestamp
    }

    /// Returns the duration for which the deploy will stay valid.
    pub fn ttl(&self) -> TimeDiff {
        self.ttl
    }

    /// Returns the price per gas unit for this deploy.
    pub fn gas_price(&self) -> u64 {
        self.gas_price
    }

    /// Returns the hash of the body of this deploy.
    pub fn body_hash(&self) -> Digest {
        self.body_hash
    }

    /// Other deploys that have to be run before this one.
    pub fn dependencies(&self) -> &Vec<DeployHash> {
        &self.dependencies
    }

    /// Returns the chain name of the network the deploy is supposed to be run on.
    pub fn chain_name(&self) -> &str {
        &self.chain_name
    }
}

impl Display for DeployHeader {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        write!(
            formatter,
            "deploy header {{ account {}, timestamp {}, ttl {}, body hash {}, chain name {} }}",
            self.account, self.timestamp, self.ttl, self.body_hash, self.chain_name,
        )
    }
}

impl ToBytes for DeployHeader {
    fn to_bytes(&self) -> Result<Vec<u8>, bytesrepr::Error> {
        let mut buffer = bytesrepr::allocate_buffer(self)?;
        buffer.extend(self.account.to_bytes()?);
        buffer.extend(self.timestamp.to_bytes()?);
        buffer.extend(self.ttl.to_bytes()?);
        buffer.extend(self.gas_price.to_bytes()?);
        buffer.extend(self.body_hash.to_bytes()?);
        buffer.extend(self.dependencies.to_bytes()?);
        buffer.extend(self.chain_name.to_bytes()?);
        Ok(buffer)
    }

    fn serialized_length(&self) -> usize {
        self.account.serialized_length()
            + self.timestamp.serialized_length()
            + self.ttl.serialized_length()
            + self.gas_price.serialized_length()
            + self.body_hash.serialized_length()
            + self.dependencies.serialized_length()
            + self.chain_name.serialized_length()
    }
}

/// The signature of a deploy and the public key of the signer.
#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct Approval {
    signer: PublicKey,
    signature: Signature,
}

impl Approval {
    pub fn create(hash: &DeployHash, secret_key: &SecretKey) -> Self {
        let signer = PublicKey::from(secret_key);
        let signature = sign(hash, secret_key, &signer);
        Self { signer, signature }
    }

    /// Returns the public key.
    pub fn signer(&self) -> &PublicKey {
        &self.signer
    }

    /// Returns the signature.
    pub fn signature(&self) -> &Signature {
        &self.signature
    }
}

impl ToBytes for Approval {
    fn to_bytes(&self) -> Result<Vec<u8>, bytesrepr::Error> {
        let mut buffer = bytesrepr::allocate_buffer(self)?;
        buffer.extend(self.signer.to_bytes()?);
        buffer.extend(self.signature.to_bytes()?);
        Ok(buffer)
    }

    fn serialized_length(&self) -> usize {
        self.signer.serialized_length() + self.signature.serialized_length()
    }
}

/// A signed item sent to the network used to request execution of Wasm.
///
/// Note that constructing a `Deploy` is done via the [`DeployBuilder`].
#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct Deploy {
    pub hash: DeployHash,
    header: DeployHeader,
    payment: ExecutableDeployItem,
    session: ExecutableDeployItem,
    approvals: BTreeSet<Approval>,
    #[serde(skip)]
    is_valid: Option<Result<(), DeployConfigurationFailure>>,
}

impl Deploy {
    /// The default time-to-live for `Deploy`s, i.e. 30 minutes.
    pub const DEFAULT_TTL: TimeDiff = TimeDiff::from_millis(30 * 60 * 1_000);
    /// The default gas price for `Deploy`s, i.e. `1`.
    pub const DEFAULT_GAS_PRICE: u64 = 1;



    /// Constructs a new signed `Deploy`.
    #[allow(clippy::too_many_arguments)]
    fn new(
        timestamp: Timestamp,
        ttl: TimeDiff,
        gas_price: u64,
        dependencies: Vec<DeployHash>,
        chain_name: String,
        payment: ExecutableDeployItem,
        session: ExecutableDeployItem,
        secret_key: &SecretKey,
        account: Option<PublicKey>,
    ) -> Deploy {
        let serialized_body = serialize_body(&payment, &session);
        let body_hash = Digest::hash(&serialized_body);

        let account = account.unwrap_or_else(|| PublicKey::from(secret_key));

        // Remove duplicates.
        let dependencies = dependencies.into_iter().unique().collect();
        let header = DeployHeader {
            account,
            timestamp,
            ttl,
            gas_price,
            body_hash,
            dependencies,
            chain_name,
        };
        let serialized_header = serialize_header(&header);
        let hash = DeployHash::new(Digest::hash(&serialized_header));

        let mut deploy = Deploy {
            hash,
            header,
            payment,
            session,
            approvals: BTreeSet::new(),
            is_valid: None,
        };

        deploy.sign(secret_key);
        deploy
    }

    /// Adds a signature of this deploy's hash to its approvals.
    pub fn sign(&mut self, secret_key: &SecretKey) {
        let approval = Approval::create(&self.hash, secret_key);
        self.approvals.insert(approval);
    }

    /// Returns the hash uniquely identifying this deploy.
    pub fn id(&self) -> &DeployHash {
        &self.hash
    }

    /// Returns the header portion of the deploy.
    pub fn header(&self) -> &DeployHeader {
        &self.header
    }

    /// Returns the payment code of the deploy.
    pub fn payment(&self) -> &ExecutableDeployItem {
        &self.payment
    }

    /// Returns the session code of the deploy.
    pub fn session(&self) -> &ExecutableDeployItem {
        &self.session
    }



    pub fn random(rng: &mut TestRng, secret_key: SecretKey) -> Self {
        let timestamp = Timestamp::random(rng);
        let ttl = TimeDiff::from_millis(rng.gen_range(60_000..3_600_000));
        Deploy::random_with_timestamp_and_ttl(rng, timestamp, ttl, secret_key)
    }

    /// Generates a random instance but using the specified `timestamp` and `ttl`.
    pub fn random_with_timestamp_and_ttl(
        rng: &mut TestRng,
        timestamp: Timestamp,
        ttl: TimeDiff,
        secret_key: SecretKey
    ) -> Self {
        let gas_price = rng.gen_range(1..100);

        let dependencies = vec![
            DeployHash::new(Digest::hash(rng.next_u64().to_le_bytes())),
            DeployHash::new(Digest::hash(rng.next_u64().to_le_bytes())),
            DeployHash::new(Digest::hash(rng.next_u64().to_le_bytes())),
        ];
        let chain_name = String::from("casper-example");

        // We need "amount" in order to be able to get correct info via `deploy_info()`.
        let payment_args = runtime_args! {
            "amount" => U512::from(10),
        };
        let payment = ExecutableDeployItem::StoredContractByName {
            name: String::from("casper-example"),
            entry_point: String::from("example-entry-point"),
            args: payment_args,
        };

        let session = rng.gen();

        Deploy::new(
            timestamp,
            ttl,
            gas_price,
            dependencies,
            chain_name,
            payment,
            session,
            &secret_key,
            None,
        )
    }

}

impl Display for Deploy {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "deploy {{ {}, account {}, timestamp {}, ttl {}, body hash {}, chain name {} }}",
            self.hash,
            self.header.account,
            self.header.timestamp,
            self.header.ttl,
            self.header.body_hash,
            self.header.chain_name
        )
    }
}

fn serialize_header(header: &DeployHeader) -> Vec<u8> {
    header
        .to_bytes()
        .unwrap_or_else(|error| panic!("should serialize deploy header: {}", error))
}

fn serialize_body(payment: &ExecutableDeployItem, session: &ExecutableDeployItem) -> Vec<u8> {
    let mut buffer = payment
        .to_bytes()
        .unwrap_or_else(|error| panic!("should serialize payment code: {}", error));
    buffer.extend(
        session
            .to_bytes()
            .unwrap_or_else(|error| panic!("should serialize session code: {}", error)),
    );
    buffer
}
