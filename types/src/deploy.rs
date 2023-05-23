#[cfg(feature = "sse-data-testing")]
use std::iter;
use std::{
    collections::BTreeSet,
    fmt::{self, Display, Formatter},
};

#[cfg(feature = "sse-data-testing")]
use rand::Rng;
use serde::{Deserialize, Serialize};

#[cfg(feature = "sse-data-testing")]
use casper_types::{bytesrepr::ToBytes, testing::TestRng};
use casper_types::{
    bytesrepr::{self},
    runtime_args, PublicKey, RuntimeArgs, SecretKey, Signature, TimeDiff, Timestamp, U512,
};

use crate::{Digest, ExecutableDeployItem};

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
    pub fn inner(&self) -> &Digest {
        &self.0
    }
}

impl Display for DeployHash {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

#[cfg(feature = "sse-data-testing")]
impl ToBytes for DeployHash {
    fn to_bytes(&self) -> Result<Vec<u8>, bytesrepr::Error> {
        self.0.to_bytes()
    }

    fn serialized_length(&self) -> usize {
        self.0.serialized_length()
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
    pub fn body_hash(&self) -> &Digest {
        &self.body_hash
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

#[cfg(feature = "sse-data-testing")]
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

#[cfg(feature = "sse-data-testing")]
impl Approval {
    pub fn create(hash: &DeployHash, secret_key: &SecretKey) -> Self {
        let signer = PublicKey::from(secret_key);
        let signature = casper_types::sign(hash.0, secret_key, &signer);
        Self { signer, signature }
    }
}

/// A signed item sent to the network used to request execution of Wasm.
#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct Deploy {
    hash: DeployHash,
    header: DeployHeader,
    payment: ExecutableDeployItem,
    session: ExecutableDeployItem,
    approvals: BTreeSet<Approval>,
}

impl Deploy {
    /// Returns the hash uniquely identifying this deploy.
    pub fn hash(&self) -> &DeployHash {
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

    /// Returns the `Approval`s of the deploy.
    pub fn approvals(&self) -> &BTreeSet<Approval> {
        &self.approvals
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

#[cfg(feature = "sse-data-testing")]
impl Deploy {
    pub fn random(rng: &mut TestRng) -> Self {
        let timestamp = Timestamp::random(rng);
        let ttl = TimeDiff::from_millis(rng.gen_range(60_000..3_600_000));
        Deploy::random_with_timestamp_and_ttl(rng, timestamp, ttl)
    }

    /// Generates a random instance but using the specified `timestamp` and `ttl`.
    pub fn random_with_timestamp_and_ttl(
        rng: &mut TestRng,
        timestamp: Timestamp,
        ttl: TimeDiff,
    ) -> Self {
        // Create the deploy "body", i.e. the payment and session items.
        //
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

        // Create the deploy header.
        let secret_key = SecretKey::random(rng);
        let account = PublicKey::from(&secret_key);
        let gas_price = rng.gen_range(1..100);
        let body_hash = Digest::hash(serialize_body(&payment, &session));
        let dependencies_count = rng.gen_range(0..4);
        let dependencies = iter::repeat_with(|| DeployHash::new(Digest::random(rng)))
            .take(dependencies_count)
            .collect();
        let chain_name = String::from("casper-example");
        let header = DeployHeader {
            account,
            timestamp,
            ttl,
            gas_price,
            body_hash,
            dependencies,
            chain_name,
        };

        // Create the deploy hash and approval.
        let hash = DeployHash::new(Digest::hash(serialize_header(&header)));
        let approvals = iter::once(Approval::create(&hash, &secret_key)).collect();

        Deploy {
            hash,
            header,
            payment,
            session,
            approvals,
        }
    }
}

#[cfg(feature = "sse-data-testing")]
fn serialize_header(header: &DeployHeader) -> Vec<u8> {
    header
        .to_bytes()
        .unwrap_or_else(|error| panic!("should serialize deploy header: {}", error))
}

#[cfg(feature = "sse-data-testing")]
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
