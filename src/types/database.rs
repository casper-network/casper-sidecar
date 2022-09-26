use anyhow::Error;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use casper_node::types::FinalitySignature as FinSig;

use crate::types::sse_events::{
    BlockAdded, DeployAccepted, DeployExpired, DeployProcessed, Fault, FinalitySignature, Step,
};

/// Describes a reference for the writing interface of an 'Event Store' database.
/// There is a one-to-one relationship between each method and each event that can be received from the node.
/// Each method takes the `data` and `id` fields as well as the source IP address (useful for tying the node-specific `id` to the relevant node).
///
/// For a reference implementation using Sqlite see, [SqliteDb](crate::SqliteDb)
#[async_trait]
pub trait DatabaseWriter {
    /// Save a BlockAdded event to the database.
    ///
    /// * `block_added`: the [BlockAdded] from the `data` field.
    /// * `event_id`: the node-specific assigned `id`.
    /// * `event_source_address`: the IP address of the source node.
    async fn save_block_added(
        &self,
        block_added: BlockAdded,
        event_id: u32,
        event_source_address: String,
    ) -> Result<usize, Error>;
    /// Save a DeployAccepted event to the database.
    ///
    /// * `deploy_accepted`: the [DeployAccepted] from the `data` field.
    /// * `event_id`: the node-specific assigned `id`.
    /// * `event_source_address`: the IP address of the source node.
    async fn save_deploy_accepted(
        &self,
        deploy_accepted: DeployAccepted,
        event_id: u32,
        event_source_address: String,
    ) -> Result<usize, Error>;
    /// Save a DeployProcessed event to the database.
    ///
    /// * `deploy_accepted`: the [DeployProcessed] from the `data` field.
    /// * `event_id`: the node-specific assigned `id`.
    /// * `event_source_address`: the IP address of the source node.
    async fn save_deploy_processed(
        &self,
        deploy_processed: DeployProcessed,
        event_id: u32,
        event_source_address: String,
    ) -> Result<usize, Error>;
    /// Save a DeployExpired event to the database.
    ///
    /// * `deploy_expired`: the [DeployExpired] from the `data` field.
    /// * `event_id`: the node-specific assigned `id`.
    /// * `event_source_address`: the IP address of the source node.
    async fn save_deploy_expired(
        &self,
        deploy_expired: DeployExpired,
        event_id: u32,
        event_source_address: String,
    ) -> Result<usize, Error>;
    /// Save a Fault event to the database.
    ///
    /// * `fault`: the [Fault] from the `data` field.
    /// * `event_id`: the node-specific assigned `id`.
    /// * `event_source_address`: the IP address of the source node.
    async fn save_fault(
        &self,
        fault: Fault,
        event_id: u32,
        event_source_address: String,
    ) -> Result<usize, Error>;
    /// Save a FinalitySignature event to the database.
    ///
    /// * `finality_signature`: the [FinalitySignature] from the `data` field.
    /// * `event_id`: the node-specific assigned `id`.
    /// * `event_source_address`: the IP address of the source node.
    async fn save_finality_signature(
        &self,
        finality_signature: FinalitySignature,
        event_id: u32,
        event_source_address: String,
    ) -> Result<usize, Error>;
    /// Save a Step event to the database.
    ///
    /// * `step`: the [Step] from the `data` field.
    /// * `event_id`: the node-specific assigned `id`.
    /// * `event_source_address`: the IP address of the source node.
    async fn save_step(
        &self,
        step: Step,
        event_id: u32,
        event_source_address: String,
    ) -> Result<usize, Error>;
}

/// Describes a reference for the reading interface of an 'Event Store' database.
///
/// For a reference implementation using Sqlite see, [SqliteDb](crate::SqliteDb)
#[async_trait]
pub trait DatabaseReader {
    /// Returns the latest [BlockAdded] by height from the database.
    async fn get_latest_block(&self) -> Result<BlockAdded, DatabaseRequestError>;
    /// Returns the [BlockAdded] corresponding to the provided [height].
    async fn get_block_by_height(&self, height: u64) -> Result<BlockAdded, DatabaseRequestError>;
    /// Returns the [BlockAdded] corresponding to the provided hex-encoded [hash].
    async fn get_block_by_hash(&self, hash: &str) -> Result<BlockAdded, DatabaseRequestError>;
    /// Returns the aggregate of the latest deploy's events.
    async fn get_latest_deploy_aggregate(
        &self,
    ) -> Result<AggregateDeployInfo, DatabaseRequestError>;
    /// Returns an aggregate of the deploy's events corresponding to the given hex-encoded `hash`
    async fn get_deploy_aggregate_by_hash(
        &self,
        hash: &str,
    ) -> Result<AggregateDeployInfo, DatabaseRequestError>;
    /// Returns the [DeployAccepted] corresponding to the given hex-encoded `hash`
    async fn get_deploy_accepted_by_hash(
        &self,
        hash: &str,
    ) -> Result<DeployAccepted, DatabaseRequestError>;
    /// Returns the [DeployProcessed] corresponding to the given hex-encoded `hash`
    async fn get_deploy_processed_by_hash(
        &self,
        hash: &str,
    ) -> Result<DeployProcessed, DatabaseRequestError>;
    /// Returns a boolean representing the expired state of the deploy corresponding to the given hex-encoded `hash`.
    ///
    /// * If there is a record present it will return `true` meaning the [Deploy] has expired.
    /// * If there is no record present it will return [NotFound](DatabaseRequestError::NotFound) rather than `false`.
    /// This is because the lack of a record does not definitely mean it hasn't expired. The deploy could have expired
    /// prior to sidecar's start point. Calling [get_deploy_aggregate_by_hash] can help in this case, if there is a [DeployAccepted]
    /// without a corresponding [DeployExpired] then you can assert that it truly has not expired.
    async fn get_deploy_expired_by_hash(&self, hash: &str) -> Result<bool, DatabaseRequestError>;
    /// Returns the [Step] event for the given era.
    async fn get_step_by_era(&self, era: u64) -> Result<Step, DatabaseRequestError>;
    /// Returns all [Fault]s that correspond to the given hex-encoded [public_key]
    async fn get_faults_by_public_key(
        &self,
        public_key: &str,
    ) -> Result<Vec<Fault>, DatabaseRequestError>;
    /// Returns all [Fault]s that occurred in the given [era]
    async fn get_faults_by_era(&self, era: u64) -> Result<Vec<Fault>, DatabaseRequestError>;
    /// Returns all [FinalitySignature](casper_node::types::FinalitySignature)s for the given hex-encoded `block_hash`.
    async fn get_finality_signatures_by_block(
        &self,
        block_hash: &str,
    ) -> Result<Vec<FinSig>, DatabaseRequestError>;
}

/// The database was unable to fulfil the request.
#[derive(Debug)]
pub enum DatabaseRequestError {
    /// The requested record was not present in the database.
    NotFound,
    /// An error occurred serialising or deserialising data from the database.
    Serialisation(Error),
    /// An error occurred somewhere unexpected.
    Unhandled(Error),
}

#[derive(Debug, Deserialize, Serialize)]
pub struct AggregateDeployInfo {
    pub(crate) deploy_hash: String,
    pub(crate) deploy_accepted: Option<DeployAccepted>,
    pub(crate) deploy_processed: Option<DeployProcessed>,
    pub(crate) deploy_expired: bool,
}

#[cfg(test)]
mod testing {
    use casper_hashing::Digest;
    use casper_node::types::DeployHash;
    use casper_types::testing::TestRng;

    use rand::Rng;

    use super::AggregateDeployInfo;
    use crate::types::sse_events::{DeployAccepted, DeployProcessed};

    impl AggregateDeployInfo {
        pub fn random(test_rng: &mut TestRng, with_hash: Option<String>) -> Self {
            let deploy_accepted = DeployAccepted::random(test_rng);

            let deploy_hash = with_hash.unwrap_or_else(|| deploy_accepted.hex_encoded_hash());

            match test_rng.gen_range(0..=2) {
                // Accepted
                0 => AggregateDeployInfo {
                    deploy_hash,
                    deploy_accepted: Some(deploy_accepted),
                    deploy_processed: None,
                    deploy_expired: false,
                },
                // Accepted -> Processed
                1 => {
                    let deploy_processed = DeployProcessed::random(
                        test_rng,
                        Some(DeployHash::from(
                            Digest::from_hex(deploy_hash.clone())
                                .expect("Error creating Digest from hash"),
                        )),
                    );

                    AggregateDeployInfo {
                        deploy_hash,
                        deploy_accepted: Some(deploy_accepted),
                        deploy_processed: Some(deploy_processed),
                        deploy_expired: false,
                    }
                }
                // Accepted -> Expired
                2 => AggregateDeployInfo {
                    deploy_hash,
                    deploy_accepted: Some(deploy_accepted),
                    deploy_processed: None,
                    deploy_expired: true,
                },
                _ => unreachable!(),
            }
        }
    }
}
