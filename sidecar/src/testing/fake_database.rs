use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use casper_types::AsymmetricType;
use rand::Rng;

use casper_node::types::FinalitySignature as FinSig;
use casper_event_types::test_rng::TestRng;

use crate::types::{
    database::{
        DatabaseReadError, DatabaseReader, DatabaseWriteError, DatabaseWriter, DeployAggregate,
    },
    sse_events::*,
};

#[derive(Clone)]
pub struct FakeDatabase {
    data: Arc<Mutex<HashMap<String, String>>>,
}

impl FakeDatabase {
    pub(crate) fn new() -> Self {
        Self {
            data: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Creates random SSE event data and saves them, returning the identifiers for each record.
    pub(crate) async fn populate_with_events(
        &self,
    ) -> Result<IdentifiersForStoredEvents, DatabaseWriteError> {
        let mut rng = TestRng::new();

        let block_added = BlockAdded::random(&mut rng);
        let deploy_accepted = DeployAccepted::random(&mut rng);
        let deploy_processed = DeployProcessed::random(&mut rng, None);
        let deploy_expired = DeployExpired::random(&mut rng, None);
        let fault = Fault::random(&mut rng);
        let finality_signature = FinalitySignature::random(&mut rng);
        let step = Step::random(&mut rng);

        let test_stored_keys = IdentifiersForStoredEvents {
            block_added_hash: block_added.hex_encoded_hash(),
            block_added_height: block_added.get_height(),
            deploy_accepted_hash: deploy_accepted.hex_encoded_hash(),
            deploy_processed_hash: deploy_processed.hex_encoded_hash(),
            deploy_expired_hash: deploy_expired.hex_encoded_hash(),
            fault_era_id: fault.era_id.value(),
            fault_public_key: fault.public_key.to_hex(),
            finality_signatures_block_hash: finality_signature.hex_encoded_block_hash(),
            step_era_id: step.era_id.value(),
        };

        self.save_block_added(block_added, rng.gen(), "127.0.0.1".to_string())
            .await?;
        self.save_deploy_accepted(deploy_accepted, rng.gen(), "127.0.0.1".to_string())
            .await?;
        self.save_deploy_processed(deploy_processed, rng.gen(), "127.0.0.1".to_string())
            .await?;
        self.save_deploy_expired(deploy_expired, rng.gen(), "127.0.0.1".to_string())
            .await?;
        self.save_fault(fault, rng.gen(), "127.0.0.1".to_string())
            .await?;
        self.save_finality_signature(finality_signature, rng.gen(), "127.0.0.1".to_string())
            .await?;
        self.save_step(step, rng.gen(), "127.0.0.1".to_string())
            .await?;

        Ok(test_stored_keys)
    }
}

#[async_trait]
impl DatabaseWriter for FakeDatabase {
    #[allow(unused)]
    async fn save_block_added(
        &self,
        block_added: BlockAdded,
        event_id: u32,
        event_source_address: String,
    ) -> Result<usize, DatabaseWriteError> {
        let mut data = self.data.lock().expect("Error acquiring lock on data");

        let identifier_hash = block_added.hex_encoded_hash();
        let identifier_height = block_added.get_height().to_string();
        let stringified_event =
            serde_json::to_string(&block_added).expect("Error serialising event data");

        // For the sake of keeping the test fixture simple, I'm saving the event twice, one record for each identifier.

        data.insert(identifier_hash, stringified_event.clone());

        data.insert(identifier_height, stringified_event);

        Ok(0)
    }

    #[allow(unused)]
    async fn save_deploy_accepted(
        &self,
        deploy_accepted: DeployAccepted,
        event_id: u32,
        event_source_address: String,
    ) -> Result<usize, DatabaseWriteError> {
        let mut data = self.data.lock().expect("Error acquiring lock on data");

        let hash = deploy_accepted.hex_encoded_hash();
        // This is suffixed to allow storage of each deploy state event without overwriting.
        let identifier = format!("{}-accepted", hash);
        let stringified_event =
            serde_json::to_string(&deploy_accepted).expect("Error serialising event data");

        data.insert(identifier, stringified_event);

        Ok(0)
    }

    #[allow(unused)]
    async fn save_deploy_processed(
        &self,
        deploy_processed: DeployProcessed,
        event_id: u32,
        event_source_address: String,
    ) -> Result<usize, DatabaseWriteError> {
        let mut data = self.data.lock().expect("Error acquiring lock on data");

        let hash = deploy_processed.hex_encoded_hash();
        // This is suffixed to allow storage of each deploy state event without overwriting.
        let identifier = format!("{}-processed", hash);
        let stringified_event =
            serde_json::to_string(&deploy_processed).expect("Error serialising event data");

        data.insert(identifier, stringified_event);

        Ok(0)
    }

    #[allow(unused)]
    async fn save_deploy_expired(
        &self,
        deploy_expired: DeployExpired,
        event_id: u32,
        event_source_address: String,
    ) -> Result<usize, DatabaseWriteError> {
        let mut data = self.data.lock().expect("Error acquiring lock on data");

        let hash = deploy_expired.hex_encoded_hash();
        // This is suffixed to allow storage of each deploy state event without overwriting.
        let identifier = format!("{}-expired", hash);
        let stringified_event = serde_json::to_string(&true).expect("Error serialising event data");

        data.insert(identifier, stringified_event);

        Ok(0)
    }

    #[allow(unused)]
    async fn save_fault(
        &self,
        fault: Fault,
        event_id: u32,
        event_source_address: String,
    ) -> Result<usize, DatabaseWriteError> {
        let mut data = self.data.lock().expect("Error acquiring lock on data");

        let identifier_era = fault.era_id.value().to_string();
        let identifier_public_key = fault.public_key.to_hex();

        let stringified_event =
            serde_json::to_string(&fault).expect("Error serialising event data");

        // For the sake of keeping the test fixture simple, I'm saving the event twice, one record for each identifier.

        data.insert(identifier_era, stringified_event.clone());

        data.insert(identifier_public_key, stringified_event);

        Ok(0)
    }

    #[allow(unused)]
    async fn save_finality_signature(
        &self,
        finality_signature: FinalitySignature,
        event_id: u32,
        event_source_address: String,
    ) -> Result<usize, DatabaseWriteError> {
        let mut data = self.data.lock().expect("Error acquiring lock on data");

        let identifier = finality_signature.hex_encoded_block_hash();
        let stringified_event =
            serde_json::to_string(&finality_signature).expect("Error serialising event data");

        data.insert(identifier, stringified_event);

        Ok(0)
    }

    #[allow(unused)]
    async fn save_step(
        &self,
        step: Step,
        event_id: u32,
        event_source_address: String,
    ) -> Result<usize, DatabaseWriteError> {
        let mut data = self.data.lock().expect("Error acquiring lock on data");

        let identifier = step.era_id.value().to_string();
        let stringified_event = serde_json::to_string(&step).expect("Error serialising event data");

        data.insert(identifier, stringified_event);

        Ok(0)
    }

    #[allow(unused)]
    async fn save_shutdown(
        &self,
        event_id: u32,
        event_source_address: String,
    ) -> Result<usize, DatabaseWriteError> {
        let mut data = self.data.lock().expect("Error acquiring lock on data");
        let unix_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();
        let event_key = format!("{}-{}", event_source_address, unix_timestamp);
        let stringified_event = serde_json::to_string("{}").expect("Error serialising event data");

        data.insert(event_key, stringified_event);
        Ok(0)
    }
}

#[async_trait]
impl DatabaseReader for FakeDatabase {
    async fn get_latest_block(&self) -> Result<BlockAdded, DatabaseReadError> {
        let mut test_rng = TestRng::new();

        let block_added = BlockAdded::random(&mut test_rng);

        Ok(block_added)
    }

    async fn get_block_by_height(&self, height: u64) -> Result<BlockAdded, DatabaseReadError> {
        let data = self.data.lock().expect("Error acquiring lock on data");

        return if let Some(event) = data.get(&height.to_string()) {
            serde_json::from_str::<BlockAdded>(event).map_err(DatabaseReadError::Serialisation)
        } else {
            Err(DatabaseReadError::NotFound)
        };
    }

    async fn get_block_by_hash(&self, hash: &str) -> Result<BlockAdded, DatabaseReadError> {
        let data = self.data.lock().expect("Error acquiring lock on data");

        return if let Some(event) = data.get(hash) {
            serde_json::from_str::<BlockAdded>(event).map_err(DatabaseReadError::Serialisation)
        } else {
            Err(DatabaseReadError::NotFound)
        };
    }

    async fn get_deploy_aggregate_by_hash(
        &self,
        hash: &str,
    ) -> Result<DeployAggregate, DatabaseReadError> {
        let data = self.data.lock().expect("Error acquiring lock on data");

        let accepted_key = format!("{}-accepted", hash);
        let processed_key = format!("{}-processed", hash);
        let expired_key = format!("{}-expired", hash);

        return if let Some(accepted) = data.get(&accepted_key) {
            let deploy_accepted = serde_json::from_str::<DeployAccepted>(accepted)
                .map_err(DatabaseReadError::Serialisation)?;

            if let Some(processed) = data.get(&processed_key) {
                let deploy_processed = serde_json::from_str::<DeployProcessed>(processed)
                    .map_err(DatabaseReadError::Serialisation)?;

                Ok(DeployAggregate {
                    deploy_hash: hash.to_string(),
                    deploy_accepted: Some(deploy_accepted),
                    deploy_processed: Some(deploy_processed),
                    deploy_expired: false,
                })
            } else if data.get(&expired_key).is_some() {
                Ok(DeployAggregate {
                    deploy_hash: hash.to_string(),
                    deploy_accepted: Some(deploy_accepted),
                    deploy_processed: None,
                    deploy_expired: true,
                })
            } else {
                Ok(DeployAggregate {
                    deploy_hash: hash.to_string(),
                    deploy_accepted: Some(deploy_accepted),
                    deploy_processed: None,
                    deploy_expired: false,
                })
            }
        } else {
            Err(DatabaseReadError::NotFound)
        };
    }

    async fn get_deploy_accepted_by_hash(
        &self,
        hash: &str,
    ) -> Result<DeployAccepted, DatabaseReadError> {
        let identifier = format!("{}-accepted", hash);

        let data = self.data.lock().expect("Error acquiring lock on data");

        return if let Some(event) = data.get(&identifier) {
            serde_json::from_str::<DeployAccepted>(event).map_err(DatabaseReadError::Serialisation)
        } else {
            Err(DatabaseReadError::NotFound)
        };
    }

    async fn get_deploy_processed_by_hash(
        &self,
        hash: &str,
    ) -> Result<DeployProcessed, DatabaseReadError> {
        let identifier = format!("{}-processed", hash);

        let data = self.data.lock().expect("Error acquiring lock on data");

        return if let Some(event) = data.get(&identifier) {
            serde_json::from_str::<DeployProcessed>(event).map_err(DatabaseReadError::Serialisation)
        } else {
            Err(DatabaseReadError::NotFound)
        };
    }

    async fn get_deploy_expired_by_hash(&self, hash: &str) -> Result<bool, DatabaseReadError> {
        let identifier = format!("{}-expired", hash);

        let data = self.data.lock().expect("Error acquiring lock on data");

        return if let Some(event) = data.get(&identifier) {
            serde_json::from_str::<bool>(event).map_err(DatabaseReadError::Serialisation)
        } else {
            Err(DatabaseReadError::NotFound)
        };
    }

    async fn get_faults_by_public_key(
        &self,
        public_key: &str,
    ) -> Result<Vec<Fault>, DatabaseReadError> {
        let data = self.data.lock().expect("Error acquiring lock on data");

        return if let Some(event) = data.get(public_key) {
            let fault =
                serde_json::from_str::<Fault>(event).map_err(DatabaseReadError::Serialisation)?;
            Ok(vec![fault])
        } else {
            Err(DatabaseReadError::NotFound)
        };
    }

    async fn get_faults_by_era(&self, era: u64) -> Result<Vec<Fault>, DatabaseReadError> {
        let data = self.data.lock().expect("Error acquiring lock on data");

        return if let Some(event) = data.get(&era.to_string()) {
            let fault =
                serde_json::from_str::<Fault>(event).map_err(DatabaseReadError::Serialisation)?;
            Ok(vec![fault])
        } else {
            Err(DatabaseReadError::NotFound)
        };
    }

    async fn get_finality_signatures_by_block(
        &self,
        block_hash: &str,
    ) -> Result<Vec<FinSig>, DatabaseReadError> {
        let data = self.data.lock().expect("Error acquiring lock on data");

        return if let Some(event) = data.get(block_hash) {
            let finality_signature =
                serde_json::from_str::<FinSig>(event).map_err(DatabaseReadError::Serialisation)?;
            Ok(vec![finality_signature])
        } else {
            Err(DatabaseReadError::NotFound)
        };
    }

    async fn get_step_by_era(&self, era: u64) -> Result<Step, DatabaseReadError> {
        let data = self.data.lock().expect("Error acquiring lock on data");

        return if let Some(event) = data.get(&era.to_string()) {
            serde_json::from_str::<Step>(event).map_err(DatabaseReadError::Serialisation)
        } else {
            Err(DatabaseReadError::NotFound)
        };
    }
}

pub struct IdentifiersForStoredEvents {
    pub block_added_hash: String,
    pub block_added_height: u64,
    pub deploy_accepted_hash: String,
    pub deploy_processed_hash: String,
    pub deploy_expired_hash: String,
    pub fault_public_key: String,
    pub fault_era_id: u64,
    pub finality_signatures_block_hash: String,
    pub step_era_id: u64,
}
