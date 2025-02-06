use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use casper_types::{testing::TestRng, AsymmetricType, FinalitySignature as FinSig};
use rand::Rng;

use crate::database::types::SseEnvelope;
use crate::types::{
    database::{
        DatabaseReadError, DatabaseReader, DatabaseWriteError, DatabaseWriter, Migration,
        TransactionAggregate, TransactionTypeId,
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
    #[allow(clippy::too_many_lines)]
    pub(crate) async fn populate_with_events(
        &self,
    ) -> Result<IdentifiersForStoredEvents, DatabaseWriteError> {
        let mut rng = TestRng::new();
        let block_added = BlockAdded::random(&mut rng);
        let transaction_accepted = TransactionAccepted::random(&mut rng);
        let transaction_processed = TransactionProcessed::random(&mut rng, None);
        let transaction_expired = TransactionExpired::random(&mut rng, None);
        let fault = Fault::random(&mut rng);
        let finality_signature = FinalitySignature::random(&mut rng);
        let step = Step::random(&mut rng);

        let test_stored_keys = IdentifiersForStoredEvents {
            block_added_hash: block_added.hex_encoded_hash(),
            block_added_height: block_added.get_height(),
            transaction_accepted_info: (
                transaction_accepted.hex_encoded_hash(),
                transaction_accepted.api_transaction_type_id(),
            ),
            transaction_processed_info: (
                transaction_processed.hex_encoded_hash(),
                transaction_processed.api_transaction_type_id(),
            ),
            transaction_expired_info: (
                transaction_expired.hex_encoded_hash(),
                transaction_expired.api_transaction_type_id(),
            ),
            fault_era_id: fault.era_id.value(),
            fault_public_key: fault.public_key.to_hex(),
            finality_signatures_block_hash: finality_signature.hex_encoded_block_hash(),
            step_era_id: step.era_id.value(),
        };

        self.save_block_added_with_event_log_data(block_added, &mut rng)
            .await?;
        self.save_transaction_accepted_with_event_log_data(transaction_accepted, &mut rng)
            .await?;
        self.save_transaction_processed_with_event_log_data(transaction_processed, &mut rng)
            .await?;
        self.save_transaction_expired_with_event_log_data(transaction_expired, &mut rng)
            .await?;
        self.save_fault_with_event_log_data(fault, &mut rng).await?;
        self.save_finality_signature_with_event_log_data(finality_signature, &mut rng)
            .await?;
        self.save_step_with_event_log_data(step, rng).await?;

        Ok(test_stored_keys)
    }

    async fn save_step_with_event_log_data(
        &self,
        step: Step,
        mut rng: TestRng,
    ) -> Result<(), DatabaseWriteError> {
        self.save_step(
            step,
            rng.gen(),
            "127.0.0.1".to_string(),
            "1.1.1".to_string(),
            "network-1".to_string(),
        )
        .await?;
        Ok(())
    }

    async fn save_finality_signature_with_event_log_data(
        &self,
        finality_signature: FinalitySignature,
        rng: &mut TestRng,
    ) -> Result<(), DatabaseWriteError> {
        self.save_finality_signature(
            finality_signature,
            rng.gen(),
            "127.0.0.1".to_string(),
            "1.1.1".to_string(),
            "network-1".to_string(),
        )
        .await?;
        Ok(())
    }

    async fn save_fault_with_event_log_data(
        &self,
        fault: Fault,
        rng: &mut TestRng,
    ) -> Result<(), DatabaseWriteError> {
        self.save_fault(
            fault,
            rng.gen(),
            "127.0.0.1".to_string(),
            "1.1.1".to_string(),
            "network-1".to_string(),
        )
        .await?;
        Ok(())
    }

    async fn save_transaction_expired_with_event_log_data(
        &self,
        transaction_expired: TransactionExpired,
        rng: &mut TestRng,
    ) -> Result<(), DatabaseWriteError> {
        self.save_transaction_expired(
            transaction_expired,
            rng.gen(),
            "127.0.0.1".to_string(),
            "1.1.1".to_string(),
            "network-1".to_string(),
        )
        .await?;
        Ok(())
    }

    async fn save_transaction_processed_with_event_log_data(
        &self,
        transaction_processed: TransactionProcessed,
        rng: &mut TestRng,
    ) -> Result<(), DatabaseWriteError> {
        self.save_transaction_processed(
            transaction_processed,
            rng.gen(),
            "127.0.0.1".to_string(),
            "1.1.1".to_string(),
            "network-1".to_string(),
        )
        .await?;
        Ok(())
    }

    async fn save_transaction_accepted_with_event_log_data(
        &self,
        transaction_accepted: TransactionAccepted,
        rng: &mut TestRng,
    ) -> Result<(), DatabaseWriteError> {
        self.save_transaction_accepted(
            transaction_accepted,
            rng.gen(),
            "127.0.0.1".to_string(),
            "1.1.1".to_string(),
            "network-1".to_string(),
        )
        .await?;
        Ok(())
    }

    async fn save_block_added_with_event_log_data(
        &self,
        block_added: BlockAdded,
        rng: &mut TestRng,
    ) -> Result<(), DatabaseWriteError> {
        self.save_block_added(
            block_added,
            rng.gen(),
            "127.0.0.1".to_string(),
            "1.1.1".to_string(),
            "network-1".to_string(),
        )
        .await?;
        Ok(())
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
        api_version: String,
        network_name: String,
    ) -> Result<u64, DatabaseWriteError> {
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
    async fn save_transaction_accepted(
        &self,
        transaction_accepted: TransactionAccepted,
        event_id: u32,
        event_source_address: String,
        api_version: String,
        network_name: String,
    ) -> Result<u64, DatabaseWriteError> {
        let mut data = self.data.lock().expect("Error acquiring lock on data");

        let hash = transaction_accepted.hex_encoded_hash();
        // This is suffixed to allow storage of each transaction state event without overwriting.
        let identifier = format!("{}-accepted", hash);
        let stringified_event =
            serde_json::to_string(&transaction_accepted).expect("Error serialising event data");

        data.insert(identifier, stringified_event);

        Ok(0)
    }

    #[allow(unused)]
    async fn save_transaction_processed(
        &self,
        transaction_processed: TransactionProcessed,
        event_id: u32,
        event_source_address: String,
        api_version: String,
        network_name: String,
    ) -> Result<u64, DatabaseWriteError> {
        let mut data = self.data.lock().expect("Error acquiring lock on data");

        let hash = transaction_processed.hex_encoded_hash();
        // This is suffixed to allow storage of each transaction state event without overwriting.
        let identifier = format!("{}-processed", hash);
        let stringified_event =
            serde_json::to_string(&transaction_processed).expect("Error serialising event data");

        data.insert(identifier, stringified_event);

        Ok(0)
    }

    #[allow(unused)]
    async fn save_transaction_expired(
        &self,
        transaction_expired: TransactionExpired,
        event_id: u32,
        event_source_address: String,
        api_version: String,
        network_name: String,
    ) -> Result<u64, DatabaseWriteError> {
        let mut data = self.data.lock().expect("Error acquiring lock on data");

        let hash = transaction_expired.hex_encoded_hash();
        // This is suffixed to allow storage of each transaction state event without overwriting.
        let identifier = format!("{}-expired", hash);
        let stringified_event =
            serde_json::to_string(&transaction_expired).expect("Error serialising event data");

        data.insert(identifier, stringified_event);

        Ok(0)
    }

    #[allow(unused)]
    async fn save_fault(
        &self,
        fault: Fault,
        event_id: u32,
        event_source_address: String,
        api_version: String,
        network_name: String,
    ) -> Result<u64, DatabaseWriteError> {
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
        api_version: String,
        network_name: String,
    ) -> Result<u64, DatabaseWriteError> {
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
        api_version: String,
        network_name: String,
    ) -> Result<u64, DatabaseWriteError> {
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
        api_version: String,
        network_name: String,
    ) -> Result<u64, DatabaseWriteError> {
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

    async fn execute_migration(&self, _migration: Migration) -> Result<(), DatabaseWriteError> {
        //Nothing to do here
        Ok(())
    }
}

#[async_trait]
impl DatabaseReader for FakeDatabase {
    async fn get_latest_block(&self) -> Result<SseEnvelope<BlockAdded>, DatabaseReadError> {
        let mut test_rng = TestRng::new();

        let block_added = BlockAdded::random(&mut test_rng);

        Ok(SseEnvelope::new(
            block_added,
            "2.0.0".to_string(),
            "network-1".to_string(),
        ))
    }

    async fn get_block_by_height(
        &self,
        height: u64,
    ) -> Result<SseEnvelope<BlockAdded>, DatabaseReadError> {
        let data = self.data.lock().expect("Error acquiring lock on data");

        return if let Some(event) = data.get(&height.to_string()) {
            let entity = serde_json::from_str::<BlockAdded>(event)
                .map_err(DatabaseReadError::Serialisation)?;
            Ok(SseEnvelope::new(
                entity,
                "2.0.0".to_string(),
                "network-1".to_string(),
            ))
        } else {
            Err(DatabaseReadError::NotFound)
        };
    }

    async fn get_block_by_hash(
        &self,
        hash: &str,
    ) -> Result<SseEnvelope<BlockAdded>, DatabaseReadError> {
        let data = self.data.lock().expect("Error acquiring lock on data");

        return if let Some(event) = data.get(hash) {
            let entity = serde_json::from_str::<BlockAdded>(event)
                .map_err(DatabaseReadError::Serialisation)?;
            Ok(SseEnvelope::new(
                entity,
                "2.0.0".to_string(),
                "network-1".to_string(),
            ))
        } else {
            Err(DatabaseReadError::NotFound)
        };
    }

    #[allow(clippy::too_many_lines)]
    async fn get_transaction_aggregate_by_identifier(
        &self,
        _transaction_type: &TransactionTypeId,
        hash: &str,
    ) -> Result<TransactionAggregate, DatabaseReadError> {
        let data = self.data.lock().expect("Error acquiring lock on data");

        let accepted_key = format!("{}-accepted", hash);
        let processed_key = format!("{}-processed", hash);
        let expired_key = format!("{}-expired", hash);

        return if let Some(accepted) = data.get(&accepted_key) {
            let transaction_accepted = serde_json::from_str::<TransactionAccepted>(accepted)
                .map_err(DatabaseReadError::Serialisation)?;

            if let Some(processed) = data.get(&processed_key) {
                let transaction_processed = serde_json::from_str::<TransactionProcessed>(processed)
                    .map_err(DatabaseReadError::Serialisation)?;

                Ok(TransactionAggregate {
                    transaction_hash: hash.to_string(),
                    transaction_accepted: Some(SseEnvelope::new(
                        transaction_accepted,
                        "2.0.0".to_string(),
                        "network-1".to_string(),
                    )),
                    transaction_processed: Some(SseEnvelope::new(
                        transaction_processed,
                        "2.0.0".to_string(),
                        "network-1".to_string(),
                    )),
                    transaction_expired: false,
                })
            } else if data.get(&expired_key).is_some() {
                let transaction_expired = match data.get(&expired_key) {
                    None => None,
                    Some(raw) => Some(
                        serde_json::from_str::<TransactionExpired>(raw)
                            .map_err(DatabaseReadError::Serialisation)?,
                    ),
                };
                Ok(TransactionAggregate {
                    transaction_hash: hash.to_string(),
                    transaction_accepted: Some(SseEnvelope::new(
                        transaction_accepted,
                        "2.0.0".to_string(),
                        "network-1".to_string(),
                    )),
                    transaction_processed: None,
                    transaction_expired: transaction_expired.is_some(),
                })
            } else {
                Ok(TransactionAggregate {
                    transaction_hash: hash.to_string(),
                    transaction_accepted: Some(SseEnvelope::new(
                        transaction_accepted,
                        "2.0.0".to_string(),
                        "network-1".to_string(),
                    )),
                    transaction_processed: None,
                    transaction_expired: false,
                })
            }
        } else {
            Err(DatabaseReadError::NotFound)
        };
    }

    async fn get_transaction_accepted_by_hash(
        &self,
        _transaction_type: &TransactionTypeId,
        hash: &str,
    ) -> Result<SseEnvelope<TransactionAccepted>, DatabaseReadError> {
        let identifier = format!("{}-accepted", hash);

        let data = self.data.lock().expect("Error acquiring lock on data");

        return if let Some(event) = data.get(&identifier) {
            let entity = serde_json::from_str::<TransactionAccepted>(event)
                .map_err(DatabaseReadError::Serialisation)?;
            Ok(SseEnvelope::new(
                entity,
                "2.0.0".to_string(),
                "network-1".to_string(),
            ))
        } else {
            Err(DatabaseReadError::NotFound)
        };
    }

    async fn get_transaction_processed_by_hash(
        &self,
        _transaction_type: &TransactionTypeId,
        hash: &str,
    ) -> Result<SseEnvelope<TransactionProcessed>, DatabaseReadError> {
        let identifier = format!("{}-processed", hash);

        let data = self.data.lock().expect("Error acquiring lock on data");

        return if let Some(event) = data.get(&identifier) {
            let entity = serde_json::from_str::<TransactionProcessed>(event)
                .map_err(DatabaseReadError::Serialisation)?;
            Ok(SseEnvelope::new(
                entity,
                "2.0.0".to_string(),
                "network-1".to_string(),
            ))
        } else {
            Err(DatabaseReadError::NotFound)
        };
    }

    async fn get_transaction_expired_by_hash(
        &self,
        _transaction_type: &TransactionTypeId,
        hash: &str,
    ) -> Result<SseEnvelope<TransactionExpired>, DatabaseReadError> {
        let identifier = format!("{}-expired", hash);

        let data = self.data.lock().expect("Error acquiring lock on data");

        return if let Some(event) = data.get(&identifier) {
            let entity = serde_json::from_str::<TransactionExpired>(event)
                .map_err(DatabaseReadError::Serialisation)?;
            Ok(SseEnvelope::new(
                entity,
                "2.0.0".to_string(),
                "network-1".to_string(),
            ))
        } else {
            Err(DatabaseReadError::NotFound)
        };
    }

    async fn get_faults_by_public_key(
        &self,
        public_key: &str,
    ) -> Result<Vec<SseEnvelope<Fault>>, DatabaseReadError> {
        let data = self.data.lock().expect("Error acquiring lock on data");

        return if let Some(event) = data.get(public_key) {
            let fault =
                serde_json::from_str::<Fault>(event).map_err(DatabaseReadError::Serialisation)?;
            Ok(vec![SseEnvelope::new(
                fault,
                "2.0.0".to_string(),
                "network-1".to_string(),
            )])
        } else {
            Err(DatabaseReadError::NotFound)
        };
    }

    async fn get_faults_by_era(
        &self,
        era: u64,
    ) -> Result<Vec<SseEnvelope<Fault>>, DatabaseReadError> {
        let data = self.data.lock().expect("Error acquiring lock on data");

        return if let Some(event) = data.get(&era.to_string()) {
            let fault =
                serde_json::from_str::<Fault>(event).map_err(DatabaseReadError::Serialisation)?;
            Ok(vec![SseEnvelope::new(
                fault,
                "2.0.0".to_string(),
                "network-1".to_string(),
            )])
        } else {
            Err(DatabaseReadError::NotFound)
        };
    }

    async fn get_finality_signatures_by_block(
        &self,
        block_hash: &str,
    ) -> Result<Vec<SseEnvelope<FinSig>>, DatabaseReadError> {
        let data = self.data.lock().expect("Error acquiring lock on data");

        return if let Some(event) = data.get(block_hash) {
            let finality_signature =
                serde_json::from_str::<FinSig>(event).map_err(DatabaseReadError::Serialisation)?;
            Ok(vec![SseEnvelope::new(
                finality_signature,
                "2.0.0".to_string(),
                "network-1".to_string(),
            )])
        } else {
            Err(DatabaseReadError::NotFound)
        };
    }

    async fn get_step_by_era(&self, era: u64) -> Result<SseEnvelope<Step>, DatabaseReadError> {
        let data = self.data.lock().expect("Error acquiring lock on data");

        return if let Some(event) = data.get(&era.to_string()) {
            let entity =
                serde_json::from_str::<Step>(event).map_err(DatabaseReadError::Serialisation)?;
            Ok(SseEnvelope::new(
                entity,
                "2.0.0".to_string(),
                "network-1".to_string(),
            ))
        } else {
            Err(DatabaseReadError::NotFound)
        };
    }

    async fn get_number_of_events(&self) -> Result<u64, DatabaseReadError> {
        Ok(0)
    }

    async fn get_newest_migration_version(&self) -> Result<Option<(u32, bool)>, DatabaseReadError> {
        Ok(None)
    }
}

pub struct IdentifiersForStoredEvents {
    pub block_added_hash: String,
    pub block_added_height: u64,
    pub transaction_accepted_info: (String, TransactionTypeId),
    pub transaction_processed_info: (String, TransactionTypeId),
    pub transaction_expired_info: (String, TransactionTypeId),
    pub fault_public_key: String,
    pub fault_era_id: u64,
    pub finality_signatures_block_hash: String,
    pub step_era_id: u64,
}
