use crate::{
    types::database::DatabaseWriteError, FinalitySignature, Step, TransactionAccepted,
    TransactionProcessed,
};
use async_trait::async_trait;
use casper_event_listener::SseEvent;
use casper_event_types::{sse_data::SseData, Filter};
use casper_types::{
    Block, BlockHash, EraId, ProtocolVersion, PublicKey, Timestamp, TransactionHash,
};
use metrics::observe_error;
use tokio::sync::mpsc::Sender;
use tracing::{debug, trace, warn};
pub mod db_saving_event_handling_service;
pub mod no_db_event_handling_service;
pub use {
    db_saving_event_handling_service::DbSavingEventHandlingService,
    no_db_event_handling_service::NoDbEventHandlingService,
};

#[async_trait]
pub trait EventHandlingService {
    async fn handle_api_version(&self, version: ProtocolVersion, filter: Filter);

    async fn handle_block_added(
        &self,
        block_hash: BlockHash,
        block: Box<Block>,
        sse_event: SseEvent,
    );

    async fn handle_transaction_accepted(
        &self,
        transaction_accepted: TransactionAccepted,
        sse_event: SseEvent,
    );

    async fn handle_transaction_expired(
        &self,
        transaction_hash: TransactionHash,
        sse_event: SseEvent,
    );

    async fn handle_transaction_processed(
        &self,
        transaction_processed: TransactionProcessed,
        sse_event: SseEvent,
    );

    async fn handle_fault(
        &self,
        era_id: EraId,
        timestamp: Timestamp,
        public_key: PublicKey,
        sse_event: SseEvent,
    );

    async fn handle_step(&self, step: Step, sse_event: SseEvent);

    async fn handle_finality_signature(
        &self,
        finality_signature: FinalitySignature,
        sse_event: SseEvent,
    );

    async fn handle_shutdown(&self, sse_event: SseEvent);
}

async fn handle_database_save_result<T>(
    entity_name: &str,
    entity_identifier: &str,
    res: Result<T, DatabaseWriteError>,
    outbound_sse_data_sender: &Sender<(SseData, Option<Filter>)>,
    inbound_filter: Filter,
    sse_data: SseData,
) {
    match res {
        Ok(_) => {
            if let Err(error) = outbound_sse_data_sender
                .send((sse_data, Some(inbound_filter)))
                .await
            {
                debug!(
                    "Error when sending to outbound_sse_data_sender. Error: {}",
                    error
                );
            }
        }
        Err(DatabaseWriteError::UniqueConstraint(uc_err)) => {
            debug!(
                "Already received {} ({}), logged in event_log",
                entity_name, entity_identifier,
            );
            trace!(?uc_err);
        }
        Err(other_err) => {
            count_error(format!("db_save_error_{}", entity_name).as_str());
            warn!(?other_err, "Unexpected error saving {}", entity_identifier);
        }
    }
}

fn count_error(reason: &str) {
    observe_error("event_handling", reason);
}
