use crate::{
    event_handling_service::handle_database_save_result, transaction_hash_to_identifier, Fault,
    FinalitySignature, Step, TransactionAccepted, TransactionProcessed,
};
use async_trait::async_trait;
use casper_event_listener::SseEvent;
use casper_event_types::{sse_data::SseData, Filter};
use casper_types::{
    Block, BlockHash, EraId, ProtocolVersion, PublicKey, Timestamp, TransactionHash,
};
use derive_new::new;
use hex_fmt::HexFmt;
use metrics::sse::observe_contract_messages;
use tokio::sync::mpsc::Sender;
use tracing::{debug, info, warn};

use super::EventHandlingService;

#[derive(new, Clone)]
pub struct NoDbEventHandlingService {
    outbound_sse_data_sender: Sender<(SseData, Option<Filter>)>,
    enable_event_logging: bool,
}

#[async_trait]
impl EventHandlingService for NoDbEventHandlingService {
    async fn handle_api_version(&self, version: ProtocolVersion, filter: Filter) {
        if let Err(error) = self
            .outbound_sse_data_sender
            .send((SseData::ApiVersion(version), Some(filter)))
            .await
        {
            debug!(
                "Error when sending to outbound_sse_data_sender. Error: {}",
                error
            );
        }
        if self.enable_event_logging {
            info!(%version, "API Version");
        }
    }

    async fn handle_block_added(
        &self,
        block_hash: BlockHash,
        _block: Box<Block>,
        sse_event: SseEvent,
    ) {
        if self.enable_event_logging {
            let hex_block_hash = HexFmt(block_hash.inner());
            info!("Block Added: {:18}", hex_block_hash);
            debug!("Block Added: {}", hex_block_hash);
        }
        let filter = sse_event.inbound_filter;
        handle_database_save_result(
            "BlockAdded",
            HexFmt(block_hash.inner()).to_string().as_str(),
            Ok(()),
            &self.outbound_sse_data_sender,
            filter,
            sse_event.data,
        )
        .await;
    }

    async fn handle_transaction_accepted(
        &self,
        transaction_accepted: TransactionAccepted,
        sse_event: SseEvent,
    ) {
        let entity_identifier = transaction_accepted.identifier();
        if self.enable_event_logging {
            info!("Transaction Accepted: {:18}", entity_identifier);
            debug!("Transaction Accepted: {}", entity_identifier);
        }
        let filter = sse_event.inbound_filter;
        handle_database_save_result(
            "TransactionAccepted",
            &entity_identifier,
            Ok(()),
            &self.outbound_sse_data_sender,
            filter,
            sse_event.data,
        )
        .await;
    }

    async fn handle_transaction_expired(
        &self,
        transaction_hash: TransactionHash,
        sse_event: SseEvent,
    ) {
        let entity_identifier = transaction_hash_to_identifier(&transaction_hash);
        if self.enable_event_logging {
            info!("Transaction Expired: {:18}", entity_identifier);
            debug!("Transaction Expired: {}", entity_identifier);
        }
        let filter = sse_event.inbound_filter;
        handle_database_save_result(
            "TransactionExpired",
            &entity_identifier,
            Ok(()),
            &self.outbound_sse_data_sender,
            filter,
            sse_event.data,
        )
        .await;
    }

    async fn handle_transaction_processed(
        &self,
        transaction_processed: TransactionProcessed,
        sse_event: SseEvent,
    ) {
        let entity_identifier = transaction_processed.identifier();
        if self.enable_event_logging {
            info!("Transaction Processed: {:18}", entity_identifier);
            debug!("Transaction Processed: {}", entity_identifier);
        }
        let filter = sse_event.inbound_filter;
        let messages_len = transaction_processed.messages().len();

        if messages_len > 0 {
            observe_contract_messages("all", messages_len);
        }
        handle_database_save_result(
            "TransactionProcessed",
            &entity_identifier,
            Ok(()),
            &self.outbound_sse_data_sender,
            filter,
            sse_event.data,
        )
        .await;
    }

    async fn handle_fault(
        &self,
        era_id: EraId,
        timestamp: Timestamp,
        public_key: PublicKey,
        sse_event: SseEvent,
    ) {
        let filter = sse_event.inbound_filter;
        let fault_identifier = format!("{}-{}", era_id.value(), public_key);
        let fault = Fault::new(era_id, public_key, timestamp);
        warn!(%fault, "Fault reported");

        handle_database_save_result(
            "Fault",
            &fault_identifier,
            Ok(()),
            &self.outbound_sse_data_sender,
            filter,
            sse_event.data,
        )
        .await;
    }

    async fn handle_step(&self, step: Step, sse_event: SseEvent) {
        let era_id = step.era_id;
        let step_identifier = format!("{}", era_id.value());
        if self.enable_event_logging {
            info!("Step at era: {}", step_identifier);
        }
        let filter = sse_event.inbound_filter;
        handle_database_save_result(
            "Step",
            step_identifier.as_str(),
            Ok(()),
            &self.outbound_sse_data_sender,
            filter,
            sse_event.data,
        )
        .await;
    }

    async fn handle_finality_signature(
        &self,
        finality_signature: FinalitySignature,
        sse_event: SseEvent,
    ) {
        if self.enable_event_logging {
            debug!(
                "Finality Signature: {} for {}",
                finality_signature.signature(),
                finality_signature.block_hash()
            );
        }
        let filter = sse_event.inbound_filter;
        handle_database_save_result(
            "FinalitySignature",
            "",
            Ok(()),
            &self.outbound_sse_data_sender,
            filter,
            sse_event.data,
        )
        .await;
    }

    async fn handle_shutdown(&self, sse_event: SseEvent) {
        warn!("Node ({}) is unavailable", sse_event.source.to_string());
        if let Err(error) = self
            .outbound_sse_data_sender
            .send((SseData::Shutdown, Some(sse_event.inbound_filter)))
            .await
        {
            debug!(
                "Error when sending to outbound_sse_data_sender. Error: {}",
                error
            );
        }
    }
}
