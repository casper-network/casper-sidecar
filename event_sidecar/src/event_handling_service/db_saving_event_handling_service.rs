use std::sync::Arc;

use crate::{
    BlockAdded, Fault, FinalitySignature, Step, TransactionAccepted, TransactionExpired,
    TransactionProcessed,
    event_handling_service::count_error,
    transaction_hash_to_identifier,
    types::database::{DatabaseReader, DatabaseWriteError, DatabaseWriter},
};
use async_trait::async_trait;
use casper_event_listener::SseEvent;
use casper_event_types::{Filter, SidecarEvent, sse_data::SseData};
use casper_types::{
    Block, BlockHash, EraId, ProtocolVersion, PublicKey, Timestamp, TransactionHash,
};
use derive_new::new;
use hex_fmt::HexFmt;
use metrics::sse::observe_contract_messages;
use tokio::sync::broadcast::Sender as BroadcastSender;
use tokio::sync::mpsc::Sender;
use tracing::{debug, info, warn};

use super::{EventHandlingService, handle_database_save_result};

#[derive(new, Clone)]
pub struct DbSavingEventHandlingService<Db: DatabaseReader + DatabaseWriter + Clone + Send + Sync> {
    outbound_sse_data_sender: Sender<(SseData, Option<Filter>)>,
    database: Db,
    enable_event_logging: bool,
    sidecar_event_sender: Option<BroadcastSender<SidecarEvent>>,
}

#[async_trait]
impl<Db> EventHandlingService for DbSavingEventHandlingService<Db>
where
    Db: DatabaseReader + DatabaseWriter + Clone + Send + Sync + 'static,
{
    async fn handle_api_version(&self, version: ProtocolVersion, filter: Filter) {
        if let Some(sender) = self.sidecar_event_sender.as_ref() {
            // `send` will return error if there is no receiving party. But we treat this
            // Sender as an event bus, so having no receiver is normal and we should muffle
            // the error since there's really nothing to do in that case
            let _ = sender.send(SidecarEvent::ApiVersion(version));
        }
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
        block: Arc<Block>,
        sse_event: SseEvent,
    ) {
        if self.enable_event_logging {
            let hex_block_hash = HexFmt(block_hash.inner());
            info!("Block Added: {hex_block_hash:18}");
            debug!("Block Added: {hex_block_hash}");
        }
        let id = sse_event.id;
        let source = sse_event.source.to_string();
        let api_version = sse_event.api_version;
        let network_name = sse_event.network_name;
        let filter = sse_event.inbound_filter;
        let res = self
            .database
            .save_block_added(
                BlockAdded::new(block_hash, block.clone()),
                id,
                source,
                api_version,
                network_name,
            )
            .await;
        if let Some(sender) = self.sidecar_event_sender.as_ref() {
            // `send` will return error if there is no receiving party. But we treat this
            // Sender as an event bus, so having no receiver is normal and we should muffle
            // the error since there's really nothing to do in that case.
            let _ = sender.send(SidecarEvent::BlockAdded {
                block: block.clone(),
            });
        }
        handle_database_save_result(
            "BlockAdded",
            HexFmt(block_hash.inner()).to_string().as_str(),
            res,
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
        let id = sse_event.id;
        let source = sse_event.source.to_string();
        let api_version = sse_event.api_version;
        let network_name = sse_event.network_name;
        let filter = sse_event.inbound_filter;
        let res = self
            .database
            .save_transaction_accepted(transaction_accepted, id, source, api_version, network_name)
            .await;
        handle_database_save_result(
            "TransactionAccepted",
            &entity_identifier,
            res,
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
        let id = sse_event.id;
        let source = sse_event.source.to_string();
        let api_version = sse_event.api_version;
        let network_name = sse_event.network_name;
        let filter = sse_event.inbound_filter;
        let res = self
            .database
            .save_transaction_expired(
                TransactionExpired::new(transaction_hash),
                id,
                source.to_string(),
                api_version,
                network_name,
            )
            .await;
        handle_database_save_result(
            "TransactionExpired",
            &entity_identifier,
            res,
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
        let id = sse_event.id;
        let source = sse_event.source.to_string();
        let api_version = sse_event.api_version;
        let network_name = sse_event.network_name;
        let filter = sse_event.inbound_filter;
        let messages_len = transaction_processed.messages().len();

        if messages_len > 0 {
            observe_contract_messages("all", messages_len);
        }
        let transaction_hash = *transaction_processed.transaction_hash();
        let block_hash = *transaction_processed.block_hash();
        let execution_result = Arc::new(transaction_processed.execution_result().clone());
        let res = self
            .database
            .save_transaction_processed(
                transaction_processed,
                id,
                source.to_string(),
                api_version,
                network_name,
            )
            .await;
        if res.is_ok() && messages_len > 0 {
            observe_contract_messages("unique", messages_len);
        }
        if let Some(sender) = self.sidecar_event_sender.as_ref() {
            // `send` will return error if there is no receiving party. But we treat this
            // Sender as an event bus, so having no receiver is normal and we should muffle
            // the error since there's really nothing to do in that case.
            let _ = sender.send(SidecarEvent::TransactionProcessed {
                transaction_hash,
                block_hash,
                execution_result,
            });
        }
        handle_database_save_result(
            "TransactionProcessed",
            &entity_identifier,
            res,
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
        let id = sse_event.id;
        let source = sse_event.source.to_string();
        let api_version = sse_event.api_version;
        let network_name = sse_event.network_name;
        let filter = sse_event.inbound_filter;
        let fault_identifier = format!("{}-{}", era_id.value(), public_key);
        let fault = Fault::new(era_id, public_key, timestamp);
        warn!(%fault, "Fault reported");
        let res = self
            .database
            .save_fault(fault, id, source, api_version, network_name)
            .await;

        handle_database_save_result(
            "Fault",
            &fault_identifier,
            res,
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

        let id = sse_event.id;
        let source = sse_event.source.to_string();
        let api_version = sse_event.api_version;
        let network_name = sse_event.network_name;
        let filter = sse_event.inbound_filter;
        let res = self
            .database
            .save_step(step, id, source, api_version, network_name)
            .await;
        handle_database_save_result(
            "Step",
            step_identifier.as_str(),
            res,
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
        let id = sse_event.id;
        let source = sse_event.source.to_string();
        let api_version = sse_event.api_version;
        let network_name = sse_event.network_name;
        let filter = sse_event.inbound_filter;
        let res = self
            .database
            .save_finality_signature(
                finality_signature.clone(),
                id,
                source,
                api_version,
                network_name,
            )
            .await;
        if let Some(sender) = self.sidecar_event_sender.as_ref() {
            // `send` will return error if there is no receiving party. But we treat this
            // Sender as an event bus, so having no receiver is normal and we should muffle
            // the error since there's really nothing to do in that case.
            let _ = sender.send(SidecarEvent::FinalitySignature(Box::new(
                finality_signature.inner(),
            )));
        }
        handle_database_save_result(
            "FinalitySignature",
            "",
            res,
            &self.outbound_sse_data_sender,
            filter,
            sse_event.data,
        )
        .await;
    }

    async fn handle_shutdown(&self, sse_event: SseEvent) {
        warn!("Node ({}) is unavailable", sse_event.source.to_string());
        let res = self
            .database
            .save_shutdown(
                sse_event.id,
                sse_event.source.to_string(),
                sse_event.api_version,
                sse_event.network_name,
            )
            .await;
        match res {
            Ok(_) | Err(DatabaseWriteError::UniqueConstraint(_)) => {
                // We push to outbound on UniqueConstraint error because in sse_server we match shutdowns to outbounds based on the filter they came from to prevent duplicates.
                // But that also means that we need to pass through all the Shutdown events so the sse_server can determine to which outbound filters they need to be pushed (we
                // don't store in DB the information from which filter did shutdown came).
                if let Err(error) = self
                    .outbound_sse_data_sender
                    .send((SseData::Shutdown, Some(sse_event.inbound_filter)))
                    .await
                {
                    debug!("Error when sending to outbound_sse_data_sender. Error: {error}");
                }
            }
            Err(other_err) => {
                count_error("db_save_error_shutdown");
                warn!(?other_err, "Unexpected error saving Shutdown");
            }
        }
    }
}
