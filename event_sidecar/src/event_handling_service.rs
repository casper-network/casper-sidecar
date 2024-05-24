use async_trait::async_trait;
use casper_event_listener::SseEvent;
use casper_event_types::{sse_data::SseData, Filter};
use casper_types::{Block, BlockHash, ProtocolVersion};
use derive_new::new;
use hex_fmt::HexFmt;
use metrics::{observe_error, sse::observe_contract_messages};
use tokio::sync::mpsc::{channel as mpsc_channel, Receiver, Sender};
use tracing::{debug, trace, warn};

use crate::{
    database::{postgresql_database::PostgreSqlDatabase, sqlite_database::SqliteDatabase},
    types::database::{DatabaseReader, DatabaseWriteError, DatabaseWriter},
    BlockAdded, Fault, FinalitySignature, Step, TransactionAccepted, TransactionExpired,
    TransactionProcessed,
};

enum EventHandlingServiceWrapper {
    PostgresEventHandlingService(DbSavingEventHandlingService<PostgreSqlDatabase>),
    SqliteEventHandlingService(DbSavingEventHandlingService<SqliteDatabase>),
}

#[async_trait]
pub trait EventHandlingService {
    async fn handle_api_version(&self, version: ProtocolVersion, filter: Filter);

    async fn handle_block_added(
        &self,
        block_hash: BlockHash,
        block: Box<Block>,
        id: u32,
        source: String,
        api_version: String,
        network_name: String,
        filter: Filter,
    );

    async fn handle_transaction_accepted(
        &self,
        transaction_accepted: TransactionAccepted,
        id: u32,
        source: String,
        api_version: String,
        network_name: String,
        filter: Filter,
    );

    async fn handle_transaction_expired(
        &self,
        transaction_accepted: TransactionExpired,
        id: u32,
        source: String,
        api_version: String,
        network_name: String,
        filter: Filter,
    );

    async fn handle_transaction_processed(
        &self,
        transaction_processed: TransactionProcessed,
        id: u32,
        source: String,
        api_version: String,
        network_name: String,
        filter: Filter,
    );

    async fn handle_fault(
        &self,
        fault: Fault,
        id: u32,
        source: String,
        api_version: String,
        network_name: String,
        filter: Filter,
    );

    async fn handle_step(
        &self,
        step: Step,
        id: u32,
        source: String,
        api_version: String,
        network_name: String,
        filter: Filter,
    );

    async fn handle_finality_signature(
        &self,
        fs: FinalitySignature,
        id: u32,
        source: String,
        api_version: String,
        network_name: String,
        filter: Filter,
    );

    async fn handle_shutdown(&self, sse_event: SseEvent);
}

#[derive(new, Clone)]
pub struct DbSavingEventHandlingService<Db: DatabaseReader + DatabaseWriter + Clone + Send + Sync> {
    outbound_sse_data_sender: Sender<(SseData, Option<Filter>)>,
    database: Db,
}

#[async_trait]
impl<Db> EventHandlingService for DbSavingEventHandlingService<Db>
where
    Db: DatabaseReader + DatabaseWriter + Clone + Send + Sync + 'static,
{
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
    }

    async fn handle_block_added(
        &self,
        block_hash: BlockHash,
        block: Box<Block>,
        id: u32,
        source: String,
        api_version: String,
        network_name: String,
        filter: Filter,
    ) {
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
        handle_database_save_result(
            "BlockAdded",
            HexFmt(block_hash.inner()).to_string().as_str(),
            res,
            &self.outbound_sse_data_sender,
            filter,
            || SseData::BlockAdded { block, block_hash },
        )
        .await;
    }

    async fn handle_transaction_accepted(
        &self,
        transaction_accepted: TransactionAccepted,
        id: u32,
        source: String,
        api_version: String,
        network_name: String,
        filter: Filter,
    ) {
        let entity_identifier = transaction_accepted.identifier();
        let transaction = transaction_accepted.transaction();
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
            || SseData::TransactionAccepted(transaction),
        )
        .await;
    }

    async fn handle_transaction_expired(
        &self,
        transaction_expired: TransactionExpired,
        id: u32,
        source: String,
        api_version: String,
        network_name: String,
        filter: Filter,
    ) {
        let transaction_hash = transaction_expired.transaction_hash();
        let entity_identifier = transaction_expired.identifier();
        let res = self
            .database
            .save_transaction_expired(
                transaction_expired,
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
            || SseData::TransactionExpired { transaction_hash },
        )
        .await;
    }

    async fn handle_transaction_processed(
        &self,
        transaction_processed: TransactionProcessed,
        id: u32,
        source: String,
        api_version: String,
        network_name: String,
        filter: Filter,
    ) {
        let messages = transaction_processed.messages();
        let entity_identifier = transaction_processed.identifier();
        if !messages.is_empty() {
            observe_contract_messages("all", messages.len());
        }
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
        if res.is_ok() && !messages.is_empty() {
            observe_contract_messages("unique", messages.len());
        }
        handle_database_save_result(
            "TransactionProcessed",
            &entity_identifier,
            res,
            &self.outbound_sse_data_sender,
            filter,
            || SseData::TransactionProcessed {
                transaction_hash,
                initiator_addr,
                timestamp,
                ttl,
                block_hash,
                execution_result,
                messages,
            },
        )
        .await;
    }

    async fn handle_fault(
        &self,
        fault: Fault,
        id: u32,
        source: String,
        api_version: String,
        network_name: String,
        filter: Filter,
    ) {
        let res = self
            .database
            .save_fault(fault.clone(), id, source, api_version, network_name)
            .await;

        handle_database_save_result(
            "Fault",
            format!("{:#?}", fault).as_str(),
            res,
            &self.outbound_sse_data_sender,
            filter,
            || SseData::Fault {
                era_id,
                timestamp,
                public_key,
            },
        )
        .await;
    }

    async fn handle_step(
        &self,
        step: Step,
        id: u32,
        source: String,
        api_version: String,
        network_name: String,
        filter: Filter,
    ) {
        let res = self
            .database
            .save_step(step, id, source, api_version, network_name)
            .await;
        handle_database_save_result(
            "Step",
            format!("{}", step.era_id.value()).as_str(),
            res,
            &self.outbound_sse_data_sender,
            filter,
            || SseData::Step {
                era_id,
                execution_effects,
            },
        )
        .await;
    }

    async fn handle_finality_signature(
        &self,
        finality_signature: FinalitySignature,
        id: u32,
        source: String,
        api_version: String,
        network_name: String,
        filter: Filter,
    ) {
        let res = self
            .database
            .save_finality_signature(
                finality_signature.clone(),
                id,
                source.to_string(),
                api_version,
                network_name,
            )
            .await;
        handle_database_save_result(
            "FinalitySignature",
            "",
            res,
            &self.outbound_sse_data_sender,
            filter,
            || SseData::FinalitySignature(fs),
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
                if let Err(error) = outbound_sse_data_sender
                    .send((SseData::Shutdown, Some(sse_event.inbound_filter)))
                    .await
                {
                    debug!(
                        "Error when sending to outbound_sse_data_sender. Error: {}",
                        error
                    );
                }
            }
            Err(other_err) => {
                count_error("db_save_error_shutdown");
                warn!(?other_err, "Unexpected error saving Shutdown")
            }
        }
    }
}

async fn handle_database_save_result<F>(
    entity_name: &str,
    entity_identifier: &str,
    res: Result<u64, DatabaseWriteError>,
    outbound_sse_data_sender: &Sender<(SseData, Option<Filter>)>,
    inbound_filter: Filter,
    build_sse_data: F,
) where
    F: FnOnce() -> SseData,
{
    match res {
        Ok(_) => {
            if let Err(error) = outbound_sse_data_sender
                .send((build_sse_data(), Some(inbound_filter)))
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
