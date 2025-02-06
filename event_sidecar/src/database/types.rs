use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{
    BlockAdded, Fault, FinalitySignature, Step, TransactionAccepted, TransactionExpired,
    TransactionProcessed,
};

/// This struct holds flags that steer DDL generation for specific databases.
pub struct DDLConfiguration {
    /// Postgresql doesn't support unsigned integers, so for some fields we need to be mindful of the fact that in postgres we might need to use a bigger type to accomodate scope of field
    pub db_supports_unsigned: bool,
}

/// Header of the SSE envelope
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct EnvelopeHeader {
    api_version: String,
    network_name: String,
}

/// Wrapper envelope for SSE events. It contains the event in `payload` field and some metadata in `header` field.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
#[aliases(
    BlockAddedEnveloped = SseEnvelope<BlockAdded>,
    TransactionAcceptedEnveloped = SseEnvelope<TransactionAccepted>,
    TransactionExpiredEnveloped = SseEnvelope<TransactionExpired>,
    TransactionProcessedEnveloped = SseEnvelope<TransactionProcessed>,
    FaultEnveloped = SseEnvelope<Fault>,
    FinalitySignatureEnveloped = SseEnvelope<FinalitySignature>,
    StepEnveloped = SseEnvelope<Step>,
)]
pub struct SseEnvelope<T> {
    header: EnvelopeHeader,
    payload: T,
}

impl<T> SseEnvelope<T> {
    pub fn new(sse_event: T, api_version: String, network_name: String) -> SseEnvelope<T> {
        SseEnvelope {
            header: EnvelopeHeader {
                api_version,
                network_name,
            },
            payload: sse_event,
        }
    }

    pub fn payload(&self) -> &T {
        &self.payload
    }

    pub fn api_version(&self) -> &String {
        &self.header.api_version
    }

    pub fn network_name(&self) -> &String {
        &self.header.network_name
    }
}
