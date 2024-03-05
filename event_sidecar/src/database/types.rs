use serde::{Deserialize, Serialize};

/// This struct holds flags that steer DDL generation for specific databases.
pub struct DDLConfiguration {
    /// Postgresql doesn't support unsigned integers, so for some fields we need to be mindful of the fact that in postgres we might need to use a bigger type to accomodate scope of field
    pub db_supports_unsigned: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SseEnvelopeHeader {
    api_version: String,
    network_name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SseEnvelope<T> {
    header: SseEnvelopeHeader,
    payload: T,
}

impl<T> SseEnvelope<T> {
    pub fn new(sse_event: T, api_version: String, network_name: String) -> SseEnvelope<T> {
        SseEnvelope {
            header: SseEnvelopeHeader {
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
