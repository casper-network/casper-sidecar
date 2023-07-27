use casper_types::ProtocolVersion;
use std::sync::Arc;
use tokio::sync::Mutex;
pub type GuardedApiVersionManager = Arc<Mutex<ApiVersionManager>>;

/// A structure to keep track what is the newest ApiVersion observed over all nodes sidecar is/was connected to.
pub struct ApiVersionManager {
    newest_protocol_version: Option<ProtocolVersion>,
}

impl ApiVersionManager {
    pub fn new() -> GuardedApiVersionManager {
        Arc::new(Mutex::new(ApiVersionManager::new_inner()))
    }

    fn new_inner() -> ApiVersionManager {
        ApiVersionManager {
            newest_protocol_version: None,
        }
    }

    /// Stores protocol version. If the stored `protocol_version` is higher than the highest observed protocol version - returns true. Otherwise returns false.
    pub fn store_version(&mut self, protocol_version: ProtocolVersion) -> bool {
        let mut did_change_latest_stored_version = false;
        if let Some(current) = self.newest_protocol_version {
            if protocol_version.gt(&current) {
                self.newest_protocol_version = Some(protocol_version);
                did_change_latest_stored_version = true
            }
        } else {
            self.newest_protocol_version = Some(protocol_version);
            did_change_latest_stored_version = true;
        }
        did_change_latest_stored_version
    }
}

#[cfg(test)]
mod tests {
    use crate::api_version_manager::ApiVersionManager;
    use casper_types::ProtocolVersion;

    #[tokio::test]
    async fn api_version_manager_should_store_newest() {
        let mut api_version_manager = ApiVersionManager::new_inner();
        let protocol_version_1 = ProtocolVersion::from_parts(1, 0, 0);
        let protocol_version_2 = ProtocolVersion::from_parts(1, 1, 0);
        assert!(api_version_manager.store_version(protocol_version_1));
        assert!(!api_version_manager.store_version(protocol_version_1));
        assert!(api_version_manager.store_version(protocol_version_2));
    }
}
