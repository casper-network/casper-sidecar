use std::sync::{Arc, LazyLock};

use async_trait::async_trait;
use casper_json_rpc::Error as RpcError;

use super::{
    super::{NodeClient, RpcWithoutParams},
    config::read_evm_config,
};
use crate::rpcs::docs::DocExample;

static NET_VERSION_EXAMPLE: LazyLock<String> = LazyLock::new(|| "1".to_string());

impl DocExample for String {
    fn doc_example() -> &'static Self {
        &NET_VERSION_EXAMPLE
    }
}

/// `net_version`.
pub struct NetVersion;

#[async_trait]
impl RpcWithoutParams for NetVersion {
    const METHOD: &'static str = "net_version";
    type ResponseResult = String;

    async fn do_handle_request(node_client: Arc<dyn NodeClient>) -> Result<String, RpcError> {
        Ok(read_evm_config(node_client.as_ref())
            .await?
            .chain_id
            .to_string())
    }
}

#[cfg(test)]
mod tests {
    use casper_binary_port::{BinaryResponse, Command, InformationRequest};
    use casper_types::ChainspecRawBytes;

    use super::*;
    use crate::rpcs::test_utils::BinaryPortMock;

    #[tokio::test]
    async fn returns_chain_id_as_a_decimal_string() {
        let client = BinaryPortMock::new();
        let chainspec = ChainspecRawBytes::new(
            br#"
[evm]
enabled = true
chain_id = 1129533695
spec = "prague"
block_gas_limit = 30000000
base_fee = 1
wei_per_mote = 1000000000
transaction_lanes = [[100, 131072, 1024, 1000000, 50]]
"#
            .to_vec()
            .into(),
            None,
            None,
        );
        let request = InformationRequest::ChainspecRawBytes
            .try_into()
            .expect("chainspec information request should convert");
        client
            .when_then(Command::Get(request), BinaryResponse::from_value(chainspec))
            .await;

        let result = NetVersion::do_handle_request(Arc::new(client))
            .await
            .expect("network version lookup should succeed");

        assert_eq!(result, "1129533695");
    }
}
