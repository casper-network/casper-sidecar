use std::sync::Arc;

use async_trait::async_trait;
use casper_json_rpc::Error as RpcError;

use super::{
    super::{NodeClient, RpcWithoutParams},
    config::read_evm_config,
    eth_u256::EthU256,
};

/// `eth_gasPrice`.
pub struct GasPrice;

#[async_trait]
impl RpcWithoutParams for GasPrice {
    const METHOD: &'static str = "eth_gasPrice";
    type ResponseResult = EthU256;

    async fn do_handle_request(node_client: Arc<dyn NodeClient>) -> Result<EthU256, RpcError> {
        let evm_config = read_evm_config(node_client.as_ref()).await?;
        Ok(EthU256::from(evm_config.base_fee_wei()))
    }
}

#[cfg(test)]
mod tests {
    use casper_binary_port::{BinaryResponse, Command, InformationRequest};
    use casper_types::ChainspecRawBytes;
    use serde_json::json;

    use super::*;
    use crate::rpcs::test_utils::BinaryPortMock;

    #[tokio::test]
    async fn returns_chainspec_base_fee_in_wei() {
        let client = BinaryPortMock::new();
        let chainspec = ChainspecRawBytes::new(
            br#"
[evm]
enabled = true
chain_id = 7
spec = "prague"
block_gas_limit = 30000000
base_fee = 5000
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

        let result = GasPrice::do_handle_request(Arc::new(client))
            .await
            .expect("gas price lookup should succeed");

        assert_eq!(result, EthU256::from(5_000_000_000_000u64));
        assert_eq!(
            serde_json::to_value(result).unwrap(),
            json!("0x48c27395000")
        );
        assert_eq!(
            21_000u128 * 5_000_000_000_000u128,
            105_000_000_000_000_000u128
        );
    }
}
