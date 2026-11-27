use std::sync::{Arc, LazyLock};

use async_trait::async_trait;
use casper_json_rpc::{Error as RpcError, Params};
use casper_types::{EvmTransaction, TimeDiff, Timestamp, Transaction, evm};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{
    super::{NodeClient, RpcWithParams},
    types::{EthApiErrorCode, HexData, internal_error, invalid_params, parse_positional_params},
};
use crate::{ClientError, rpcs::docs::DocExample};

const DEFAULT_EVM_TX_TTL: TimeDiff = TimeDiff::from_seconds(300);

static SEND_RAW_TRANSACTION_PARAMS_EXAMPLE: LazyLock<SendRawTransactionParams> =
    LazyLock::new(|| SendRawTransactionParams {
        raw_transaction: HexData::from(Vec::new()),
    });

/// Params for `eth_sendRawTransaction`.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct SendRawTransactionParams {
    raw_transaction: HexData,
}

impl SendRawTransactionParams {
    fn raw_transaction(self) -> Vec<u8> {
        self.raw_transaction.into_bytes()
    }
}

impl DocExample for SendRawTransactionParams {
    fn doc_example() -> &'static Self {
        &SEND_RAW_TRANSACTION_PARAMS_EXAMPLE
    }
}

/// `eth_sendRawTransaction`.
pub struct SendRawTransaction;

#[async_trait]
impl RpcWithParams for SendRawTransaction {
    const METHOD: &'static str = "eth_sendRawTransaction";
    type RequestParams = SendRawTransactionParams;
    type ResponseResult = evm::Hash;

    fn try_parse_params(maybe_params: Option<Params>) -> Result<Self::RequestParams, RpcError> {
        let (raw_transaction,) = parse_positional_params::<(HexData,)>(maybe_params)?;
        Ok(SendRawTransactionParams { raw_transaction })
    }

    async fn do_handle_request(
        node_client: Arc<dyn NodeClient>,
        params: SendRawTransactionParams,
    ) -> Result<evm::Hash, RpcError> {
        let evm_transaction = EvmTransaction::from_signed_rlp(
            params.raw_transaction(),
            Timestamp::now(),
            DEFAULT_EVM_TX_TTL,
        )
        .map_err(|error| invalid_params(format!("invalid EVM transaction: {error}")))?;
        let hash = evm_transaction.hash();
        node_client
            .try_accept_transaction(Transaction::from(evm_transaction))
            .await
            .map_err(|error| match error {
                ClientError::InvalidTransaction(reason) => {
                    RpcError::new(EthApiErrorCode::TransactionRejected, reason.to_string())
                }
                other => internal_error(other),
            })?;
        Ok(hash.hash())
    }
}

#[cfg(test)]
mod tests {
    use alloy_consensus::{SignableTransaction, TxEip7702, TxEnvelope, crypto::secp256k1};
    use alloy_eips::{
        eip2718::Encodable2718,
        eip2930::{AccessList, AccessListItem},
        eip7702::Authorization,
    };
    use alloy_primitives::{Address as AlloyAddress, B256, Bytes as AlloyBytes, U256 as AlloyU256};
    use casper_binary_port::{BinaryResponse, BinaryResponseAndRequest, Command, ErrorCode};
    use casper_types::{
        EvmTransactionKind,
        bytesrepr::{self, Bytes, ToBytes},
    };
    use serde_json::json;
    use tokio::sync::Mutex;

    use super::super::transaction_response::{
        TransactionLocation, project_transaction, tests::fixture_transactions_with_access_list,
    };
    use super::*;

    const SIGNING_SECRET: [u8; 32] = [7; 32];
    const AUTHORIZATION_SECRET: [u8; 32] = [8; 32];

    #[derive(Default)]
    struct ClientMock {
        accepted: Mutex<Option<Transaction>>,
        error_code: Option<ErrorCode>,
    }

    #[async_trait]
    impl NodeClient for ClientMock {
        async fn send_request(
            &self,
            req: Command,
        ) -> Result<BinaryResponseAndRequest, ClientError> {
            let Command::TryAcceptTransaction { transaction } = req else {
                panic!("unexpected request: {req:?}");
            };
            *self.accepted.lock().await = Some(transaction);
            let response = self
                .error_code
                .map_or_else(BinaryResponse::new_empty, BinaryResponse::new_error);
            Ok(BinaryResponseAndRequest::new(response, Bytes::new()))
        }
    }

    #[tokio::test]
    async fn reports_node_transaction_rejections_without_masking_the_reason() {
        for (code, expected_reason) in [
            (
                ErrorCode::InvalidTransactionNoLaneMatches,
                "couldn't associate a transaction lane with the transaction",
            ),
            (
                ErrorCode::InvalidTransactionEvmGasLimitExceedsTransactionGasLimit,
                "The EVM transaction gas limit exceeds the protocol transaction gas limit",
            ),
            (
                ErrorCode::InvalidTransactionEvmGasPriceBelowBaseFee,
                "The EVM transaction gas price is below the base fee",
            ),
            (
                ErrorCode::InvalidTransactionEvmMissingChainId,
                "The EVM transaction is missing a chain ID",
            ),
            (
                ErrorCode::InvalidTransactionEvmNonceTooFarAhead,
                "The EVM transaction nonce is too far ahead of the account nonce",
            ),
        ] {
            let node_client = Arc::new(ClientMock {
                error_code: Some(code),
                ..Default::default()
            });
            let error = SendRawTransaction::do_handle_request(
                node_client,
                SendRawTransactionParams {
                    raw_transaction: HexData::from(signed_eip7702_raw_transaction()),
                },
            )
            .await
            .expect_err("node should reject the transaction");
            let error = serde_json::to_value(error).unwrap();
            assert_eq!(error["code"], -32003);
            assert_eq!(error["message"], "Transaction rejected");
            assert_eq!(error["data"], expected_reason);
        }
    }

    #[tokio::test]
    async fn node_internal_failure_is_still_an_internal_error() {
        let node_client = Arc::new(ClientMock {
            error_code: Some(ErrorCode::InternalError),
            ..Default::default()
        });
        let error = SendRawTransaction::do_handle_request(
            node_client,
            SendRawTransactionParams {
                raw_transaction: HexData::from(signed_eip7702_raw_transaction()),
            },
        )
        .await
        .expect_err("node request should fail");
        assert_eq!(error.code(), -32603);
    }

    #[tokio::test]
    async fn accepts_eip7702_raw_transaction() {
        let raw_transaction = signed_eip7702_raw_transaction();
        let expected = EvmTransaction::from_signed_rlp(
            raw_transaction.clone(),
            Timestamp::zero(),
            DEFAULT_EVM_TX_TTL,
        )
        .expect("raw transaction should decode");
        let node_client = Arc::new(ClientMock::default());

        let hash = SendRawTransaction::do_handle_request(
            node_client.clone(),
            SendRawTransactionParams {
                raw_transaction: HexData::from(raw_transaction),
            },
        )
        .await
        .expect("raw transaction should be accepted");

        assert_eq!(hash, expected.hash().hash());
        let accepted = node_client
            .accepted
            .lock()
            .await
            .take()
            .expect("transaction should be sent to the node");
        let Transaction::Evm(accepted) = accepted else {
            panic!("expected an EVM transaction");
        };
        assert_eq!(accepted.kind(), EvmTransactionKind::Eip7702);
        assert_eq!(accepted.authorization_list(), expected.authorization_list());
    }

    #[tokio::test]
    async fn preserves_access_lists_through_submission_and_projection() {
        let access_list = AccessList(vec![
            AccessListItem {
                address: AlloyAddress::from([0x11; 20]),
                storage_keys: vec![B256::from([0x22; 32]), B256::from([0x33; 32])],
            },
            AccessListItem {
                address: AlloyAddress::from([0x44; 20]),
                storage_keys: Vec::new(),
            },
        ]);
        // Legacy envelopes cannot carry an access list. Check all three supported typed
        // envelopes across the RPC, Casper binary serialization, and Ethereum projection.
        for expected in fixture_transactions_with_access_list(access_list.clone())
            .into_iter()
            .skip(1)
        {
            let raw_transaction = expected.raw_signed_rlp().unwrap();
            let node_client = Arc::new(ClientMock::default());
            let hash = SendRawTransaction::do_handle_request(
                node_client.clone(),
                SendRawTransactionParams {
                    raw_transaction: HexData::from(raw_transaction.clone()),
                },
            )
            .await
            .expect("transaction carrying an access list should be forwarded");
            assert_eq!(hash, expected.hash().hash());

            let accepted = node_client.accepted.lock().await.take().unwrap();
            let decoded: Transaction =
                bytesrepr::deserialize(accepted.to_bytes().unwrap()).unwrap();
            let Transaction::Evm(decoded) = decoded else {
                panic!("expected an EVM transaction");
            };
            assert_eq!(decoded.access_list(), expected.access_list());
            assert_eq!(decoded.raw_signed_rlp().unwrap(), raw_transaction);

            let response = project_transaction(&decoded, TransactionLocation::Pending).unwrap();
            let response = serde_json::to_value(response).unwrap();
            assert_eq!(response["hash"], json!(hash));
            assert_eq!(
                response["accessList"],
                json!([
                    {
                        "address": format!("0x{}", "11".repeat(20)),
                        "storageKeys": [
                            format!("0x{}", "22".repeat(32)),
                            format!("0x{}", "33".repeat(32)),
                        ],
                    },
                    {
                        "address": format!("0x{}", "44".repeat(20)),
                        "storageKeys": [],
                    },
                ])
            );
        }
    }

    #[tokio::test]
    async fn preserves_metamask_zero_effective_tip_transaction() {
        let raw_transaction = "0x02f87c84435350ff8087038d7ea4c6800087038d7ea4c6800082520894f802d29f3329059ad20096fe903ce88d4d4eeb8d88d02ab486cedc000080c080a0ee519cfed46878e49a8fef1d894fb70e5a88df775142241ff0931628b7a860a1a0694740f25e9d7f73f79597b282e0d1738c8d4dc9bf8aa264fab89fdbd463db60";
        let params =
            SendRawTransaction::try_parse_params(Some(Params::Array(vec![json!(raw_transaction)])))
                .expect("captured MetaMask transaction should parse");
        let node_client = Arc::new(ClientMock::default());

        SendRawTransaction::do_handle_request(node_client.clone(), params)
            .await
            .expect("captured MetaMask transaction should be forwarded");

        let accepted = node_client
            .accepted
            .lock()
            .await
            .take()
            .expect("transaction should be sent to the node");
        let Transaction::Evm(accepted) = accepted else {
            panic!("expected an EVM transaction");
        };
        assert_eq!(accepted.kind(), EvmTransactionKind::Eip1559);
        assert_eq!(accepted.max_fee_per_gas(), 1_000_000_000_000_000);
        assert_eq!(
            accepted.max_priority_fee_per_gas(),
            Some(1_000_000_000_000_000)
        );
        assert_eq!(
            accepted.effective_priority_fee_per_gas(1_000_000_000_000_000),
            0
        );
    }

    fn signed_eip7702_raw_transaction() -> Vec<u8> {
        let authorization = Authorization {
            chain_id: AlloyU256::from(7),
            address: AlloyAddress::from([9; 20]),
            nonce: 4,
        };
        let authorization_signature = secp256k1::sign_message(
            B256::from(AUTHORIZATION_SECRET),
            authorization.signature_hash(),
        )
        .expect("authorization signing should succeed");
        let tx = TxEip7702 {
            chain_id: 7,
            nonce: 3,
            gas_limit: 70_000,
            max_fee_per_gas: 2_000_000_000,
            max_priority_fee_per_gas: 0,
            to: AlloyAddress::from([4; 20]),
            value: AlloyU256::from(987u64),
            access_list: AccessList::default(),
            authorization_list: vec![authorization.into_signed(authorization_signature)],
            input: AlloyBytes::from(vec![0xde, 0xad]),
        };
        let transaction_signature =
            secp256k1::sign_message(B256::from(SIGNING_SECRET), tx.signature_hash())
                .expect("transaction signing should succeed");
        let envelope: TxEnvelope = tx.into_signed(transaction_signature).into();
        envelope.encoded_2718()
    }
}
