use alloy_consensus::{TxEnvelope, transaction::to_eip155_value};
use alloy_eips::{Decodable2718, eip2930::AccessList};
use alloy_primitives::{Address as AlloyAddress, B256, U256 as AlloyU256};
use casper_json_rpc::Error as RpcError;
use casper_types::{EvmTransaction, EvmTransactionHash, EvmTransactionKind, U256, evm};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{
    eth_u256::EthU256,
    types::{EthAddress, HexData, internal_error},
};

/// Whether an Ethereum transaction is pending or has been included in a block.
#[derive(Clone, Copy, Debug)]
pub(crate) enum TransactionLocation {
    Pending,
    BlockIncluded {
        block_hash: evm::Hash,
        block_number: u64,
        transaction_index: usize,
        effective_gas_price: u128,
    },
}

/// The transactions representation returned by Ethereum block lookup RPCs.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub(crate) enum BlockTransactions {
    Hashes(Vec<evm::Hash>),
    Full(Vec<TransactionResponse>),
}

impl BlockTransactions {
    #[cfg(test)]
    pub(crate) fn hashes(&self) -> Option<&[evm::Hash]> {
        match self {
            BlockTransactions::Hashes(hashes) => Some(hashes),
            BlockTransactions::Full(_) => None,
        }
    }

    #[cfg(test)]
    pub(crate) fn full(&self) -> Option<&[TransactionResponse]> {
        match self {
            BlockTransactions::Hashes(_) => None,
            BlockTransactions::Full(transactions) => Some(transactions),
        }
    }
}

/// Ethereum JSON-RPC transaction response for the supported signed envelope types.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub(crate) enum TransactionResponse {
    Eip7702(Eip7702TransactionResponse),
    Eip1559(Eip1559TransactionResponse),
    Eip2930(Eip2930TransactionResponse),
    Legacy(LegacyTransactionResponse),
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TransactionFields {
    block_hash: Option<evm::Hash>,
    block_number: Option<EthU256>,
    transaction_index: Option<EthU256>,
    hash: evm::Hash,
    from: EthAddress,
    to: Option<EthAddress>,
    nonce: EthU256,
    gas: EthU256,
    value: EthU256,
    input: HexData,
    r: EthU256,
    s: EthU256,
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct LegacyTransactionResponse {
    #[serde(flatten)]
    fields: TransactionFields,
    #[serde(rename = "type")]
    transaction_type: EthU256,
    chain_id: Option<EthU256>,
    gas_price: EthU256,
    v: EthU256,
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Eip2930TransactionResponse {
    #[serde(flatten)]
    fields: TransactionFields,
    #[serde(rename = "type")]
    transaction_type: EthU256,
    chain_id: EthU256,
    gas_price: EthU256,
    access_list: Vec<AccessListItemResponse>,
    v: EthU256,
    y_parity: EthU256,
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Eip1559TransactionResponse {
    #[serde(flatten)]
    fields: TransactionFields,
    #[serde(rename = "type")]
    transaction_type: EthU256,
    chain_id: EthU256,
    gas_price: EthU256,
    max_fee_per_gas: EthU256,
    max_priority_fee_per_gas: EthU256,
    access_list: Vec<AccessListItemResponse>,
    v: EthU256,
    y_parity: EthU256,
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Eip7702TransactionResponse {
    #[serde(flatten)]
    fields: TransactionFields,
    #[serde(rename = "type")]
    transaction_type: EthU256,
    chain_id: EthU256,
    gas_price: EthU256,
    max_fee_per_gas: EthU256,
    max_priority_fee_per_gas: EthU256,
    access_list: Vec<AccessListItemResponse>,
    authorization_list: Vec<AuthorizationResponse>,
    v: EthU256,
    y_parity: EthU256,
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AccessListItemResponse {
    address: EthAddress,
    storage_keys: Vec<evm::Hash>,
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AuthorizationResponse {
    chain_id: EthU256,
    address: EthAddress,
    nonce: EthU256,
    r: EthU256,
    s: EthU256,
    y_parity: EthU256,
}

/// Projects a stored Casper EVM transaction into the Ethereum JSON-RPC response shape.
pub(crate) fn project_transaction(
    transaction: &EvmTransaction,
    location: TransactionLocation,
) -> Result<TransactionResponse, RpcError> {
    let raw_signed_rlp = transaction
        .raw_signed_rlp()
        .map_err(|error| internal_error(format!("stored EVM envelope is invalid: {error}")))?;
    let envelope = decode_stored_envelope(&raw_signed_rlp)?;

    ensure_envelope_hash(&envelope, transaction.hash())?;

    let signature = envelope.signature();
    let fields = TransactionFields {
        block_hash: location.block_hash(),
        block_number: location.block_number().map(EthU256::from),
        transaction_index: location.transaction_index().map(EthU256::from),
        hash: transaction.hash().hash(),
        from: EthAddress::from(transaction.from()),
        to: transaction.to().map(EthAddress::from),
        nonce: EthU256::from(transaction.nonce()),
        gas: EthU256::from(transaction.gas_limit()),
        value: EthU256::from(transaction.value()),
        input: HexData::from(transaction.input()),
        r: alloy_u256_to_eth(signature.r()),
        s: alloy_u256_to_eth(signature.s()),
    };
    let y_parity = EthU256::from(u8::from(signature.v()));

    match envelope {
        TxEnvelope::Legacy(signed) => {
            require_kind(transaction, EvmTransactionKind::Legacy)?;
            let tx = signed.tx();
            Ok(TransactionResponse::Legacy(LegacyTransactionResponse {
                fields,
                transaction_type: EthU256::from(0u8),
                chain_id: tx.chain_id.map(EthU256::from),
                gas_price: EthU256::from(tx.gas_price),
                v: EthU256::from(to_eip155_value(signed.signature().v(), tx.chain_id)),
            }))
        }
        TxEnvelope::Eip2930(signed) => {
            require_kind(transaction, EvmTransactionKind::Eip2930)?;
            let tx = signed.tx();
            Ok(TransactionResponse::Eip2930(Eip2930TransactionResponse {
                fields,
                transaction_type: EthU256::from(1u8),
                chain_id: EthU256::from(tx.chain_id),
                gas_price: EthU256::from(tx.gas_price),
                access_list: project_access_list(&tx.access_list),
                v: y_parity,
                y_parity,
            }))
        }
        TxEnvelope::Eip1559(signed) => {
            require_kind(transaction, EvmTransactionKind::Eip1559)?;
            let tx = signed.tx();
            Ok(TransactionResponse::Eip1559(Eip1559TransactionResponse {
                fields,
                transaction_type: EthU256::from(2u8),
                chain_id: EthU256::from(tx.chain_id),
                gas_price: EthU256::from(location.dynamic_gas_price(tx.max_fee_per_gas)),
                max_fee_per_gas: EthU256::from(tx.max_fee_per_gas),
                max_priority_fee_per_gas: EthU256::from(tx.max_priority_fee_per_gas),
                access_list: project_access_list(&tx.access_list),
                v: y_parity,
                y_parity,
            }))
        }
        TxEnvelope::Eip7702(signed) => {
            require_kind(transaction, EvmTransactionKind::Eip7702)?;
            if transaction.to().is_none() {
                return Err(internal_error(
                    "stored EIP-7702 transaction does not include a recipient",
                ));
            }
            let tx = signed.tx();
            Ok(TransactionResponse::Eip7702(Eip7702TransactionResponse {
                fields,
                transaction_type: EthU256::from(4u8),
                chain_id: EthU256::from(tx.chain_id),
                gas_price: EthU256::from(location.dynamic_gas_price(tx.max_fee_per_gas)),
                max_fee_per_gas: EthU256::from(tx.max_fee_per_gas),
                max_priority_fee_per_gas: EthU256::from(tx.max_priority_fee_per_gas),
                access_list: project_access_list(&tx.access_list),
                authorization_list: tx
                    .authorization_list
                    .iter()
                    .map(|authorization| AuthorizationResponse {
                        chain_id: alloy_u256_to_eth(*authorization.chain_id()),
                        address: alloy_address_to_eth(*authorization.address()),
                        nonce: EthU256::from(authorization.nonce()),
                        r: alloy_u256_to_eth(authorization.r()),
                        s: alloy_u256_to_eth(authorization.s()),
                        y_parity: EthU256::from(authorization.y_parity()),
                    })
                    .collect(),
                v: y_parity,
                y_parity,
            }))
        }
        TxEnvelope::Eip4844(_) => Err(internal_error(
            "stored EVM transaction has unsupported envelope type 3",
        )),
    }
}

impl TransactionLocation {
    fn block_hash(self) -> Option<evm::Hash> {
        match self {
            TransactionLocation::Pending => None,
            TransactionLocation::BlockIncluded { block_hash, .. } => Some(block_hash),
        }
    }

    fn block_number(self) -> Option<u64> {
        match self {
            TransactionLocation::Pending => None,
            TransactionLocation::BlockIncluded { block_number, .. } => Some(block_number),
        }
    }

    fn transaction_index(self) -> Option<usize> {
        match self {
            TransactionLocation::Pending => None,
            TransactionLocation::BlockIncluded {
                transaction_index, ..
            } => Some(transaction_index),
        }
    }

    fn dynamic_gas_price(self, max_fee_per_gas: u128) -> u128 {
        match self {
            TransactionLocation::Pending => max_fee_per_gas,
            TransactionLocation::BlockIncluded {
                effective_gas_price,
                ..
            } => effective_gas_price,
        }
    }
}

fn require_kind(
    transaction: &EvmTransaction,
    decoded_kind: EvmTransactionKind,
) -> Result<(), RpcError> {
    if transaction.kind() != decoded_kind {
        return Err(internal_error(format!(
            "stored EVM transaction kind {} does not match decoded envelope kind {decoded_kind}",
            transaction.kind()
        )));
    }
    Ok(())
}

fn ensure_envelope_hash(
    envelope: &TxEnvelope,
    stored_hash: EvmTransactionHash,
) -> Result<(), RpcError> {
    if envelope.tx_hash().as_slice() != stored_hash.as_ref() {
        return Err(internal_error(
            "stored EVM transaction hash does not match its signed envelope",
        ));
    }
    Ok(())
}

fn decode_stored_envelope(raw_signed_rlp: &[u8]) -> Result<TxEnvelope, RpcError> {
    TxEnvelope::decode_2718_exact(raw_signed_rlp)
        .map_err(|error| internal_error(format!("stored EVM envelope is malformed: {error}")))
}

fn project_access_list(access_list: &AccessList) -> Vec<AccessListItemResponse> {
    access_list
        .iter()
        .map(|item| AccessListItemResponse {
            address: alloy_address_to_eth(item.address),
            storage_keys: item
                .storage_keys
                .iter()
                .copied()
                .map(alloy_hash_to_evm_hash)
                .collect(),
        })
        .collect()
}

fn alloy_address_to_eth(address: AlloyAddress) -> EthAddress {
    let mut bytes = [0u8; evm::ADDRESS_LENGTH];
    bytes.copy_from_slice(address.as_slice());
    EthAddress::from(evm::Address::new(bytes))
}

fn alloy_hash_to_evm_hash(hash: B256) -> evm::Hash {
    let mut bytes = [0u8; evm::HASH_LENGTH];
    bytes.copy_from_slice(hash.as_slice());
    evm::Hash::new(bytes)
}

fn alloy_u256_to_eth(value: AlloyU256) -> EthU256 {
    EthU256::from(U256::from_big_endian(&value.to_be_bytes::<32>()))
}

#[cfg(test)]
pub(super) mod tests {
    use alloy_consensus::{
        SignableTransaction, TxEip1559, TxEip2930, TxEip7702, TxEnvelope, TxLegacy,
        crypto::secp256k1,
    };
    use alloy_eips::{Encodable2718, eip2930::AccessList, eip7702::Authorization};
    use alloy_primitives::{
        Address as AlloyAddress, B256, Bytes as AlloyBytes, TxKind, U256 as AlloyU256,
    };
    use casper_json_rpc::ReservedErrorCode;
    use casper_types::{TimeDiff, Timestamp};

    use super::*;

    const SIGNING_SECRET: [u8; 32] = [7; 32];
    const AUTHORIZATION_SECRET: [u8; 32] = [8; 32];

    #[test]
    fn projects_all_supported_signed_envelope_fields() {
        let responses = fixture_transactions()
            .iter()
            .enumerate()
            .map(|(index, transaction)| {
                let location = if index < 2 {
                    TransactionLocation::Pending
                } else {
                    TransactionLocation::BlockIncluded {
                        block_hash: evm::Hash::new([0xaa; evm::HASH_LENGTH]),
                        block_number: 42,
                        transaction_index: index,
                        effective_gas_price: 1_500,
                    }
                };
                serde_json::to_value(project_transaction(transaction, location).unwrap()).unwrap()
            })
            .collect::<Vec<_>>();

        assert_eq!(
            serde_json::Value::Array(responses),
            serde_json::json!([
                {
                    "blockHash": null,
                    "blockNumber": null,
                    "transactionIndex": null,
                    "hash": "0x2446ba19118d01e32da60703fc9e614eb26363863982fb3d66f856a967835f4f",
                    "from": "0x4a62316623ad457f02cdc5d997ded67a383ec569",
                    "to": null,
                    "nonce": "0x1",
                    "gas": "0x5208",
                    "value": "0xb",
                    "input": "0x6000",
                    "r": "0x861c407cd5752914c7386631fd8095bf2128f1e5dedfaf1a1cb6c7870cda874d",
                    "s": "0x132d3d7d351a704220725de257291cfb760a3648a19890bf649aca29fa6e5f5e",
                    "type": "0x0",
                    "chainId": "0x7",
                    "gasPrice": "0x3e8",
                    "v": "0x31"
                },
                {
                    "blockHash": null,
                    "blockNumber": null,
                    "transactionIndex": null,
                    "hash": "0x47bd4d5c401b1def492ada8058836610a5e9deacceb67b39b1f49f4c0161163e",
                    "from": "0x4a62316623ad457f02cdc5d997ded67a383ec569",
                    "to": "0x2222222222222222222222222222222222222222",
                    "nonce": "0x2",
                    "gas": "0xc350",
                    "value": "0xc",
                    "input": "0xdead",
                    "r": "0x20c5a785b022506cf19cc008b94ef6caa1a139b8157f7edf505a959809cf2b1c",
                    "s": "0x775f159adf806cf4588d2e59f5a08a2d7f2a30b0d4fcaa702a29bc7f09ce5aac",
                    "type": "0x1",
                    "chainId": "0x7",
                    "gasPrice": "0x44c",
                    "accessList": [],
                    "v": "0x0",
                    "yParity": "0x0"
                },
                {
                    "blockHash": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "blockNumber": "0x2a",
                    "transactionIndex": "0x2",
                    "hash": "0x2c23c7fdbbbfeab53e1a6868a2939ec99ea9f72d74d877b166485683443747a4",
                    "from": "0x4a62316623ad457f02cdc5d997ded67a383ec569",
                    "to": "0x5555555555555555555555555555555555555555",
                    "nonce": "0x3",
                    "gas": "0xea60",
                    "value": "0xd",
                    "input": "0xbeef",
                    "r": "0xf50dc4d72ba5c6c0faa83d494042f416b587205b3b33a5aaec66f181712648d7",
                    "s": "0xdbffc6c648296dd45dbf4570a2077b3207db105306152711659838b00148c0f",
                    "type": "0x2",
                    "chainId": "0x7",
                    "gasPrice": "0x5dc",
                    "maxFeePerGas": "0xbb8",
                    "maxPriorityFeePerGas": "0x0",
                    "accessList": [],
                    "v": "0x1",
                    "yParity": "0x1"
                },
                {
                    "blockHash": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "blockNumber": "0x2a",
                    "transactionIndex": "0x3",
                    "hash": "0x5f4b79eb44cf4d491edca6bb52de99ac2fb3d051b7b4df9a77a6d65a477a8ae5",
                    "from": "0x4a62316623ad457f02cdc5d997ded67a383ec569",
                    "to": "0x7777777777777777777777777777777777777777",
                    "nonce": "0x4",
                    "gas": "0x11170",
                    "value": "0xe",
                    "input": "0xcafe",
                    "r": "0x4ad8d73617c165c43ee7595d1e0ea711b937bbbc6ec5c935fe2833bd3aeac9b6",
                    "s": "0x6fb961b5df6ec5f803c73a455959a6946b1aba98205182b945541281fca30221",
                    "type": "0x4",
                    "chainId": "0x7",
                    "gasPrice": "0x5dc",
                    "maxFeePerGas": "0xfa0",
                    "maxPriorityFeePerGas": "0x0",
                    "accessList": [],
                    "authorizationList": [{
                        "chainId": "0x7",
                        "address": "0x6666666666666666666666666666666666666666",
                        "nonce": "0x4",
                        "r": "0x18c0d70034a77bd8b47205a22880b28d56d47e3b82c8463e613d35664f820be0",
                        "s": "0x571f3b0535e6143bf0f30dbcc3596dd3ae3de60985dac234f65866ef152abf36",
                        "yParity": "0x1"
                    }],
                    "v": "0x1",
                    "yParity": "0x1"
                }
            ])
        );
    }

    #[test]
    fn missing_approval_is_an_internal_consistency_error() {
        let transaction = fixture_transactions().remove(0).with_evm_approval(None);

        let error = project_transaction(&transaction, TransactionLocation::Pending)
            .expect_err("stored transaction without an approval must fail");

        assert_eq!(
            error,
            RpcError::new(
                ReservedErrorCode::InternalError,
                "stored EVM envelope is invalid: missing EVM approval",
            )
        );
    }

    #[test]
    fn malformed_signed_envelope_is_an_internal_consistency_error() {
        let error = decode_stored_envelope(&[2, 0xff])
            .expect_err("malformed stored RLP must fail as an internal consistency error");

        assert_eq!(error.code(), ReservedErrorCode::InternalError as i64);
    }

    #[test]
    fn signed_envelope_hash_mismatch_is_an_internal_consistency_error() {
        let transaction = fixture_transactions().remove(0);
        let envelope =
            TxEnvelope::decode_2718_exact(&transaction.raw_signed_rlp().unwrap()).unwrap();

        let error = ensure_envelope_hash(&envelope, EvmTransactionHash::from_raw([0xff; 32]))
            .expect_err("a mismatched signed envelope hash must fail");

        assert_eq!(
            error,
            RpcError::new(
                ReservedErrorCode::InternalError,
                "stored EVM transaction hash does not match its signed envelope",
            )
        );
    }

    pub(crate) fn fixture_transactions() -> Vec<EvmTransaction> {
        fixture_transactions_with_access_list(AccessList::default())
    }

    pub(crate) fn fixture_transactions_with_access_list(
        access_list: AccessList,
    ) -> Vec<EvmTransaction> {
        let legacy = TxLegacy {
            chain_id: Some(7),
            nonce: 1,
            gas_price: 1_000,
            gas_limit: 21_000,
            to: TxKind::Create,
            value: AlloyU256::from(11),
            input: AlloyBytes::from(vec![0x60, 0x00]),
        };
        let eip2930 = TxEip2930 {
            chain_id: 7,
            nonce: 2,
            gas_price: 1_100,
            gas_limit: 50_000,
            to: TxKind::Call(AlloyAddress::from([0x22; 20])),
            value: AlloyU256::from(12),
            access_list: access_list.clone(),
            input: AlloyBytes::from(vec![0xde, 0xad]),
        };
        let eip1559 = TxEip1559 {
            chain_id: 7,
            nonce: 3,
            gas_limit: 60_000,
            max_fee_per_gas: 3_000,
            max_priority_fee_per_gas: 0,
            to: TxKind::Call(AlloyAddress::from([0x55; 20])),
            value: AlloyU256::from(13),
            access_list: access_list.clone(),
            input: AlloyBytes::from(vec![0xbe, 0xef]),
        };
        let authorization = Authorization {
            chain_id: AlloyU256::from(7),
            address: AlloyAddress::from([0x66; 20]),
            nonce: 4,
        };
        let authorization_signature = secp256k1::sign_message(
            B256::from(AUTHORIZATION_SECRET),
            authorization.signature_hash(),
        )
        .unwrap();
        let eip7702 = TxEip7702 {
            chain_id: 7,
            nonce: 4,
            gas_limit: 70_000,
            max_fee_per_gas: 4_000,
            max_priority_fee_per_gas: 0,
            to: AlloyAddress::from([0x77; 20]),
            value: AlloyU256::from(14),
            access_list,
            authorization_list: vec![authorization.into_signed(authorization_signature)],
            input: AlloyBytes::from(vec![0xca, 0xfe]),
        };

        vec![
            signed_transaction(legacy),
            signed_transaction(eip2930),
            signed_transaction(eip1559),
            signed_transaction(eip7702),
        ]
    }

    fn signed_transaction<T>(transaction: T) -> EvmTransaction
    where
        T: SignableTransaction<alloy_primitives::Signature>,
        TxEnvelope: From<alloy_consensus::Signed<T>>,
    {
        let signature =
            secp256k1::sign_message(B256::from(SIGNING_SECRET), transaction.signature_hash())
                .unwrap();
        let envelope: TxEnvelope = transaction.into_signed(signature).into();
        EvmTransaction::from_signed_rlp(
            envelope.encoded_2718(),
            Timestamp::from(1_000),
            TimeDiff::from_seconds(300),
        )
        .unwrap()
    }
}
