use std::collections::BTreeMap;

use casper_binary_port::KeyPrefix;
use once_cell::sync::Lazy;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::rpcs::error::Error;
use casper_types::{
    bytesrepr::ToBytes, contracts::ContractPackage, global_state::TrieMerkleProof, Account,
    AddressableEntity, AvailableBlockRange, BlockHeader, BlockIdentifier, BlockWithSignatures,
    ByteCode, Contract, ContractWasm, EntityAddr, EntryPointValue, GlobalStateIdentifier, Key,
    NamedKeys, Package, StoredValue,
};

use crate::NodeClient;

pub(super) static MERKLE_PROOF: Lazy<String> = Lazy::new(|| {
    String::from(
        "01000000006ef2e0949ac76e55812421f755abe129b6244fe7168b77f47a72536147614625016ef2e0949ac76e\
        55812421f755abe129b6244fe7168b77f47a72536147614625000000003529cde5c621f857f75f3810611eb4af3\
        f998caaa9d4a3413cf799f99c67db0307010000006ef2e0949ac76e55812421f755abe129b6244fe7168b77f47a\
        7253614761462501010102000000006e06000000000074769d28aac597a36a03a932d4b43e4f10bf0403ee5c41d\
        d035102553f5773631200b9e173e8f05361b681513c14e25e3138639eb03232581db7557c9e8dbbc83ce9450022\
        6a9a7fe4f2b7b88d5103a4fc7400f02bf89c860c9ccdd56951a2afe9be0e0267006d820fb5676eb2960e15722f7\
        725f3f8f41030078f8b2e44bf0dc03f71b176d6e800dc5ae9805068c5be6da1a90b2528ee85db0609cc0fb4bd60\
        bbd559f497a98b67f500e1e3e846592f4918234647fca39830b7e1e6ad6f5b7a99b39af823d82ba1873d0000030\
        00000010186ff500f287e9b53f823ae1582b1fa429dfede28015125fd233a31ca04d5012002015cc42669a55467\
        a1fdf49750772bfc1aed59b9b085558eb81510e9b015a7c83b0301e3cf4a34b1db6bfa58808b686cb8fe21ebe0c\
        1bcbcee522649d2b135fe510fe3")
});

/// An enum to be used as the `data` field of a JSON-RPC error response.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields, untagged)]
pub enum ErrorData {
    /// The requested block of state root hash is not available on this node.
    MissingBlockOrStateRoot {
        /// Additional info.
        message: String,
        /// The height range (inclusive) of fully available blocks.
        available_block_range: AvailableBlockRange,
    },
}

/// An addressable entity or a legacy account or contract.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum EntityWithBackwardCompat {
    /// An addressable entity.
    AddressableEntity {
        /// The addressable entity.
        entity: AddressableEntity,
        /// The named keys of the addressable entity.
        named_keys: NamedKeys,
        /// The entry points of the addressable entity.
        entry_points: Vec<EntryPointValue>,
        /// The bytecode of the addressable entity. Returned when `include_bytecode` is `true`.
        bytecode: Option<ByteCodeWithProof>,
    },
    /// An account.
    Account(Account),
    /// A contract.
    Contract {
        /// The contract.
        contract: Contract,
        /// The Wasm code of the contract. Returned when `include_bytecode` is `true`.
        wasm: Option<ContractWasmWithProof>,
    },
}

/// A package or a legacy contract package.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum PackageWithBackwardCompat {
    /// A package.
    Package(Package),
    /// A contract package.
    ContractPackage(ContractPackage),
}

/// Byte code of an entity with a proof.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ByteCodeWithProof {
    code: ByteCode,
    merkle_proof: String,
}

impl ByteCodeWithProof {
    /// Creates a new `ByteCodeWithProof`.
    pub fn new(code: ByteCode, merkle_proof: String) -> Self {
        Self { code, merkle_proof }
    }
}

/// Wasm code of a contract with a proof.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ContractWasmWithProof {
    wasm: ContractWasm,
    merkle_proof: String,
}

impl ContractWasmWithProof {
    /// Creates a new `ContractWasmWithProof`.
    pub fn new(wasm: ContractWasm, merkle_proof: String) -> Self {
        Self { wasm, merkle_proof }
    }
}

pub async fn get_block_with_signatures(
    node_client: &dyn NodeClient,
    identifier: Option<BlockIdentifier>,
) -> Result<BlockWithSignatures, Error> {
    match node_client
        .read_block_with_signatures(identifier)
        .await
        .map_err(|err| Error::NodeRequest("block with signatures", err))?
    {
        Some(block) => Ok(block),
        None => {
            let available_range = node_client
                .read_available_block_range()
                .await
                .map_err(|err| Error::NodeRequest("available block range", err))?;
            Err(Error::NoBlockFound(identifier, available_range))
        }
    }
}

pub async fn get_block_header(
    node_client: &dyn NodeClient,
    identifier: Option<BlockIdentifier>,
) -> Result<BlockHeader, Error> {
    match node_client
        .read_block_header(identifier)
        .await
        .map_err(|err| Error::NodeRequest("block header", err))?
    {
        Some(header) => Ok(header),
        None => {
            let available_range = node_client
                .read_available_block_range()
                .await
                .map_err(|err| Error::NodeRequest("available block range", err))?;
            Err(Error::NoBlockFound(identifier, available_range))
        }
    }
}

pub async fn get_latest_switch_block_header(
    node_client: &dyn NodeClient,
) -> Result<BlockHeader, Error> {
    match node_client
        .read_latest_switch_block_header()
        .await
        .map_err(|err| Error::NodeRequest("latest switch block header", err))?
    {
        Some(header) => Ok(header),
        None => {
            let available_range = node_client
                .read_available_block_range()
                .await
                .map_err(|err| Error::NodeRequest("available block range", err))?;
            Err(Error::NoBlockFound(None, available_range))
        }
    }
}

pub async fn get_entity_named_keys(
    node_client: &dyn NodeClient,
    entity_addr: EntityAddr,
    state_identifier: Option<GlobalStateIdentifier>,
) -> Result<NamedKeys, Error> {
    let stored_values = node_client
        .query_global_state_by_prefix(state_identifier, KeyPrefix::NamedKeysByEntity(entity_addr))
        .await
        .map_err(|err| Error::NodeRequest("entity named keys", err))?;
    let named_keys = stored_values
        .into_iter()
        .map(|stored_value| {
            if let StoredValue::NamedKey(named_key) = stored_value {
                let key = named_key
                    .get_key()
                    .map_err(|err| Error::InvalidNamedKeys(err.to_string()))?;
                let name = named_key
                    .get_name()
                    .map_err(|err| Error::InvalidNamedKeys(err.to_string()))?;
                Ok((name, key))
            } else {
                Err(Error::InvalidNamedKeys(format!(
                    "unexpected stored value: {}",
                    stored_value.type_name()
                )))
            }
        })
        .collect::<Result<BTreeMap<String, Key>, Error>>()?;
    Ok(NamedKeys::from(named_keys))
}

pub async fn get_entity_entry_points(
    node_client: &dyn NodeClient,
    entity_addr: EntityAddr,
    state_identifier: Option<GlobalStateIdentifier>,
) -> Result<Vec<EntryPointValue>, Error> {
    let stored_values_v1 = node_client
        .query_global_state_by_prefix(
            state_identifier,
            KeyPrefix::EntryPointsV1ByEntity(entity_addr),
        )
        .await
        .map_err(|err| Error::NodeRequest("entity named keys", err))?;
    let stored_values_v2 = node_client
        .query_global_state_by_prefix(
            state_identifier,
            KeyPrefix::EntryPointsV2ByEntity(entity_addr),
        )
        .await
        .map_err(|err| Error::NodeRequest("entity named keys", err))?;

    stored_values_v1
        .into_iter()
        .chain(stored_values_v2)
        .map(|stored_value| {
            if let StoredValue::EntryPoint(entry_point) = stored_value {
                Ok(entry_point)
            } else {
                Err(Error::InvalidNamedKeys(format!(
                    "unexpected stored value: {}",
                    stored_value.type_name()
                )))
            }
        })
        .collect::<Result<_, _>>()
}

pub fn encode_proof(proof: &Vec<TrieMerkleProof<Key, StoredValue>>) -> Result<String, Error> {
    Ok(base16::encode_lower(
        &proof.to_bytes().map_err(Error::BytesreprFailure)?,
    ))
}
