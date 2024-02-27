use once_cell::sync::Lazy;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::rpcs::error::Error;
use casper_types::{
    account::AccountHash, addressable_entity::EntityKindTag, binary_port::GlobalStateQueryResult,
    bytesrepr::ToBytes, global_state::TrieMerkleProof, Account, AddressableEntity,
    AddressableEntityHash, AvailableBlockRange, BlockHeader, BlockIdentifier,
    GlobalStateIdentifier, Key, SignedBlock, StoredValue, URef, U512,
};

use crate::NodeClient;

use super::state::PurseIdentifier;

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

/// An addressable entity or a legacy account.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum EntityOrAccount {
    /// An addressable entity.
    AddressableEntity(AddressableEntity),
    /// A legacy account.
    LegacyAccount(Account),
}

pub async fn get_signed_block(
    node_client: &dyn NodeClient,
    identifier: Option<BlockIdentifier>,
) -> Result<SignedBlock, Error> {
    match node_client
        .read_signed_block(identifier)
        .await
        .map_err(|err| Error::NodeRequest("signed block", err))?
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

pub async fn resolve_account_hash(
    node_client: &dyn NodeClient,
    account_hash: AccountHash,
    state_identifier: Option<GlobalStateIdentifier>,
) -> Result<Option<SuccessfulQueryResult<EntityOrAccount>>, Error> {
    let account_key = Key::Account(account_hash);
    let Some((stored_value, account_merkle_proof)) = node_client
        .query_global_state(state_identifier, account_key, vec![])
        .await
        .map_err(|err| Error::NodeRequest("account stored value", err))?
        .map(GlobalStateQueryResult::into_inner)
    else {
        return Ok(None);
    };

    let (value, merkle_proof) = match stored_value {
        StoredValue::Account(account) => (
            EntityOrAccount::LegacyAccount(account),
            account_merkle_proof,
        ),
        StoredValue::CLValue(entity_key_as_clvalue) => {
            let key: Key = entity_key_as_clvalue
                .into_t()
                .map_err(|_| Error::InvalidAddressableEntity)?;
            let Some((value, merkle_proof)) = node_client
                .query_global_state(state_identifier, key, vec![])
                .await
                .map_err(|err| Error::NodeRequest("account owning a purse", err))?
                .map(GlobalStateQueryResult::into_inner)
            else {
                return Ok(None);
            };
            let entity = value
                .into_addressable_entity()
                .ok_or(Error::InvalidAddressableEntity)?;
            (EntityOrAccount::AddressableEntity(entity), merkle_proof)
        }
        _ => return Err(Error::InvalidAccountInfo),
    };
    Ok(Some(SuccessfulQueryResult {
        value,
        merkle_proof,
    }))
}

pub async fn resolve_entity_hash(
    node_client: &dyn NodeClient,
    tag: EntityKindTag,
    entity_hash: AddressableEntityHash,
    state_identifier: Option<GlobalStateIdentifier>,
) -> Result<Option<SuccessfulQueryResult<AddressableEntity>>, Error> {
    let entity_key = Key::addressable_entity_key(tag, entity_hash);
    let Some((value, merkle_proof)) = node_client
        .query_global_state(state_identifier, entity_key, vec![])
        .await
        .map_err(|err| Error::NodeRequest("entity stored value", err))?
        .map(GlobalStateQueryResult::into_inner)
    else {
        return Ok(None);
    };

    Ok(Some(SuccessfulQueryResult {
        value: value
            .into_addressable_entity()
            .ok_or(Error::InvalidAddressableEntity)?,
        merkle_proof,
    }))
}

pub async fn get_main_purse(
    node_client: &dyn NodeClient,
    identifier: PurseIdentifier,
    state_identifier: Option<GlobalStateIdentifier>,
) -> Result<URef, Error> {
    let account_hash = match identifier {
        PurseIdentifier::MainPurseUnderPublicKey(account_public_key) => {
            account_public_key.to_account_hash()
        }
        PurseIdentifier::MainPurseUnderAccountHash(account_hash) => account_hash,
        PurseIdentifier::PurseUref(purse_uref) => return Ok(purse_uref),
    };
    match resolve_account_hash(node_client, account_hash, state_identifier)
        .await?
        .ok_or(Error::MainPurseNotFound)?
        .value
    {
        EntityOrAccount::AddressableEntity(entity) => Ok(entity.main_purse()),
        EntityOrAccount::LegacyAccount(account) => Ok(account.main_purse()),
    }
}

pub async fn get_balance(
    node_client: &dyn NodeClient,
    uref: URef,
    state_identifier: Option<GlobalStateIdentifier>,
) -> Result<SuccessfulQueryResult<U512>, Error> {
    let key = Key::Balance(uref.addr());
    let (value, merkle_proof) = node_client
        .query_global_state(state_identifier, key, vec![])
        .await
        .map_err(|err| Error::NodeRequest("balance by uref", err))?
        .ok_or(Error::GlobalStateEntryNotFound)?
        .into_inner();
    let value = value
        .into_cl_value()
        .ok_or(Error::InvalidPurseBalance)?
        .into_t()
        .map_err(|_| Error::InvalidPurseBalance)?;
    Ok(SuccessfulQueryResult {
        value,
        merkle_proof,
    })
}

pub fn encode_proof(proof: &Vec<TrieMerkleProof<Key, StoredValue>>) -> Result<String, Error> {
    Ok(base16::encode_lower(
        &proof.to_bytes().map_err(Error::BytesreprFailure)?,
    ))
}

#[derive(Debug)]
pub struct SuccessfulQueryResult<A> {
    pub value: A,
    pub merkle_proof: Vec<TrieMerkleProof<Key, StoredValue>>,
}

impl<A> SuccessfulQueryResult<A> {
    pub fn into_inner(self) -> (A, Vec<TrieMerkleProof<Key, StoredValue>>) {
        (self.value, self.merkle_proof)
    }
}
