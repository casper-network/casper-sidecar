use hex_buffer_serde::{Hex, HexForm};
#[cfg(feature = "sse-data-testing")]
use rand::{
    distributions::{Alphanumeric, Distribution, Standard},
    Rng,
};
use serde::{Deserialize, Serialize};

#[cfg(feature = "sse-data-testing")]
use casper_types::bytesrepr::{self, Bytes, ToBytes};
use casper_types::{
    system::auction::ARG_AMOUNT, CLValue, ContractHash, ContractPackageHash, ContractVersion,
    RuntimeArgs, U512,
};
use utoipa::ToSchema;

#[cfg(feature = "sse-data-testing")]
macro_rules! bx {
    ($e:expr) => {
        Box::new($e)
    };
}

#[cfg(feature = "sse-data-testing")]
const TAG_LENGTH: usize = 1;
#[cfg(feature = "sse-data-testing")]
const MODULE_BYTES_TAG: u8 = 0;
#[cfg(feature = "sse-data-testing")]
const STORED_CONTRACT_BY_HASH_TAG: u8 = 1;
#[cfg(feature = "sse-data-testing")]
const STORED_CONTRACT_BY_NAME_TAG: u8 = 2;
#[cfg(feature = "sse-data-testing")]
const STORED_VERSIONED_CONTRACT_BY_HASH_TAG: u8 = 3;
#[cfg(feature = "sse-data-testing")]
const STORED_VERSIONED_CONTRACT_BY_NAME_TAG: u8 = 4;
#[cfg(feature = "sse-data-testing")]
const TRANSFER_TAG: u8 = 5;
#[cfg(feature = "sse-data-testing")]
const MAX_PAYMENT_AMOUNT: u64 = 2_500_000_000;

/// The payment or session code of a [`Deploy`].
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Debug, ToSchema)]
#[serde(deny_unknown_fields)]
pub enum ExecutableDeployItem {
    /// Raw bytes of compiled Wasm code, which must include a `call` entry point, and the arguments
    /// to call at runtime.
    ModuleBytes {
        /// The compiled Wasm bytes.
        #[schema(value_type = String)]
        module_bytes: Bytes,
        /// The arguments to be passed to the entry point at runtime.
        args: RuntimeArgs,
    },
    /// A contract stored in global state, referenced by its "hash", along with the entry point and
    /// arguments to call at runtime.
    StoredContractByHash {
        /// The contract's identifier.
        #[serde(with = "HexForm")]
        hash: ContractHash,
        /// The contract's entry point to be called at runtime.
        entry_point: String,
        /// The arguments to be passed to the entry point at runtime.
        args: RuntimeArgs,
    },
    /// A contract stored in global state, referenced by a named key existing in the `Deploy`'s
    /// account context, along with the entry point and arguments to call at runtime.
    StoredContractByName {
        /// The named of the named key under which the contract is referenced.
        name: String,
        /// The contract's entry point to be called at runtime.
        entry_point: String,
        /// The arguments to be passed to the entry point at runtime.
        args: RuntimeArgs,
    },
    /// A versioned contract stored in global state, referenced by its "hash", along with the entry
    /// point and arguments to call at runtime.
    StoredVersionedContractByHash {
        /// The contract package's identifier.
        #[serde(with = "HexForm")]
        hash: ContractPackageHash,
        /// The version of the contract to call.  If `None`, the highest enabled version is used.
        version: Option<ContractVersion>,
        /// The contract's entry point to be called at runtime.
        entry_point: String,
        /// The arguments to be passed to the entry point at runtime.
        args: RuntimeArgs,
    },
    /// A versioned contract stored in global state, referenced by a named key existing in the
    /// `Deploy`'s account context, along with the entry point and arguments to call at runtime.
    StoredVersionedContractByName {
        /// The named of the named key under which the contract package is referenced.
        name: String,
        /// The version of the contract to call.  If `None`, the highest enabled version is used.
        version: Option<ContractVersion>,
        /// The contract's entry point to be called at runtime.
        entry_point: String,
        /// The arguments to be passed to the entry point at runtime.
        args: RuntimeArgs,
    },
    /// A native transfer which does not contain or reference any Wasm code.
    Transfer {
        /// The arguments to be passed to the native transfer entry point at runtime.
        args: RuntimeArgs,
    },
}

#[cfg(feature = "sse-data-testing")]
impl ExecutableDeployItem {
    fn fields_serialized_length(&self) -> usize {
        let components: Vec<Box<dyn ToBytes>> = match self {
            ExecutableDeployItem::ModuleBytes { module_bytes, args } => {
                vec![bx!(module_bytes), bx!(args)]
            }
            ExecutableDeployItem::StoredContractByHash {
                hash,
                entry_point,
                args,
            } => vec![bx!(hash), bx!(entry_point), bx!(args)],
            ExecutableDeployItem::StoredContractByName {
                name,
                entry_point,
                args,
            } => vec![bx!(name), bx!(entry_point), bx!(args)],
            ExecutableDeployItem::StoredVersionedContractByHash {
                hash,
                version,
                entry_point,
                args,
            } => vec![bx!(hash), bx!(version), bx!(entry_point), bx!(args)],
            ExecutableDeployItem::StoredVersionedContractByName {
                name,
                version,
                entry_point,
                args,
            } => vec![bx!(name), bx!(version), bx!(entry_point), bx!(args)],
            ExecutableDeployItem::Transfer { args } => vec![bx!(args)],
        };
        components
            .into_iter()
            .map(|to_bytes| to_bytes.serialized_length())
            .sum()
    }
}

#[cfg(feature = "sse-data-testing")]
impl ToBytes for ExecutableDeployItem {
    fn to_bytes(&self) -> Result<Vec<u8>, bytesrepr::Error> {
        let mut buffer = bytesrepr::allocate_buffer(self)?;
        match self {
            ExecutableDeployItem::ModuleBytes { module_bytes, args } => {
                write_module_bytes(&mut buffer, module_bytes, args)?
            }
            ExecutableDeployItem::StoredContractByHash {
                hash,
                entry_point,
                args,
            } => write_stored_contract(&mut buffer, hash, entry_point, args)?,
            ExecutableDeployItem::StoredContractByName {
                name,
                entry_point,
                args,
            } => write_stored_contract_by_name(&mut buffer, name, entry_point, args)?,
            ExecutableDeployItem::StoredVersionedContractByHash {
                hash,
                version,
                entry_point,
                args,
            } => write_versioned_contract_by_hash(&mut buffer, hash, version, entry_point, args)?,
            ExecutableDeployItem::StoredVersionedContractByName {
                name,
                version,
                entry_point,
                args,
            } => write_versioned_contract_by_name(&mut buffer, name, version, entry_point, args)?,
            ExecutableDeployItem::Transfer { args } => write_transfer(&mut buffer, args)?,
        }
        Ok(buffer)
    }

    fn serialized_length(&self) -> usize {
        TAG_LENGTH + self.fields_serialized_length()
    }
}

#[cfg(feature = "sse-data-testing")]
fn write_transfer(buffer: &mut Vec<u8>, args: &RuntimeArgs) -> Result<(), bytesrepr::Error> {
    buffer.insert(0, TRANSFER_TAG);
    buffer.extend(args.to_bytes()?);
    Ok(())
}

#[cfg(feature = "sse-data-testing")]
fn write_versioned_contract_by_name(
    buffer: &mut Vec<u8>,
    name: &String,
    version: &Option<u32>,
    entry_point: &String,
    args: &RuntimeArgs,
) -> Result<(), bytesrepr::Error> {
    buffer.insert(0, STORED_VERSIONED_CONTRACT_BY_NAME_TAG);
    buffer.extend(name.to_bytes()?);
    buffer.extend(version.to_bytes()?);
    buffer.extend(entry_point.to_bytes()?);
    buffer.extend(args.to_bytes()?);
    Ok(())
}

#[cfg(feature = "sse-data-testing")]
fn write_versioned_contract_by_hash(
    buffer: &mut Vec<u8>,
    hash: &ContractPackageHash,
    version: &Option<u32>,
    entry_point: &String,
    args: &RuntimeArgs,
) -> Result<(), bytesrepr::Error> {
    buffer.insert(0, STORED_VERSIONED_CONTRACT_BY_HASH_TAG);
    buffer.extend(hash.to_bytes()?);
    buffer.extend(version.to_bytes()?);
    buffer.extend(entry_point.to_bytes()?);
    buffer.extend(args.to_bytes()?);
    Ok(())
}

#[cfg(feature = "sse-data-testing")]
fn write_stored_contract_by_name(
    buffer: &mut Vec<u8>,
    name: &String,
    entry_point: &String,
    args: &RuntimeArgs,
) -> Result<(), bytesrepr::Error> {
    buffer.insert(0, STORED_CONTRACT_BY_NAME_TAG);
    buffer.extend(name.to_bytes()?);
    buffer.extend(entry_point.to_bytes()?);
    buffer.extend(args.to_bytes()?);
    Ok(())
}

#[cfg(feature = "sse-data-testing")]
fn write_stored_contract(
    buffer: &mut Vec<u8>,
    hash: &ContractHash,
    entry_point: &String,
    args: &RuntimeArgs,
) -> Result<(), bytesrepr::Error> {
    buffer.insert(0, STORED_CONTRACT_BY_HASH_TAG);
    buffer.extend(hash.to_bytes()?);
    buffer.extend(entry_point.to_bytes()?);
    buffer.extend(args.to_bytes()?);
    Ok(())
}

#[cfg(feature = "sse-data-testing")]
fn write_module_bytes(
    buffer: &mut Vec<u8>,
    module_bytes: &Bytes,
    args: &RuntimeArgs,
) -> Result<(), bytesrepr::Error> {
    buffer.insert(0, MODULE_BYTES_TAG);
    buffer.extend(module_bytes.to_bytes()?);
    buffer.extend(args.to_bytes()?);
    Ok(())
}

#[cfg(feature = "sse-data-testing")]
impl Distribution<ExecutableDeployItem> for Standard {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> ExecutableDeployItem {
        let mut args = RuntimeArgs::new();
        let _ = args.insert(random_string(rng), Bytes::from(random_bytes(rng)));
        match rng.gen_range(0..5) {
            0 => ExecutableDeployItem::ModuleBytes {
                module_bytes: random_bytes(rng).into(),
                args,
            },
            1 => ExecutableDeployItem::StoredContractByHash {
                hash: ContractHash::new(rng.gen()),
                entry_point: random_string(rng),
                args,
            },
            2 => ExecutableDeployItem::StoredContractByName {
                name: random_string(rng),
                entry_point: random_string(rng),
                args,
            },
            3 => ExecutableDeployItem::StoredVersionedContractByHash {
                hash: ContractPackageHash::new(rng.gen()),
                version: rng.gen(),
                entry_point: random_string(rng),
                args,
            },
            4 => ExecutableDeployItem::StoredVersionedContractByName {
                name: random_string(rng),
                version: rng.gen(),
                entry_point: random_string(rng),
                args,
            },
            5 => random_transfer(rng),
            _ => unreachable!(),
        }
    }
}

#[cfg(feature = "sse-data-testing")]
fn random_string<R: Rng + ?Sized>(rng: &mut R) -> String {
    rng.sample_iter(&Alphanumeric)
        .take(20)
        .map(char::from)
        .collect()
}

#[cfg(feature = "sse-data-testing")]
fn random_bytes<R: Rng + ?Sized>(rng: &mut R) -> Vec<u8> {
    let mut bytes = vec![0u8; rng.gen_range(0..100)];
    rng.fill_bytes(bytes.as_mut());
    bytes
}

#[cfg(feature = "sse-data-testing")]
fn random_transfer<R: Rng + ?Sized>(rng: &mut R) -> ExecutableDeployItem {
    let amount = rng.gen_range(MAX_PAYMENT_AMOUNT..1_000_000_000_000_000);
    let mut transfer_args = RuntimeArgs::new();
    transfer_args.insert_cl_value(
        ARG_AMOUNT,
        CLValue::from_t(U512::from(amount)).expect("should get CLValue from U512"),
    );
    ExecutableDeployItem::Transfer {
        args: transfer_args,
    }
}
