use casper_execution_engine::core::engine_state::MAX_PAYMENT_AMOUNT;
use hex_buffer_serde::{Hex, HexForm};
use serde::{Deserialize, Serialize};

use casper_types::{bytesrepr::{self, Bytes, ToBytes}, ContractHash, ContractPackageHash, ContractVersion, RuntimeArgs, U512, CLValue};
use casper_types::system::auction::ARG_AMOUNT;
use rand::distributions::{Alphanumeric, Distribution, Standard};
use rand::Rng;

const TAG_LENGTH: usize = 1;
const MODULE_BYTES_TAG: u8 = 0;
const STORED_CONTRACT_BY_HASH_TAG: u8 = 1;
const STORED_CONTRACT_BY_NAME_TAG: u8 = 2;
const STORED_VERSIONED_CONTRACT_BY_HASH_TAG: u8 = 3;
const STORED_VERSIONED_CONTRACT_BY_NAME_TAG: u8 = 4;
const TRANSFER_TAG: u8 = 5;

/// The payment or session code of a [`Deploy`].
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub enum ExecutableDeployItem {
    /// Raw bytes of compiled Wasm code, which must include a `call` entry point, and the arguments
    /// to call at runtime.
    ModuleBytes {
        /// The compiled Wasm bytes.
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

// impl ExecutableDeployItem {
//     /// Returns a new `ExecutableDeployItem::ModuleBytes`.
//     pub fn new_module_bytes(module_bytes: Bytes, args: RuntimeArgs) -> Self {
//         ExecutableDeployItem::ModuleBytes { module_bytes, args }
//     }
//
//     /// Returns a new `ExecutableDeployItem::ModuleBytes` suitable for use as standard payment code
//     /// of a `Deploy`.
//     pub fn new_standard_payment<A: Into<U512>>(amount: A) -> Self {
//         ExecutableDeployItem::ModuleBytes {
//             module_bytes: Bytes::new(),
//             args: runtime_args! {
//                 STANDARD_PAYMENT_ARG_AMOUNT => amount.into(),
//             },
//         }
//     }
//
//     /// Returns a new `ExecutableDeployItem::StoredContractByHash`.
//     pub fn new_stored_contract_by_hash(
//         hash: ContractHash,
//         entry_point: String,
//         args: RuntimeArgs,
//     ) -> Self {
//         ExecutableDeployItem::StoredContractByHash {
//             hash,
//             entry_point,
//             args,
//         }
//     }
//
//     /// Returns a new `ExecutableDeployItem::StoredContractByName`.
//     pub fn new_stored_contract_by_name(
//         name: String,
//         entry_point: String,
//         args: RuntimeArgs,
//     ) -> Self {
//         ExecutableDeployItem::StoredContractByName {
//             name,
//             entry_point,
//             args,
//         }
//     }
//
//     /// Returns a new `ExecutableDeployItem::StoredVersionedContractByHash`.
//     pub fn new_stored_versioned_contract_by_hash(
//         hash: ContractPackageHash,
//         version: Option<ContractVersion>,
//         entry_point: String,
//         args: RuntimeArgs,
//     ) -> Self {
//         ExecutableDeployItem::StoredVersionedContractByHash {
//             hash,
//             version,
//             entry_point,
//             args,
//         }
//     }
//
//     /// Returns a new `ExecutableDeployItem::StoredVersionedContractByName`.
//     pub fn new_stored_versioned_contract_by_name(
//         name: String,
//         version: Option<ContractVersion>,
//         entry_point: String,
//         args: RuntimeArgs,
//     ) -> Self {
//         ExecutableDeployItem::StoredVersionedContractByName {
//             name,
//             version,
//             entry_point,
//             args,
//         }
//     }
//
//     /// Returns a new `ExecutableDeployItem` suitable for use as session code for a transfer.
//     ///
//     /// If `maybe_source` is None, the account's main purse is used as the source.
//     pub fn new_transfer<A: Into<U512>>(
//         amount: A,
//         maybe_source: Option<URef>,
//         target: TransferTarget,
//         maybe_transfer_id: Option<u64>,
//     ) -> Self {
//         let mut args = RuntimeArgs::new();
//         args.insert(TRANSFER_ARG_AMOUNT, amount.into())
//             .expect("should serialize amount arg");
//
//         if let Some(source) = maybe_source {
//             args.insert(TRANSFER_ARG_SOURCE, source)
//                 .expect("should serialize source arg");
//         }
//
//         match target {
//             TransferTarget::PublicKey(public_key) => args
//                 .insert(TRANSFER_ARG_TARGET, public_key)
//                 .expect("should serialize public key target arg"),
//             TransferTarget::AccountHash(account_hash) => args
//                 .insert(TRANSFER_ARG_TARGET, account_hash)
//                 .expect("should serialize account hash target arg"),
//             TransferTarget::URef(uref) => args
//                 .insert(TRANSFER_ARG_TARGET, uref)
//                 .expect("should serialize uref target arg"),
//         }
//
//         args.insert(TRANSFER_ARG_ID, maybe_transfer_id)
//             .expect("should serialize transfer id arg");
//
//         ExecutableDeployItem::Transfer { args }
//     }
//
//     /// Returns the runtime arguments.
//     #[cfg(test)]
//     pub(crate) fn args(&self) -> &RuntimeArgs {
//         match self {
//             ExecutableDeployItem::ModuleBytes { args, .. }
//             | ExecutableDeployItem::StoredContractByHash { args, .. }
//             | ExecutableDeployItem::StoredContractByName { args, .. }
//             | ExecutableDeployItem::StoredVersionedContractByHash { args, .. }
//             | ExecutableDeployItem::StoredVersionedContractByName { args, .. }
//             | ExecutableDeployItem::Transfer { args } => args,
//         }
//     }
// }

impl ToBytes for ExecutableDeployItem {
    fn to_bytes(&self) -> Result<Vec<u8>, bytesrepr::Error> {
        let mut buffer = bytesrepr::allocate_buffer(self)?;
        match self {
            ExecutableDeployItem::ModuleBytes { module_bytes, args } => {
                buffer.insert(0, MODULE_BYTES_TAG);
                buffer.extend(module_bytes.to_bytes()?);
                buffer.extend(args.to_bytes()?);
            }
            ExecutableDeployItem::StoredContractByHash {
                hash,
                entry_point,
                args,
            } => {
                buffer.insert(0, STORED_CONTRACT_BY_HASH_TAG);
                buffer.extend(hash.to_bytes()?);
                buffer.extend(entry_point.to_bytes()?);
                buffer.extend(args.to_bytes()?)
            }
            ExecutableDeployItem::StoredContractByName {
                name,
                entry_point,
                args,
            } => {
                buffer.insert(0, STORED_CONTRACT_BY_NAME_TAG);
                buffer.extend(name.to_bytes()?);
                buffer.extend(entry_point.to_bytes()?);
                buffer.extend(args.to_bytes()?)
            }
            ExecutableDeployItem::StoredVersionedContractByHash {
                hash,
                version,
                entry_point,
                args,
            } => {
                buffer.insert(0, STORED_VERSIONED_CONTRACT_BY_HASH_TAG);
                buffer.extend(hash.to_bytes()?);
                buffer.extend(version.to_bytes()?);
                buffer.extend(entry_point.to_bytes()?);
                buffer.extend(args.to_bytes()?)
            }
            ExecutableDeployItem::StoredVersionedContractByName {
                name,
                version,
                entry_point,
                args,
            } => {
                buffer.insert(0, STORED_VERSIONED_CONTRACT_BY_NAME_TAG);
                buffer.extend(name.to_bytes()?);
                buffer.extend(version.to_bytes()?);
                buffer.extend(entry_point.to_bytes()?);
                buffer.extend(args.to_bytes()?)
            }
            ExecutableDeployItem::Transfer { args } => {
                buffer.insert(0, TRANSFER_TAG);
                buffer.extend(args.to_bytes()?)
            }
        }
        Ok(buffer)
    }

    fn serialized_length(&self) -> usize {
        TAG_LENGTH
            + match self {
                ExecutableDeployItem::ModuleBytes { module_bytes, args } => {
                    module_bytes.serialized_length() + args.serialized_length()
                }
                ExecutableDeployItem::StoredContractByHash {
                    hash,
                    entry_point,
                    args,
                } => {
                    hash.serialized_length()
                        + entry_point.serialized_length()
                        + args.serialized_length()
                }
                ExecutableDeployItem::StoredContractByName {
                    name,
                    entry_point,
                    args,
                } => {
                    name.serialized_length()
                        + entry_point.serialized_length()
                        + args.serialized_length()
                }
                ExecutableDeployItem::StoredVersionedContractByHash {
                    hash,
                    version,
                    entry_point,
                    args,
                } => {
                    hash.serialized_length()
                        + version.serialized_length()
                        + entry_point.serialized_length()
                        + args.serialized_length()
                }
                ExecutableDeployItem::StoredVersionedContractByName {
                    name,
                    version,
                    entry_point,
                    args,
                } => {
                    name.serialized_length()
                        + version.serialized_length()
                        + entry_point.serialized_length()
                        + args.serialized_length()
                }
                ExecutableDeployItem::Transfer { args } => args.serialized_length(),
            }
    }
}

impl Distribution<ExecutableDeployItem> for Standard {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> ExecutableDeployItem {
        fn random_bytes<R: Rng + ?Sized>(rng: &mut R) -> Vec<u8> {
            let mut bytes = vec![0u8; rng.gen_range(0..100)];
            rng.fill_bytes(bytes.as_mut());
            bytes
        }

        fn random_string<R: Rng + ?Sized>(rng: &mut R) -> String {
            rng.sample_iter(&Alphanumeric)
                .take(20)
                .map(char::from)
                .collect()
        }

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
            5 => {
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
            _ => unreachable!(),
        }
    }
}