// //! Testing utilities.
// //!
// //! Contains various parts and components to aid writing tests and simulations using the
// //! `casper-node` library.
// pub(crate) mod test_clock;
// mod test_rng;
//
// use casper_hashing::Digest;
// use casper_types::testing::TestRng;
// use casper_types::{runtime_args, TimeDiff, Timestamp, U512, RuntimeArgs, bytesrepr::Bytes, ContractHash, ContractPackageHash, ContractVersion, SecretKey};
// use casper_node::types::{Deploy, DeployHash};
//
// use schemars::JsonSchema;
// use rand::{Rng, RngCore};
// use serde::{Deserialize, Deserializer, Serialize, Serializer};
// use hex_buffer_serde::{Hex, HexForm};
//
// mod contract_hash_as_digest {
//     use super::*;
//
//     pub(super) fn serialize<S: Serializer>(
//         contract_hash: &ContractHash,
//         serializer: S,
//     ) -> Result<S::Ok, S::Error> {
//         Digest::from(contract_hash.value()).serialize(serializer)
//     }
//
//     pub(super) fn deserialize<'de, D: Deserializer<'de>>(
//         deserializer: D,
//     ) -> Result<ContractHash, D::Error> {
//         let digest = Digest::deserialize(deserializer)?;
//         Ok(ContractHash::new(digest.value()))
//     }
// }
//
// mod contract_package_hash_as_digest {
//     use super::*;
//
//     pub(super) fn serialize<S: Serializer>(
//         contract_package_hash: &ContractPackageHash,
//         serializer: S,
//     ) -> Result<S::Ok, S::Error> {
//         Digest::from(contract_package_hash.value()).serialize(serializer)
//     }
//
//     pub(super) fn deserialize<'de, D: Deserializer<'de>>(
//         deserializer: D,
//     ) -> Result<ContractPackageHash, D::Error> {
//         let digest = Digest::deserialize(deserializer)?;
//         Ok(ContractPackageHash::new(digest.value()))
//     }
// }
//
// /// Represents possible variants of an executable deploy.
// #[derive(
// Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
// )]
// #[serde(deny_unknown_fields)]
// pub enum ExecutableDeployItem {
//     /// Executable specified as raw bytes that represent WASM code and an instance of
//     /// [`RuntimeArgs`].
//     ModuleBytes {
//         /// Raw WASM module bytes with assumed "call" export as an entrypoint.
//         #[serde(with = "HexForm")]
//         #[schemars(with = "String", description = "Hex-encoded raw Wasm bytes.")]
//         module_bytes: Bytes,
//         /// Runtime arguments.
//         args: RuntimeArgs,
//     },
//     /// Stored contract referenced by its [`ContractHash`], entry point and an instance of
//     /// [`RuntimeArgs`].
//     StoredContractByHash {
//         /// Contract hash.
//         #[serde(with = "contract_hash_as_digest")]
//         #[schemars(with = "String", description = "Hex-encoded hash.")]
//         hash: ContractHash,
//         /// Name of an entry point.
//         entry_point: String,
//         /// Runtime arguments.
//         args: RuntimeArgs,
//     },
//     /// Stored contract referenced by a named key existing in the signer's account context, entry
//     /// point and an instance of [`RuntimeArgs`].
//     StoredContractByName {
//         /// Named key.
//         name: String,
//         /// Name of an entry point.
//         entry_point: String,
//         /// Runtime arguments.
//         args: RuntimeArgs,
//     },
//     /// Stored versioned contract referenced by its [`ContractPackageHash`], entry point and an
//     /// instance of [`RuntimeArgs`].
//     StoredVersionedContractByHash {
//         /// Contract package hash
//         #[serde(with = "contract_package_hash_as_digest")]
//         #[schemars(with = "String", description = "Hex-encoded hash.")]
//         hash: ContractPackageHash,
//         /// An optional version of the contract to call. It will default to the highest enabled
//         /// version if no value is specified.
//         version: Option<ContractVersion>,
//         /// Entry point name.
//         entry_point: String,
//         /// Runtime arguments.
//         args: RuntimeArgs,
//     },
//     /// Stored versioned contract referenced by a named key existing in the signer's account
//     /// context, entry point and an instance of [`RuntimeArgs`].
//     StoredVersionedContractByName {
//         /// Named key.
//         name: String,
//         /// An optional version of the contract to call. It will default to the highest enabled
//         /// version if no value is specified.
//         version: Option<ContractVersion>,
//         /// Entry point name.
//         entry_point: String,
//         /// Runtime arguments.
//         args: RuntimeArgs,
//     },
//     /// A native transfer which does not contain or reference a WASM code.
//     Transfer {
//         /// Runtime arguments.
//         args: RuntimeArgs,
//     },
// }
//
// /// Creates a test deploy created at given instant and with given ttl.
// pub(crate) fn create_test_deploy(
//     created_ago: TimeDiff,
//     ttl: TimeDiff,
//     now: Timestamp,
//     test_rng: &mut TestRng,
// ) -> Deploy {
//     random_deploy_with_timestamp_and_ttl(
//         test_rng,
//         now - created_ago,
//         ttl
//     )
// }
//
// /// Creates a random deploy that is considered expired.
// pub(crate) fn create_expired_deploy(now: Timestamp, test_rng: &mut TestRng) -> Deploy {
//     create_test_deploy(
//         TimeDiff::from_seconds(20),
//         TimeDiff::from_seconds(10),
//         now,
//         test_rng,
//     )
// }
//
// /// Creates a random deploy that is considered not expired.
// pub(crate) fn create_not_expired_deploy(now: Timestamp, test_rng: &mut TestRng) -> Deploy {
//     create_test_deploy(
//         TimeDiff::from_seconds(20),
//         TimeDiff::from_seconds(60),
//         now,
//         test_rng,
//     )
// }
//
// // /// Generates a random instance but using the specified `timestamp` and `ttl`.
// // fn random_deploy_with_timestamp_and_ttl(
// //     rng: &mut TestRng,
// //     timestamp: Timestamp,
// //     ttl: TimeDiff,
// // ) -> Deploy {
// //     let gas_price = rng.gen_range(1..100);
// //
// //     let dependencies = vec![
// //         DeployHash::new(Digest::hash(rng.next_u64().to_le_bytes())),
// //         DeployHash::new(Digest::hash(rng.next_u64().to_le_bytes())),
// //         DeployHash::new(Digest::hash(rng.next_u64().to_le_bytes())),
// //     ];
// //     let chain_name = String::from("casper-example");
// //
// //     // We need "amount" in order to be able to get correct info via `deploy_info()`.
// //     let payment_args = runtime_args! {
// //             "amount" => U512::from(10i8),
// //         };
// //     let payment = ExecutableDeployItem::StoredContractByName {
// //         name: String::from("casper-example"),
// //         entry_point: String::from("example-entry-point"),
// //         args: payment_args,
// //     };
// //
// //     let session = rng.gen();
// //
// //     let secret_key = SecretKey::random(rng);
// //
// //     Deploy::new(
// //         timestamp,
// //         ttl,
// //         gas_price,
// //         dependencies,
// //         chain_name,
// //         // todo the payment expects the type that is internal to the node EE code.
// //         payment,
// //         session,
// //         &secret_key,
// //         None,
// //     )
// // }