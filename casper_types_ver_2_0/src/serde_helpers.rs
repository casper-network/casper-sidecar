use alloc::string::String;
use core::convert::TryFrom;

use serde::{de::Error as SerdeError, Deserialize, Deserializer, Serialize, Serializer};

use crate::Digest;

pub(crate) mod raw_32_byte_array {
    use super::*;

    pub(crate) fn serialize<S: Serializer>(
        array: &[u8; 32],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            base16::encode_lower(array).serialize(serializer)
        } else {
            array.serialize(serializer)
        }
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<[u8; 32], D::Error> {
        if deserializer.is_human_readable() {
            let hex_string = String::deserialize(deserializer)?;
            let bytes = base16::decode(hex_string.as_bytes()).map_err(SerdeError::custom)?;
            <[u8; 32]>::try_from(bytes.as_ref()).map_err(SerdeError::custom)
        } else {
            <[u8; 32]>::deserialize(deserializer)
        }
    }
}

pub(crate) mod contract_hash_as_digest {
    use super::*;
    use crate::AddressableEntityHash;

    pub(crate) fn serialize<S: Serializer>(
        contract_hash: &AddressableEntityHash,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        Digest::from(contract_hash.value()).serialize(serializer)
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<AddressableEntityHash, D::Error> {
        let digest = Digest::deserialize(deserializer)?;
        Ok(AddressableEntityHash::new(digest.value()))
    }
}

pub(crate) mod contract_package_hash_as_digest {
    use super::*;
    use crate::PackageHash;

    pub(crate) fn serialize<S: Serializer>(
        contract_package_hash: &PackageHash,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        Digest::from(contract_package_hash.value()).serialize(serializer)
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<PackageHash, D::Error> {
        let digest = Digest::deserialize(deserializer)?;
        Ok(PackageHash::new(digest.value()))
    }
}

/// This module allows `DeployHash`es to be serialized and deserialized using the underlying
/// `[u8; 32]` rather than delegating to the wrapped `Digest`, which in turn delegates to a
/// `Vec<u8>` for legacy reasons.
///
/// This is required as the `DeployHash` defined in `casper-types` up until v4.0.0 used the array
/// form, while the `DeployHash` defined in `casper-node` during this period delegated to `Digest`.
///
/// We use this module in places where the old `casper_types_ver_2_0::DeployHash` was held as a member of a
/// type which implements `Serialize` and/or `Deserialize`.
pub(crate) mod deploy_hash_as_array {
    use super::*;
    use crate::DeployHash;

    pub(crate) fn serialize<S: Serializer>(
        deploy_hash: &DeployHash,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            base16::encode_lower(&deploy_hash.inner().value()).serialize(serializer)
        } else {
            deploy_hash.inner().value().serialize(serializer)
        }
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<DeployHash, D::Error> {
        let bytes = if deserializer.is_human_readable() {
            let hex_string = String::deserialize(deserializer)?;
            let vec_bytes = base16::decode(hex_string.as_bytes()).map_err(SerdeError::custom)?;
            <[u8; DeployHash::LENGTH]>::try_from(vec_bytes.as_ref()).map_err(SerdeError::custom)?
        } else {
            <[u8; DeployHash::LENGTH]>::deserialize(deserializer)?
        };
        Ok(DeployHash::new(Digest::from(bytes)))
    }
}
