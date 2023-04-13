#[cfg(feature = "sse-data-testing")]
use blake2::{
    digest::{Update, VariableOutput},
    VarBlake2b,
};
use casper_types::{
    bytesrepr::{self, ToBytes},
    checksummed_hex,
};
use datasize::DataSize;
use hex_fmt::HexFmt;
use schemars::JsonSchema;
use serde::{de::Error as SerdeError, Deserialize, Deserializer, Serialize, Serializer};
use std::{
    array::TryFromSliceError,
    fmt::{self, Debug, Display, Formatter},
};

/// The output of the hash function.
#[derive(Copy, Clone, DataSize, Ord, PartialOrd, Eq, PartialEq, Hash, Default, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Digest(#[schemars(skip, with = "String")]  [u8; Digest::LENGTH]);

impl Digest {
    /// The number of bytes in a `Digest`.
    pub const LENGTH: usize = 32;

    #[cfg(feature = "sse-data-testing")]
    pub fn hash<T: AsRef<[u8]>>(data: T) -> Digest {
        Self::blake2b_hash(data)
    }

    #[cfg(feature = "sse-data-testing")]
    /// Creates a 32-byte BLAKE2b hash digest from a given a piece of data
    pub(crate) fn blake2b_hash<T: AsRef<[u8]>>(data: T) -> Digest {
        let mut ret = [0u8; Digest::LENGTH];
        // NOTE: Safe to unwrap here because our digest length is constant and valid
        let mut hasher = VarBlake2b::new(Digest::LENGTH).unwrap();
        hasher.update(data);
        hasher.finalize_variable(|hash| ret.clone_from_slice(hash));
        Digest(ret)
    }

    #[cfg(feature = "sse-data-testing")]
    /// Hashes a pair of byte slices.
    pub fn hash_pair<T: AsRef<[u8]>, U: AsRef<[u8]>>(data1: T, data2: U) -> Digest {
        let mut result = [0; Digest::LENGTH];
        let mut hasher = VarBlake2b::new(Digest::LENGTH).unwrap();
        hasher.update(data1);
        hasher.update(data2);
        hasher.finalize_variable(|slice| {
            result.copy_from_slice(slice);
        });
        Digest(result)
    }
    #[cfg(feature = "sse-data-testing")]
    pub fn into_vec(self) -> Vec<u8> {
        self.0.to_vec()
    }
}

#[cfg(feature = "sse-data-testing")]
impl AsRef<[u8]> for Digest {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

#[cfg(feature = "sse-data-testing")]
impl From<[u8; Digest::LENGTH]> for Digest {
    fn from(arr: [u8; Digest::LENGTH]) -> Self {
        Digest(arr)
    }
}

#[cfg(feature = "sse-data-testing")]
impl<'a> TryFrom<&'a [u8]> for Digest {
    type Error = TryFromSliceError;

    fn try_from(slice: &[u8]) -> Result<Digest, Self::Error> {
        <[u8; Digest::LENGTH]>::try_from(slice).map(Digest)
    }
}

impl Serialize for Digest {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            base16::encode_lower(&self.0).serialize(serializer)
        } else {
            // This is to keep backwards compatibility with how HexForm encodes
            // byte arrays. HexForm treats this like a slice.
            self.0[..].serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for Digest {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if deserializer.is_human_readable() {
            let hex_string = String::deserialize(deserializer)?;
            let bytes =
                checksummed_hex::decode(hex_string.as_bytes()).map_err(SerdeError::custom)?;
            let data =
                <[u8; Digest::LENGTH]>::try_from(bytes.as_ref()).map_err(SerdeError::custom)?;
            Ok(Digest::from(data))
        } else {
            let data = <Vec<u8>>::deserialize(deserializer)?;
            Digest::try_from(data.as_slice()).map_err(D::Error::custom)
        }
    }
}

#[cfg(feature = "sse-data-testing")]
impl Debug for Digest {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", base16::encode_lower(&self.0))
    }
}

#[cfg(feature = "sse-data-testing")]
impl Display for Digest {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{:10}", HexFmt(&self.0))
    }
}

#[cfg(feature = "sse-data-testing")]
impl ToBytes for Digest {
    #[inline(always)]
    fn to_bytes(&self) -> Result<Vec<u8>, bytesrepr::Error> {
        self.0.to_bytes()
    }

    #[inline(always)]
    fn serialized_length(&self) -> usize {
        self.0.serialized_length()
    }

    #[inline(always)]
    fn write_bytes(&self, writer: &mut Vec<u8>) -> Result<(), bytesrepr::Error> {
        writer.extend_from_slice(&self.0);
        Ok(())
    }
}
