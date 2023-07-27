use std::{
    array::TryFromSliceError,
    fmt::{self, Debug, Display, Formatter},
};

#[cfg(feature = "sse-data-testing")]
use blake2::{
    digest::{Update, VariableOutput},
    VarBlake2b,
};
use hex_fmt::HexFmt;
#[cfg(feature = "sse-data-testing")]
use rand::Rng;
use serde::{de::Error as SerdeError, Deserialize, Deserializer, Serialize, Serializer};

#[cfg(feature = "sse-data-testing")]
use casper_types::bytesrepr::{self, ToBytes};
use casper_types::checksummed_hex;
#[cfg(feature = "sse-data-testing")]
use casper_types::testing::TestRng;

/// The output of the hash function.
#[derive(Copy, Clone, Default, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct Digest([u8; Digest::LENGTH]);

impl Digest {
    /// The number of bytes in a `Digest`.
    pub const LENGTH: usize = 32;
}

impl<'a> TryFrom<&'a [u8]> for Digest {
    type Error = TryFromSliceError;

    fn try_from(slice: &[u8]) -> Result<Digest, Self::Error> {
        <[u8; Digest::LENGTH]>::try_from(slice).map(Digest)
    }
}

impl Serialize for Digest {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            HexFmt(&self.0).to_string().serialize(serializer)
        } else {
            // This is to keep backwards compatibility with how HexForm encodes byte arrays.
            // HexForm treats this like a slice.
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
            Ok(Digest(data))
        } else {
            let data = <Vec<u8>>::deserialize(deserializer)?;
            Digest::try_from(data.as_slice()).map_err(D::Error::custom)
        }
    }
}

impl Debug for Digest {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", HexFmt(&self.0))
    }
}

impl Display for Digest {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{:10}", HexFmt(&self.0))
    }
}

#[cfg(feature = "sse-data-testing")]
impl Digest {
    pub fn hash<T: AsRef<[u8]>>(data: T) -> Digest {
        let mut ret = [0u8; Digest::LENGTH];
        // NOTE: Safe to unwrap here because our digest length is constant and valid
        let mut hasher = VarBlake2b::new(Digest::LENGTH).unwrap();
        hasher.update(data);
        hasher.finalize_variable(|hash| ret.clone_from_slice(hash));
        Digest(ret)
    }

    pub fn random(rng: &mut TestRng) -> Digest {
        Digest(rng.gen())
    }

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
impl ToBytes for Digest {
    fn to_bytes(&self) -> Result<Vec<u8>, bytesrepr::Error> {
        self.0.to_bytes()
    }

    fn serialized_length(&self) -> usize {
        self.0.serialized_length()
    }
}
