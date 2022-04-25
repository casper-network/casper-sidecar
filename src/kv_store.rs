use anyhow::Error;
use rocksdb::{Options, DB};
use std::path::Path;
use std::sync::Arc;

/// Describes a Key-Value store, T should be the Struct containing the DB instance, i.e. RocksDB
pub trait KVStore<T> {
    /// Create a new DB instance to connect to or connect to an existing one.
    fn new(storage_path: &str) -> Result<T, Error>;

    /// Save data to database
    fn save(&self, k: &str, v: &str) -> Result<(), Error>;

    /// Find data from database with a key
    fn find(&self, k: &str) -> Result<Option<String>, Error>;
}

#[derive(Clone)]
pub struct RocksDB {
    db: Arc<DB>,
}

impl KVStore<RocksDB> for RocksDB {
    fn new(storage_path: &str) -> Result<RocksDB, Error> {
        let db = DB::open_default(storage_path).expect("Failed to open DB instance");
        Ok(RocksDB { db: Arc::new(db) })
    }

    fn save(&self, k: &str, v: &str) -> Result<(), Error> {
        match self.db.put(k.as_bytes(), v.as_bytes()) {
            Ok(_) => Ok(()),
            Err(_) => Err(Error::msg(format!("Unable to save {} under {}", v, k))),
        }
    }

    fn find(&self, k: &str) -> Result<Option<String>, Error> {
        match self.db.get(k.as_bytes()) {
            Ok(Some(v)) => {
                let result = String::from_utf8(v).map_err(|err| Error::msg(err.to_string()))?;
                Ok(Some(result))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(Error::msg(e.to_string())),
        }
    }
}

impl RocksDB {
    pub fn connect_read_only(path: &Path) -> Result<RocksDB, Error> {
        let connection = DB::open_for_read_only(&Options::default(), path, false)
            .expect("Failed to open read only connection to KV Store");
        Ok(RocksDB {
            db: Arc::new(connection),
        })
    }
}
