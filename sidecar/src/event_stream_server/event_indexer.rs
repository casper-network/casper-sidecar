use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::{fs, path::PathBuf};

use tracing::{debug, error, warn};

const CACHE_FILENAME: &str = "sse_index";

pub(super) type EventIndex = AtomicU32;

#[derive(Clone, Debug)]
pub(super) struct EventIndexer {
    index: Arc<EventIndex>,
    persistent_cache: PathBuf,
}

impl EventIndexer {
    pub(super) fn new(storage_path: PathBuf) -> Self {
        fs::create_dir_all(&storage_path).unwrap_or_else(|err| {
            error!("Failed to create directory for sse cache: {}", err);
        });
        let persistent_cache = storage_path.join(CACHE_FILENAME);
        let mut bytes = 0u32.to_le_bytes();
        match fs::read(&persistent_cache) {
            Err(error) => {
                if persistent_cache.exists() {
                    warn!(
                        file = %persistent_cache.display(),
                        %error,
                        "failed to read sse cache file"
                    );
                }
            }
            Ok(cached_bytes) => {
                if cached_bytes.len() == bytes.len() {
                    bytes.copy_from_slice(cached_bytes.as_slice());
                } else {
                    warn!(
                        file = %persistent_cache.display(),
                        byte_count = %cached_bytes.len(),
                        "failed to parse sse cache file"
                    );
                }
            }
        }

        let index_u32 = u32::from_le_bytes(bytes);
        debug!(%index_u32, "initialized sse index");

        let index = Arc::new(EventIndex::new(index_u32));

        EventIndexer {
            index,
            persistent_cache,
        }
    }

    pub(super) fn next_index(&self) -> u32 {
        self.index.fetch_add(1, Ordering::SeqCst)
    }

    #[cfg(test)]
    pub(super) fn current_index(&self) -> u32 {
        self.index.load(Ordering::SeqCst)
    }
}

impl Drop for EventIndexer {
    fn drop(&mut self) {
        match fs::write(
            &self.persistent_cache,
            self.index.load(Ordering::SeqCst).to_le_bytes(),
        ) {
            Err(error) => warn!(
                file = %self.persistent_cache.display(),
                %error,
                "failed to write sse cache file"
            ),
            Ok(_) => debug!(
                file = %self.persistent_cache.display(),
                index = %self.index.load(Ordering::SeqCst),
                "cached sse index to file"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::iter;

    use super::*;

    #[test]
    fn should_persist_in_cache() {
        let tempdir = tempfile::tempdir().unwrap();

        // This represents a single session where five events are produced before the session ends.
        let init_and_increment_by_five = |expected_first_index: u32| {
            let event_indexer = EventIndexer::new(tempdir.path().to_path_buf());
            for i in 0..5 {
                assert_eq!(event_indexer.next_index(), expected_first_index + i);
            }
            // Explicitly drop, just to be clear that the cache write is being triggered.
            drop(event_indexer);
        };

        // Should start at 0 when no cache file exists.
        init_and_increment_by_five(0);

        // Should keep reading and writing to cache over ten subsequent sessions.
        for session in 1..11 {
            init_and_increment_by_five(session * 5);
        }
    }

    #[test]
    fn should_wrap() {
        let tempdir = tempfile::tempdir().unwrap();

        let mut event_indexer = EventIndexer::new(tempdir.path().to_path_buf());
        event_indexer.index = Arc::new(AtomicU32::new(u32::MAX));

        assert_eq!(event_indexer.next_index(), u32::MAX);
        assert_eq!(event_indexer.next_index(), 0);
    }

    #[test]
    fn should_reset_index_on_cache_read_failure() {
        let tempdir = tempfile::tempdir().unwrap();

        // Create a folder with the same name as the cache file to cause reading to fail.
        fs::create_dir(tempdir.path().join(CACHE_FILENAME)).unwrap();
        let event_indexer = EventIndexer::new(tempdir.path().to_path_buf());
        assert_eq!(event_indexer.next_index(), 0);
    }

    #[test]
    fn should_reset_index_on_corrupt_cache() {
        let tempdir = tempfile::tempdir().unwrap();

        {
            // Create the cache file with too few bytes to be parsed as an `Index`.
            let index: EventIndex = AtomicU32::new(1);
            fs::write(
                tempdir.path().join(CACHE_FILENAME),
                &index.load(Ordering::SeqCst).to_le_bytes()[1..],
            )
            .unwrap();

            let event_indexer = EventIndexer::new(tempdir.path().to_path_buf());
            assert_eq!(event_indexer.next_index(), 0);
        }

        {
            // Create the cache file with too many bytes to be parsed as an `Index`.
            let index: EventIndex = AtomicU32::new(1);
            let bytes: Vec<u8> = index
                .load(Ordering::SeqCst)
                .to_le_bytes()
                .iter()
                .chain(iter::once(&0))
                .copied()
                .collect();
            fs::write(tempdir.path().join(CACHE_FILENAME), bytes).unwrap();

            let event_indexer = EventIndexer::new(tempdir.path().to_path_buf());
            assert_eq!(event_indexer.next_index(), 0);
        }
    }
}
