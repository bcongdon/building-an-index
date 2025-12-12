use anyhow::Result;
use std::path::Path;

/// Trait for read-only access to a blob store.
/// All implementations are optimized for read-only access at runtime.
pub trait BlobStore: Sized {
    /// Open an existing blob store from the given path.
    fn open(path: &Path) -> Result<Self>;

    /// Get a blob by its key. Returns None if the key doesn't exist.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Get all keys in the store.
    fn keys(&self) -> Result<Vec<Vec<u8>>>;

    /// Get the number of entries in the store.
    fn len(&self) -> usize;

    /// Check if the store is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get the name of this backend for display purposes.
    fn backend_name() -> &'static str;
}

/// Trait for building a blob store.
/// Used during the build phase to create the index files.
pub trait BlobStoreBuilder: Sized {
    /// Create a new blob store builder at the given path.
    fn create(path: &Path) -> Result<Self>;

    /// Insert a key-value pair into the store.
    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()>;

    /// Finish building the store and flush to disk.
    fn finish(self) -> Result<()>;
}
