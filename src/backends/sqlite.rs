use crate::store::{BlobStore, BlobStoreBuilder};
use anyhow::{Context, Result};
use rusqlite::{params, Connection, OpenFlags, OptionalExtension};
use std::path::Path;

pub type SqliteWithoutRowidStore = SqliteStoreImpl<true>;
pub type SqliteWithoutRowidStoreBuilder = SqliteStoreBuilderImpl<true>;

pub type SqliteRowidStore = SqliteStoreImpl<false>;
pub type SqliteRowidStoreBuilder = SqliteStoreBuilderImpl<false>;

/// Backwards-compatible alias: historically this was `WITHOUT ROWID`.
pub type SqliteStore = SqliteStoreImpl<true>;
/// Backwards-compatible alias: historically this was `WITHOUT ROWID`.
pub type SqliteStoreBuilder = SqliteStoreBuilderImpl<true>;

/// SQLite-based blob store using the native B-tree index.
///
/// `WITHOUT_ROWID = true` uses SQLite's `WITHOUT ROWID` table layout.
/// `WITHOUT_ROWID = false` uses a normal ROWID table with a unique index on `key`.
pub struct SqliteStoreImpl<const WITHOUT_ROWID: bool> {
    conn: Connection,
    count: usize,
}

impl<const WITHOUT_ROWID: bool> BlobStore for SqliteStoreImpl<WITHOUT_ROWID> {
    fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .context("Failed to open SQLite database")?;

        // Read-time optimizations
        conn.execute_batch(
            "
            PRAGMA mmap_size = 0;  -- Disable memory-mapped I/O
            PRAGMA cache_size = -32768;    -- 32MB page cache (negative = KB)
            PRAGMA temp_store = MEMORY;
            PRAGMA query_only = ON;
            ",
        )
        .context("Failed to set read pragmas")?;

        // Get the count
        let count: usize = conn
            .query_row("SELECT COUNT(*) FROM blobs", [], |row| row.get(0))
            .context("Failed to get count")?;

        Ok(Self { conn, count })
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut stmt = self
            .conn
            .prepare_cached("SELECT value FROM blobs WHERE key = ?")
            .context("Failed to prepare statement")?;

        let result = stmt
            .query_row([key], |row| row.get(0))
            .optional()
            .context("Failed to query blob")?;

        Ok(result)
    }

    fn keys(&self) -> Result<Vec<Vec<u8>>> {
        let mut stmt = self
            .conn
            .prepare("SELECT key FROM blobs")
            .context("Failed to prepare statement")?;

        let keys = stmt
            .query_map([], |row| row.get(0))
            .context("Failed to query keys")?
            .collect::<Result<Vec<Vec<u8>>, _>>()
            .context("Failed to collect keys")?;

        Ok(keys)
    }

    fn len(&self) -> usize {
        self.count
    }

    fn backend_name() -> &'static str {
        if WITHOUT_ROWID {
            "SQLite (WITHOUT ROWID)"
        } else {
            "SQLite (ROWID)"
        }
    }
}

/// Builder for SQLite blob store.
pub struct SqliteStoreBuilderImpl<const WITHOUT_ROWID: bool> {
    conn: Connection,
}

impl<const WITHOUT_ROWID: bool> BlobStoreBuilder for SqliteStoreBuilderImpl<WITHOUT_ROWID> {
    fn create(path: &Path) -> Result<Self> {
        // Remove existing file if present
        if path.exists() {
            std::fs::remove_file(path).context("Failed to remove existing file")?;
        }

        let conn = Connection::open(path).context("Failed to create SQLite database")?;

        let table_ddl = if WITHOUT_ROWID {
            "CREATE TABLE blobs (
                key BLOB PRIMARY KEY NOT NULL,
                value BLOB NOT NULL
            ) WITHOUT ROWID;"
        } else {
            "CREATE TABLE blobs (
                key BLOB PRIMARY KEY NOT NULL,
                value BLOB NOT NULL
            );"
        };

        let schema_sql = format!(
            "
            PRAGMA page_size = 4096;       -- Optimal for most filesystems
            PRAGMA journal_mode = OFF;     -- No journal for write-once data
            PRAGMA synchronous = OFF;      -- No fsync during builds
            PRAGMA cache_size = -32768;    -- 32MB page cache
            PRAGMA locking_mode = EXCLUSIVE;
            PRAGMA temp_store = MEMORY;
            
            {}
            
            BEGIN TRANSACTION;
            ",
            table_ddl
        );

        // Build-time optimizations
        // Note: page_size must be set before creating tables
        conn.execute_batch(&schema_sql)
            .context("Failed to create table")?;

        Ok(Self { conn })
    }

    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.conn
            .execute(
                "INSERT INTO blobs (key, value) VALUES (?, ?)",
                params![key, value],
            )
            .context("Failed to insert blob")?;
        Ok(())
    }

    fn finish(self) -> Result<()> {
        // Commit the transaction and optimize for reads
        self.conn
            .execute_batch(
                "
                COMMIT;           -- Commit bulk insert transaction
                PRAGMA optimize;  -- Run query planner optimizations
                ANALYZE;          -- Generate statistics for query planner
                VACUUM;           -- Compact database and defragment
                ",
            )
            .context("Failed to optimize")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::collection::vec as prop_vec;
    use proptest::prelude::*;
    use std::collections::HashMap;
    use tempfile::NamedTempFile;

    #[test]
    fn test_sqlite_roundtrip() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        // Build the store
        {
            let mut builder = SqliteStoreBuilder::create(path).unwrap();
            builder.insert(b"key1", b"value1").unwrap();
            builder.insert(b"key2", b"value2").unwrap();
            builder.insert(b"key3", b"value3").unwrap();
            builder.finish().unwrap();
        }

        // Read it back
        let store = SqliteStore::open(path).unwrap();

        assert_eq!(store.len(), 3);
        assert_eq!(store.get(b"key1").unwrap(), Some(b"value1".to_vec()));
        assert_eq!(store.get(b"key2").unwrap(), Some(b"value2".to_vec()));
        assert_eq!(store.get(b"key3").unwrap(), Some(b"value3".to_vec()));
        assert_eq!(store.get(b"nonexistent").unwrap(), None);
    }

    #[test]
    fn test_sqlite_keys() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        {
            let mut builder = SqliteStoreBuilder::create(path).unwrap();
            builder.insert(b"alpha", b"a").unwrap();
            builder.insert(b"beta", b"b").unwrap();
            builder.finish().unwrap();
        }

        let store = SqliteStore::open(path).unwrap();
        let mut keys = store.keys().unwrap();
        keys.sort();

        assert_eq!(keys, vec![b"alpha".to_vec(), b"beta".to_vec()]);
    }

    #[test]
    fn test_sqlite_binary_data() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        let binary_key: Vec<u8> = (0..255).collect();
        let binary_value: Vec<u8> = (0..1000).map(|i| (i % 256) as u8).collect();

        {
            let mut builder = SqliteStoreBuilder::create(path).unwrap();
            builder.insert(&binary_key, &binary_value).unwrap();
            builder.finish().unwrap();
        }

        let store = SqliteStore::open(path).unwrap();
        assert_eq!(store.get(&binary_key).unwrap(), Some(binary_value));
    }

    #[test]
    fn test_sqlite_empty_store() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        {
            let builder = SqliteStoreBuilder::create(path).unwrap();
            builder.finish().unwrap();
        }

        let store = SqliteStore::open(path).unwrap();
        assert_eq!(store.len(), 0);
        assert!(store.is_empty());
        assert_eq!(store.keys().unwrap(), Vec::<Vec<u8>>::new());
    }

    proptest! {
        #[test]
        fn prop_sqlite_roundtrip_single(key in prop_vec(any::<u8>(), 1..100), value in prop_vec(any::<u8>(), 0..1000)) {
            let temp_file = NamedTempFile::new().unwrap();
            let path = temp_file.path();

            {
                let mut builder = SqliteStoreBuilder::create(path).unwrap();
                builder.insert(&key, &value).unwrap();
                builder.finish().unwrap();
            }

            let store = SqliteStore::open(path).unwrap();
            prop_assert_eq!(store.len(), 1);
            prop_assert_eq!(store.get(&key).unwrap(), Some(value));
        }

        #[test]
        fn prop_sqlite_roundtrip_multiple(entries in prop_vec((prop_vec(any::<u8>(), 1..50), prop_vec(any::<u8>(), 0..500)), 1..50)) {
            let temp_file = NamedTempFile::new().unwrap();
            let path = temp_file.path();

            // Deduplicate keys (last value wins)
            let mut expected: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
            for (key, value) in &entries {
                expected.insert(key.clone(), value.clone());
            }

            {
                let mut builder = SqliteStoreBuilder::create(path).unwrap();
                for (key, value) in expected.iter() {
                    builder.insert(key, value).unwrap();
                }
                builder.finish().unwrap();
            }

            let store = SqliteStore::open(path).unwrap();
            prop_assert_eq!(store.len(), expected.len());

            for (key, value) in &expected {
                prop_assert_eq!(store.get(key).unwrap(), Some(value.clone()));
            }
        }

        #[test]
        fn prop_sqlite_missing_keys(
            stored_keys in prop_vec(prop_vec(any::<u8>(), 1..50), 1..20),
            missing_key in prop_vec(any::<u8>(), 1..50)
        ) {
            let temp_file = NamedTempFile::new().unwrap();
            let path = temp_file.path();

            {
                let mut builder = SqliteStoreBuilder::create(path).unwrap();
                for key in &stored_keys {
                    builder.insert(key, b"value").unwrap();
                }
                builder.finish().unwrap();
            }

            let store = SqliteStore::open(path).unwrap();

            // If missing_key is not in stored_keys, get should return None
            if !stored_keys.contains(&missing_key) {
                prop_assert_eq!(store.get(&missing_key).unwrap(), None);
            }
        }
    }
}
