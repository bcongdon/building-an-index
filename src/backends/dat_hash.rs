use crate::store::{BlobStore, BlobStoreBuilder};
use anyhow::{bail, Context, Result};
use std::cell::RefCell;
use std::collections::hash_map::DefaultHasher;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

const MAGIC: &[u8; 8] = b"HASHIDX1";
const HEADER_SIZE: usize = 64;
const BUCKET_SIZE: usize = 24; // key_hash (8) + blob_offset (8) + blob_len (8)
const LOAD_FACTOR: f64 = 0.7; // Keep load factor below this

/// Header layout:
/// - magic: 8 bytes
/// - bucket_count: 8 bytes (u64)
/// - blob_heap_offset: 8 bytes (u64)
/// - entry_count: 8 bytes (u64)
/// - reserved: 32 bytes

/// Bucket layout:
/// - key_hash: 8 bytes (u64, 0 = empty)
/// - blob_offset: 8 bytes (u64)
/// - blob_len: 8 bytes (u64)

/// Blob heap entry layout:
/// - key_len: 4 bytes (u32)
/// - key: variable
/// - value: rest until blob_len

/// Hash .dat store with an in-memory lookup table and disk-based blob reads.
/// (No mmap.) Buckets are read into RAM on open; blob data is read via disk seeks.
pub struct HashDatStore {
    /// Parsed hash buckets loaded into memory at open()
    buckets: Vec<Bucket>,
    /// File handle for reading blob data via seeks
    data_file: RefCell<File>,
    bucket_count: u64,
    entry_count: usize,
}

#[derive(Clone, Copy, Debug)]
struct Bucket {
    key_hash: u64,
    blob_offset: u64,
    blob_len: u64,
}

impl HashDatStore {
    fn hash_key(key: &[u8]) -> u64 {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let h = hasher.finish();
        // Ensure non-zero (0 means empty bucket)
        if h == 0 {
            1
        } else {
            h
        }
    }

    fn get_bucket(&self, index: usize) -> (u64, u64, u64) {
        let b = self.buckets[index];
        (b.key_hash, b.blob_offset, b.blob_len)
    }

    /// Read data from file at the given offset (disk seek)
    fn read_at(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        let mut file = self.data_file.borrow_mut();
        file.seek(SeekFrom::Start(offset))?;
        let mut buf = vec![0u8; len];
        file.read_exact(&mut buf)?;
        Ok(buf)
    }

    fn find_key(&self, key: &[u8]) -> Result<Option<(u64, u64)>> {
        let key_hash = Self::hash_key(key);
        let bucket_count = self.bucket_count as usize;
        let mut index = (key_hash as usize) % bucket_count;

        for _ in 0..bucket_count {
            let (stored_hash, blob_offset, blob_len) = self.get_bucket(index);

            if stored_hash == 0 {
                // Empty bucket, key not found
                return Ok(None);
            }

            if stored_hash == key_hash {
                // Potential match, verify key in blob heap via disk read
                // Read just the key_len first (4 bytes)
                let key_len_buf = self.read_at(blob_offset, 4)?;
                let key_len = u32::from_le_bytes(key_len_buf[0..4].try_into().unwrap()) as usize;

                // Read the actual key
                let stored_key = self.read_at(blob_offset + 4, key_len)?;

                if stored_key == key {
                    return Ok(Some((blob_offset, blob_len)));
                }
            }

            // Linear probing
            index = (index + 1) % bucket_count;
        }

        Ok(None)
    }

    fn get_blob(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        // Read the entire blob entry
        let blob_data = self.read_at(offset, len as usize)?;

        let key_len = u32::from_le_bytes(blob_data[0..4].try_into().unwrap()) as usize;
        let value_start = 4 + key_len;
        let value_len = len as usize - 4 - key_len;

        Ok(blob_data[value_start..value_start + value_len].to_vec())
    }
}

impl BlobStore for HashDatStore {
    fn open(path: &Path) -> Result<Self> {
        // Read header (no mmap)
        let mut header_file = File::open(path).context("Failed to open hash dat file")?;
        let mut header = [0u8; HEADER_SIZE];
        header_file
            .read_exact(&mut header)
            .context("Failed to read hash dat header")?;

        if &header[0..8] != MAGIC {
            bail!("Invalid magic number");
        }

        let bucket_count = u64::from_le_bytes(header[8..16].try_into().unwrap());
        let blob_heap_offset = u64::from_le_bytes(header[16..24].try_into().unwrap());
        let entry_count = u64::from_le_bytes(header[24..32].try_into().unwrap()) as usize;

        let expected_blob_heap_offset = (HEADER_SIZE + bucket_count as usize * BUCKET_SIZE) as u64;
        if blob_heap_offset != expected_blob_heap_offset {
            bail!(
                "Invalid blob_heap_offset: expected {}, got {}",
                expected_blob_heap_offset,
                blob_heap_offset
            );
        }

        // Read and parse buckets into memory.
        let bucket_bytes_len = bucket_count as usize * BUCKET_SIZE;
        let mut bucket_bytes = vec![0u8; bucket_bytes_len];
        header_file
            .read_exact(&mut bucket_bytes)
            .context("Failed to read hash buckets")?;

        let mut buckets = Vec::with_capacity(bucket_count as usize);
        for i in 0..bucket_count as usize {
            let off = i * BUCKET_SIZE;
            let data = &bucket_bytes[off..off + BUCKET_SIZE];

            let key_hash = u64::from_le_bytes(data[0..8].try_into().unwrap());
            let blob_offset = u64::from_le_bytes(data[8..16].try_into().unwrap());
            let blob_len = u64::from_le_bytes(data[16..24].try_into().unwrap());
            buckets.push(Bucket {
                key_hash,
                blob_offset,
                blob_len,
            });
        }

        // Open another file handle for data reads
        let data_file = File::open(path).context("Failed to open hash dat file for data reads")?;

        Ok(Self {
            buckets,
            data_file: RefCell::new(data_file),
            bucket_count,
            entry_count,
        })
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.find_key(key)? {
            Some((offset, len)) => Ok(Some(self.get_blob(offset, len)?)),
            None => Ok(None),
        }
    }

    fn keys(&self) -> Result<Vec<Vec<u8>>> {
        let mut keys = Vec::with_capacity(self.entry_count);
        let bucket_count = self.bucket_count as usize;

        for i in 0..bucket_count {
            let (key_hash, blob_offset, blob_len) = self.get_bucket(i);

            if key_hash != 0 {
                // Read blob from disk to extract key
                let blob_data = self.read_at(blob_offset, blob_len as usize)?;

                let key_len = u32::from_le_bytes(blob_data[0..4].try_into().unwrap()) as usize;
                let key = blob_data[4..4 + key_len].to_vec();
                keys.push(key);
            }
        }

        Ok(keys)
    }

    fn len(&self) -> usize {
        self.entry_count
    }

    fn backend_name() -> &'static str {
        "Custom Offset File Format"
    }
}

/// Builder for hash .dat store.
pub struct HashDatStoreBuilder {
    path: std::path::PathBuf,
    entries: Vec<(Vec<u8>, Vec<u8>)>,
}

impl BlobStoreBuilder for HashDatStoreBuilder {
    fn create(path: &Path) -> Result<Self> {
        Ok(Self {
            path: path.to_path_buf(),
            entries: Vec::new(),
        })
    }

    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.entries.push((key.to_vec(), value.to_vec()));
        Ok(())
    }

    fn finish(self) -> Result<()> {
        let file = File::create(&self.path).context("Failed to create hash dat file")?;
        let mut writer = BufWriter::new(file);

        let entry_count = self.entries.len();
        let bucket_count = ((entry_count as f64 / LOAD_FACTOR).ceil() as usize).max(1);

        // Write header placeholder
        writer.write_all(&[0u8; HEADER_SIZE])?;

        // Initialize buckets
        let mut buckets: Vec<(u64, u64, u64)> = vec![(0, 0, 0); bucket_count];

        // Calculate blob heap offset
        let blob_heap_offset = (HEADER_SIZE + bucket_count * BUCKET_SIZE) as u64;
        let mut current_blob_offset = blob_heap_offset;

        // Build blob heap entries and populate buckets
        let mut blob_heap: Vec<u8> = Vec::new();

        for (key, value) in &self.entries {
            let key_hash = HashDatStore::hash_key(key);

            // Find bucket using linear probing
            let mut index = (key_hash as usize) % bucket_count;
            loop {
                if buckets[index].0 == 0 {
                    // Empty bucket found
                    let blob_len = 4 + key.len() + value.len();
                    buckets[index] = (key_hash, current_blob_offset, blob_len as u64);

                    // Add to blob heap: key_len + key + value
                    blob_heap.extend_from_slice(&(key.len() as u32).to_le_bytes());
                    blob_heap.extend_from_slice(key);
                    blob_heap.extend_from_slice(value);

                    current_blob_offset += blob_len as u64;
                    break;
                }
                index = (index + 1) % bucket_count;
            }
        }

        // Write buckets
        for (key_hash, blob_offset, blob_len) in &buckets {
            writer.write_all(&key_hash.to_le_bytes())?;
            writer.write_all(&blob_offset.to_le_bytes())?;
            writer.write_all(&blob_len.to_le_bytes())?;
        }

        // Write blob heap
        writer.write_all(&blob_heap)?;

        // Go back and write header
        writer.seek(SeekFrom::Start(0))?;
        writer.write_all(MAGIC)?;
        writer.write_all(&(bucket_count as u64).to_le_bytes())?;
        writer.write_all(&blob_heap_offset.to_le_bytes())?;
        writer.write_all(&(entry_count as u64).to_le_bytes())?;

        writer.flush()?;

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
    fn test_hash_roundtrip() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        // Build the store
        {
            let mut builder = HashDatStoreBuilder::create(path).unwrap();
            builder.insert(b"key1", b"value1").unwrap();
            builder.insert(b"key2", b"value2").unwrap();
            builder.insert(b"key3", b"value3").unwrap();
            builder.finish().unwrap();
        }

        // Read it back
        let store = HashDatStore::open(path).unwrap();

        assert_eq!(store.len(), 3);
        assert_eq!(store.get(b"key1").unwrap(), Some(b"value1".to_vec()));
        assert_eq!(store.get(b"key2").unwrap(), Some(b"value2".to_vec()));
        assert_eq!(store.get(b"key3").unwrap(), Some(b"value3".to_vec()));
        assert_eq!(store.get(b"nonexistent").unwrap(), None);
    }

    #[test]
    fn test_hash_keys() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        {
            let mut builder = HashDatStoreBuilder::create(path).unwrap();
            builder.insert(b"alpha", b"a").unwrap();
            builder.insert(b"beta", b"b").unwrap();
            builder.finish().unwrap();
        }

        let store = HashDatStore::open(path).unwrap();
        let mut keys = store.keys().unwrap();
        keys.sort(); // Hash table doesn't preserve order

        assert_eq!(keys, vec![b"alpha".to_vec(), b"beta".to_vec()]);
    }

    #[test]
    fn test_hash_binary_data() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        let binary_key: Vec<u8> = (0..255).collect();
        let binary_value: Vec<u8> = (0..1000).map(|i| (i % 256) as u8).collect();

        {
            let mut builder = HashDatStoreBuilder::create(path).unwrap();
            builder.insert(&binary_key, &binary_value).unwrap();
            builder.finish().unwrap();
        }

        let store = HashDatStore::open(path).unwrap();
        assert_eq!(store.get(&binary_key).unwrap(), Some(binary_value));
    }

    #[test]
    fn test_hash_collision_handling() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        // Insert many keys to trigger collisions
        let num_entries = 100;

        {
            let mut builder = HashDatStoreBuilder::create(path).unwrap();
            for i in 0..num_entries {
                let key = format!("key_{:04}", i);
                let value = format!("value_{:04}", i);
                builder.insert(key.as_bytes(), value.as_bytes()).unwrap();
            }
            builder.finish().unwrap();
        }

        let store = HashDatStore::open(path).unwrap();
        assert_eq!(store.len(), num_entries);

        // Verify all entries can be retrieved
        for i in 0..num_entries {
            let key = format!("key_{:04}", i);
            let expected_value = format!("value_{:04}", i);
            assert_eq!(
                store.get(key.as_bytes()).unwrap(),
                Some(expected_value.into_bytes())
            );
        }
    }

    #[test]
    fn test_hash_large_values() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        let large_value: Vec<u8> = (0..100_000).map(|i| (i % 256) as u8).collect();

        {
            let mut builder = HashDatStoreBuilder::create(path).unwrap();
            builder.insert(b"large", &large_value).unwrap();
            builder.finish().unwrap();
        }

        let store = HashDatStore::open(path).unwrap();
        assert_eq!(store.get(b"large").unwrap(), Some(large_value));
    }

    #[test]
    fn test_hash_empty_store() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        {
            let builder = HashDatStoreBuilder::create(path).unwrap();
            builder.finish().unwrap();
        }

        let store = HashDatStore::open(path).unwrap();
        assert_eq!(store.len(), 0);
        assert!(store.is_empty());
    }

    proptest! {
        #[test]
        fn prop_hash_roundtrip_single(key in prop_vec(any::<u8>(), 1..100), value in prop_vec(any::<u8>(), 0..1000)) {
            let temp_file = NamedTempFile::new().unwrap();
            let path = temp_file.path();

            {
                let mut builder = HashDatStoreBuilder::create(path).unwrap();
                builder.insert(&key, &value).unwrap();
                builder.finish().unwrap();
            }

            let store = HashDatStore::open(path).unwrap();
            prop_assert_eq!(store.len(), 1);
            prop_assert_eq!(store.get(&key).unwrap(), Some(value));
        }

        #[test]
        fn prop_hash_roundtrip_multiple(entries in prop_vec((prop_vec(any::<u8>(), 1..50), prop_vec(any::<u8>(), 0..500)), 1..50)) {
            let temp_file = NamedTempFile::new().unwrap();
            let path = temp_file.path();

            // Deduplicate keys
            let mut expected: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
            for (key, value) in &entries {
                expected.insert(key.clone(), value.clone());
            }

            {
                let mut builder = HashDatStoreBuilder::create(path).unwrap();
                for (key, value) in expected.iter() {
                    builder.insert(key, value).unwrap();
                }
                builder.finish().unwrap();
            }

            let store = HashDatStore::open(path).unwrap();
            prop_assert_eq!(store.len(), expected.len());

            for (key, value) in &expected {
                prop_assert_eq!(store.get(key).unwrap(), Some(value.clone()));
            }
        }

        #[test]
        fn prop_hash_handles_collisions(num_entries in 10..100usize) {
            let temp_file = NamedTempFile::new().unwrap();
            let path = temp_file.path();

            let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..num_entries)
                .map(|i| (format!("key_{:06}", i).into_bytes(), format!("value_{:06}", i).into_bytes()))
                .collect();

            {
                let mut builder = HashDatStoreBuilder::create(path).unwrap();
                for (key, value) in &entries {
                    builder.insert(key, value).unwrap();
                }
                builder.finish().unwrap();
            }

            let store = HashDatStore::open(path).unwrap();
            prop_assert_eq!(store.len(), num_entries);

            for (key, value) in &entries {
                prop_assert_eq!(store.get(key).unwrap(), Some(value.clone()));
            }
        }

        #[test]
        fn prop_hash_missing_keys(
            stored_keys in prop_vec(prop_vec(any::<u8>(), 1..50), 1..20),
            missing_key in prop_vec(any::<u8>(), 1..50)
        ) {
            let temp_file = NamedTempFile::new().unwrap();
            let path = temp_file.path();

            {
                let mut builder = HashDatStoreBuilder::create(path).unwrap();
                for key in &stored_keys {
                    builder.insert(key, b"value").unwrap();
                }
                builder.finish().unwrap();
            }

            let store = HashDatStore::open(path).unwrap();

            if !stored_keys.contains(&missing_key) {
                prop_assert_eq!(store.get(&missing_key).unwrap(), None);
            }
        }
    }
}
