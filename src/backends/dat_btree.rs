use crate::store::{BlobStore, BlobStoreBuilder};
use anyhow::{bail, Context, Result};
use memmap2::Mmap;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::Path;

const MAGIC: &[u8; 8] = b"BTREEIDX";
const HEADER_SIZE: usize = 64;

/// Header layout:
/// - magic: 8 bytes
/// - btree_root_offset: 8 bytes (u64)
/// - blob_heap_offset: 8 bytes (u64)
/// - entry_count: 8 bytes (u64)
/// - reserved: 32 bytes
#[repr(C)]
struct Header {
    magic: [u8; 8],
    btree_root_offset: u64,
    blob_heap_offset: u64,
    entry_count: u64,
}

/// B-tree node entry in a page:
/// - key_len: 4 bytes (u32)
/// - key: variable
/// - blob_offset: 8 bytes (u64)
/// - blob_len: 8 bytes (u64)

/// B-tree .dat store using memory-mapped file.
pub struct BTreeDatStore {
    mmap: Mmap,
    btree_root_offset: u64,
    blob_heap_offset: u64,
    entry_count: usize,
}

impl BTreeDatStore {
    fn read_header(data: &[u8]) -> Result<Header> {
        if data.len() < HEADER_SIZE {
            bail!("File too small for header");
        }

        let mut magic = [0u8; 8];
        magic.copy_from_slice(&data[0..8]);

        if &magic != MAGIC {
            bail!("Invalid magic number");
        }

        let btree_root_offset = u64::from_le_bytes(data[8..16].try_into().unwrap());
        let blob_heap_offset = u64::from_le_bytes(data[16..24].try_into().unwrap());
        let entry_count = u64::from_le_bytes(data[24..32].try_into().unwrap());

        Ok(Header {
            magic,
            btree_root_offset,
            blob_heap_offset,
            entry_count,
        })
    }

    /// Binary search through the B-tree pages to find a key.
    fn find_key(&self, key: &[u8]) -> Option<(u64, u64)> {
        let data = &self.mmap[..];
        let btree_start = self.btree_root_offset as usize;
        let btree_end = self.blob_heap_offset as usize;

        // The B-tree is stored as a flat sorted array of entries across pages
        // We'll do a linear scan through pages, then binary search within each page
        // For simplicity, we store all entries in sorted order across pages

        let mut offset = btree_start;
        while offset < btree_end {
            // Read entry: key_len (4) + key + blob_offset (8) + blob_len (8)
            if offset + 4 > btree_end {
                break;
            }

            let key_len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;

            if offset + key_len + 16 > btree_end {
                break;
            }

            let entry_key = &data[offset..offset + key_len];
            offset += key_len;

            let blob_offset = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
            offset += 8;

            let blob_len = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
            offset += 8;

            match entry_key.cmp(key) {
                std::cmp::Ordering::Equal => return Some((blob_offset, blob_len)),
                std::cmp::Ordering::Greater => return None, // Sorted, so key doesn't exist
                std::cmp::Ordering::Less => continue,
            }
        }

        None
    }

    fn get_blob(&self, offset: u64, len: u64) -> Vec<u8> {
        let start = offset as usize;
        let end = start + len as usize;
        self.mmap[start..end].to_vec()
    }
}

impl BlobStore for BTreeDatStore {
    fn open(path: &Path) -> Result<Self> {
        let file = File::open(path).context("Failed to open B-tree dat file")?;
        let mmap = unsafe { Mmap::map(&file).context("Failed to mmap file")? };

        let header = Self::read_header(&mmap)?;

        Ok(Self {
            mmap,
            btree_root_offset: header.btree_root_offset,
            blob_heap_offset: header.blob_heap_offset,
            entry_count: header.entry_count as usize,
        })
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self
            .find_key(key)
            .map(|(offset, len)| self.get_blob(offset, len)))
    }

    fn keys(&self) -> Result<Vec<Vec<u8>>> {
        let data = &self.mmap[..];
        let btree_start = self.btree_root_offset as usize;
        let btree_end = self.blob_heap_offset as usize;

        let mut keys = Vec::with_capacity(self.entry_count);
        let mut offset = btree_start;

        while offset < btree_end {
            if offset + 4 > btree_end {
                break;
            }

            let key_len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;

            if offset + key_len + 16 > btree_end {
                break;
            }

            let entry_key = data[offset..offset + key_len].to_vec();
            keys.push(entry_key);

            offset += key_len + 16; // Skip key + blob_offset + blob_len
        }

        Ok(keys)
    }

    fn len(&self) -> usize {
        self.entry_count
    }

    fn backend_name() -> &'static str {
        "B-tree DAT"
    }
}

/// Builder for B-tree .dat store.
pub struct BTreeDatStoreBuilder {
    path: std::path::PathBuf,
    entries: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl BlobStoreBuilder for BTreeDatStoreBuilder {
    fn create(path: &Path) -> Result<Self> {
        Ok(Self {
            path: path.to_path_buf(),
            entries: BTreeMap::new(),
        })
    }

    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.entries.insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    fn finish(self) -> Result<()> {
        let file = File::create(&self.path).context("Failed to create B-tree dat file")?;
        let mut writer = BufWriter::new(file);

        // Reserve space for header
        writer.write_all(&[0u8; HEADER_SIZE])?;

        let btree_root_offset = HEADER_SIZE as u64;

        // Write entries in sorted order (BTreeMap maintains order)
        // First, we need to know blob offsets, so we'll compute them
        let mut btree_entries: Vec<(Vec<u8>, u64, u64)> = Vec::with_capacity(self.entries.len());

        // Calculate where blob heap will start
        let mut btree_size = 0usize;
        for (key, _value) in &self.entries {
            btree_size += 4 + key.len() + 8 + 8; // key_len + key + blob_offset + blob_len
        }

        let blob_heap_offset = btree_root_offset + btree_size as u64;
        let mut current_blob_offset = blob_heap_offset;

        // Compute blob offsets
        for (key, value) in &self.entries {
            btree_entries.push((key.clone(), current_blob_offset, value.len() as u64));
            current_blob_offset += value.len() as u64;
        }

        // Write B-tree entries
        for (key, blob_offset, blob_len) in &btree_entries {
            writer.write_all(&(key.len() as u32).to_le_bytes())?;
            writer.write_all(key)?;
            writer.write_all(&blob_offset.to_le_bytes())?;
            writer.write_all(&blob_len.to_le_bytes())?;
        }

        // Write blob heap
        for (_key, value) in &self.entries {
            writer.write_all(value)?;
        }

        // Go back and write header
        writer.seek(SeekFrom::Start(0))?;
        writer.write_all(MAGIC)?;
        writer.write_all(&btree_root_offset.to_le_bytes())?;
        writer.write_all(&blob_heap_offset.to_le_bytes())?;
        writer.write_all(&(self.entries.len() as u64).to_le_bytes())?;

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
    fn test_btree_roundtrip() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        // Build the store
        {
            let mut builder = BTreeDatStoreBuilder::create(path).unwrap();
            builder.insert(b"key1", b"value1").unwrap();
            builder.insert(b"key2", b"value2").unwrap();
            builder.insert(b"key3", b"value3").unwrap();
            builder.finish().unwrap();
        }

        // Read it back
        let store = BTreeDatStore::open(path).unwrap();

        assert_eq!(store.len(), 3);
        assert_eq!(store.get(b"key1").unwrap(), Some(b"value1".to_vec()));
        assert_eq!(store.get(b"key2").unwrap(), Some(b"value2".to_vec()));
        assert_eq!(store.get(b"key3").unwrap(), Some(b"value3".to_vec()));
        assert_eq!(store.get(b"nonexistent").unwrap(), None);
    }

    #[test]
    fn test_btree_keys_sorted() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        {
            let mut builder = BTreeDatStoreBuilder::create(path).unwrap();
            // Insert in random order
            builder.insert(b"zebra", b"z").unwrap();
            builder.insert(b"alpha", b"a").unwrap();
            builder.insert(b"middle", b"m").unwrap();
            builder.finish().unwrap();
        }

        let store = BTreeDatStore::open(path).unwrap();
        let keys = store.keys().unwrap();

        // Keys should be in sorted order due to BTreeMap
        assert_eq!(
            keys,
            vec![b"alpha".to_vec(), b"middle".to_vec(), b"zebra".to_vec()]
        );
    }

    #[test]
    fn test_btree_binary_data() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        let binary_key: Vec<u8> = (0..255).collect();
        let binary_value: Vec<u8> = (0..1000).map(|i| (i % 256) as u8).collect();

        {
            let mut builder = BTreeDatStoreBuilder::create(path).unwrap();
            builder.insert(&binary_key, &binary_value).unwrap();
            builder.finish().unwrap();
        }

        let store = BTreeDatStore::open(path).unwrap();
        assert_eq!(store.get(&binary_key).unwrap(), Some(binary_value));
    }

    #[test]
    fn test_btree_large_values() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        let large_value: Vec<u8> = (0..100_000).map(|i| (i % 256) as u8).collect();

        {
            let mut builder = BTreeDatStoreBuilder::create(path).unwrap();
            builder.insert(b"large", &large_value).unwrap();
            builder.finish().unwrap();
        }

        let store = BTreeDatStore::open(path).unwrap();
        assert_eq!(store.get(b"large").unwrap(), Some(large_value));
    }

    #[test]
    fn test_btree_empty_store() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        {
            let builder = BTreeDatStoreBuilder::create(path).unwrap();
            builder.finish().unwrap();
        }

        let store = BTreeDatStore::open(path).unwrap();
        assert_eq!(store.len(), 0);
        assert!(store.is_empty());
        assert_eq!(store.keys().unwrap(), Vec::<Vec<u8>>::new());
    }

    proptest! {
        #[test]
        fn prop_btree_roundtrip_single(key in prop_vec(any::<u8>(), 1..100), value in prop_vec(any::<u8>(), 0..1000)) {
            let temp_file = NamedTempFile::new().unwrap();
            let path = temp_file.path();

            {
                let mut builder = BTreeDatStoreBuilder::create(path).unwrap();
                builder.insert(&key, &value).unwrap();
                builder.finish().unwrap();
            }

            let store = BTreeDatStore::open(path).unwrap();
            prop_assert_eq!(store.len(), 1);
            prop_assert_eq!(store.get(&key).unwrap(), Some(value));
        }

        #[test]
        fn prop_btree_roundtrip_multiple(entries in prop_vec((prop_vec(any::<u8>(), 1..50), prop_vec(any::<u8>(), 0..500)), 1..50)) {
            let temp_file = NamedTempFile::new().unwrap();
            let path = temp_file.path();

            // Deduplicate keys (last value wins, but BTreeMap sorts)
            let mut expected: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
            for (key, value) in &entries {
                expected.insert(key.clone(), value.clone());
            }

            {
                let mut builder = BTreeDatStoreBuilder::create(path).unwrap();
                for (key, value) in expected.iter() {
                    builder.insert(key, value).unwrap();
                }
                builder.finish().unwrap();
            }

            let store = BTreeDatStore::open(path).unwrap();
            prop_assert_eq!(store.len(), expected.len());

            for (key, value) in &expected {
                prop_assert_eq!(store.get(key).unwrap(), Some(value.clone()));
            }
        }

        #[test]
        fn prop_btree_keys_always_sorted(entries in prop_vec((prop_vec(any::<u8>(), 1..30), prop_vec(any::<u8>(), 0..100)), 1..30)) {
            let temp_file = NamedTempFile::new().unwrap();
            let path = temp_file.path();

            let mut unique_keys: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
            for (key, value) in &entries {
                unique_keys.insert(key.clone(), value.clone());
            }

            {
                let mut builder = BTreeDatStoreBuilder::create(path).unwrap();
                for (key, value) in unique_keys.iter() {
                    builder.insert(key, value).unwrap();
                }
                builder.finish().unwrap();
            }

            let store = BTreeDatStore::open(path).unwrap();
            let keys = store.keys().unwrap();

            // Verify keys are sorted
            let mut sorted_keys = keys.clone();
            sorted_keys.sort();
            prop_assert_eq!(keys, sorted_keys);
        }

        #[test]
        fn prop_btree_missing_keys(
            stored_keys in prop_vec(prop_vec(any::<u8>(), 1..50), 1..20),
            missing_key in prop_vec(any::<u8>(), 1..50)
        ) {
            let temp_file = NamedTempFile::new().unwrap();
            let path = temp_file.path();

            {
                let mut builder = BTreeDatStoreBuilder::create(path).unwrap();
                for key in &stored_keys {
                    builder.insert(key, b"value").unwrap();
                }
                builder.finish().unwrap();
            }

            let store = BTreeDatStore::open(path).unwrap();

            if !stored_keys.contains(&missing_key) {
                prop_assert_eq!(store.get(&missing_key).unwrap(), None);
            }
        }
    }
}
