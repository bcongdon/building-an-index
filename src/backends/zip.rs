use crate::store::{BlobStore, BlobStoreBuilder};
use anyhow::{Context, Result};
use std::cell::RefCell;
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;
use zip::read::ZipArchive;
use zip::write::FileOptions;
use zip::ZipWriter;

/// Zip-based blob store.
/// Keys are stored as file names (hex-encoded), values as file contents.
pub struct ZipStore {
    archive: RefCell<ZipArchive<File>>,
    count: usize,
}

impl ZipStore {
    fn key_to_filename(key: &[u8]) -> String {
        hex::encode(key)
    }

    fn filename_to_key(filename: &str) -> Vec<u8> {
        hex::decode(filename).unwrap_or_default()
    }
}

impl BlobStore for ZipStore {
    fn open(path: &Path) -> Result<Self> {
        let file = File::open(path).context("Failed to open zip file")?;
        let archive = ZipArchive::new(file).context("Failed to read zip archive")?;
        let count = archive.len();

        Ok(Self {
            archive: RefCell::new(archive),
            count,
        })
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let filename = Self::key_to_filename(key);

        let mut archive = self.archive.borrow_mut();

        let result = match archive.by_name(&filename) {
            Ok(mut file) => {
                let mut contents = Vec::with_capacity(file.size() as usize);
                file.read_to_end(&mut contents)
                    .context("Failed to read file from zip")?;
                Ok(Some(contents))
            }
            Err(zip::result::ZipError::FileNotFound) => Ok(None),
            Err(e) => Err(e).context("Failed to find file in zip"),
        };
        result
    }

    fn keys(&self) -> Result<Vec<Vec<u8>>> {
        let mut keys = Vec::with_capacity(self.count);
        let archive = self.archive.borrow();

        for i in 0..self.count {
            let name = archive
                .name_for_index(i)
                .context("Failed to get filename")?;
            keys.push(Self::filename_to_key(name));
        }

        Ok(keys)
    }

    fn len(&self) -> usize {
        self.count
    }

    fn backend_name() -> &'static str {
        "Zip"
    }
}

/// Builder for zip blob store.
pub struct ZipStoreBuilder {
    writer: ZipWriter<File>,
    count: usize,
}

impl BlobStoreBuilder for ZipStoreBuilder {
    fn create(path: &Path) -> Result<Self> {
        let file = File::create(path).context("Failed to create zip file")?;
        let writer = ZipWriter::new(file);

        Ok(Self { writer, count: 0 })
    }

    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let filename = ZipStore::key_to_filename(key);

        let options = FileOptions::<()>::default()
            .compression_method(zip::CompressionMethod::Stored) // No compression for fair comparison
            .unix_permissions(0o644);

        self.writer
            .start_file(&filename, options)
            .context("Failed to start file in zip")?;

        self.writer
            .write_all(value)
            .context("Failed to write file to zip")?;

        self.count += 1;
        Ok(())
    }

    fn finish(self) -> Result<()> {
        self.writer.finish().context("Failed to finish zip")?;
        Ok(())
    }
}

// We need hex encoding for filenames
mod hex {
    const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";

    pub fn encode(data: &[u8]) -> String {
        let mut result = String::with_capacity(data.len() * 2);
        for byte in data {
            result.push(HEX_CHARS[(byte >> 4) as usize] as char);
            result.push(HEX_CHARS[(byte & 0xf) as usize] as char);
        }
        result
    }

    pub fn decode(s: &str) -> Option<Vec<u8>> {
        if s.len() % 2 != 0 {
            return None;
        }

        let mut result = Vec::with_capacity(s.len() / 2);
        let bytes = s.as_bytes();

        for chunk in bytes.chunks(2) {
            let high = hex_char_to_nibble(chunk[0])?;
            let low = hex_char_to_nibble(chunk[1])?;
            result.push((high << 4) | low);
        }

        Some(result)
    }

    fn hex_char_to_nibble(c: u8) -> Option<u8> {
        match c {
            b'0'..=b'9' => Some(c - b'0'),
            b'a'..=b'f' => Some(c - b'a' + 10),
            b'A'..=b'F' => Some(c - b'A' + 10),
            _ => None,
        }
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
    fn test_zip_roundtrip() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        // Build the store
        {
            let mut builder = ZipStoreBuilder::create(path).unwrap();
            builder.insert(b"key1", b"value1").unwrap();
            builder.insert(b"key2", b"value2").unwrap();
            builder.insert(b"key3", b"value3").unwrap();
            builder.finish().unwrap();
        }

        // Read it back
        let store = ZipStore::open(path).unwrap();

        assert_eq!(store.len(), 3);
        assert_eq!(store.get(b"key1").unwrap(), Some(b"value1".to_vec()));
        assert_eq!(store.get(b"key2").unwrap(), Some(b"value2".to_vec()));
        assert_eq!(store.get(b"key3").unwrap(), Some(b"value3".to_vec()));
        assert_eq!(store.get(b"nonexistent").unwrap(), None);
    }

    #[test]
    fn test_zip_keys() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        {
            let mut builder = ZipStoreBuilder::create(path).unwrap();
            builder.insert(b"alpha", b"a").unwrap();
            builder.insert(b"beta", b"b").unwrap();
            builder.finish().unwrap();
        }

        let store = ZipStore::open(path).unwrap();
        let mut keys = store.keys().unwrap();
        keys.sort();

        assert_eq!(keys, vec![b"alpha".to_vec(), b"beta".to_vec()]);
    }

    #[test]
    fn test_zip_binary_data() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        let binary_key: Vec<u8> = (0..255).collect();
        let binary_value: Vec<u8> = (0..1000).map(|i| (i % 256) as u8).collect();

        {
            let mut builder = ZipStoreBuilder::create(path).unwrap();
            builder.insert(&binary_key, &binary_value).unwrap();
            builder.finish().unwrap();
        }

        let store = ZipStore::open(path).unwrap();
        assert_eq!(store.get(&binary_key).unwrap(), Some(binary_value));
    }

    #[test]
    fn test_zip_large_values() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        let large_value: Vec<u8> = (0..100_000).map(|i| (i % 256) as u8).collect();

        {
            let mut builder = ZipStoreBuilder::create(path).unwrap();
            builder.insert(b"large", &large_value).unwrap();
            builder.finish().unwrap();
        }

        let store = ZipStore::open(path).unwrap();
        assert_eq!(store.get(b"large").unwrap(), Some(large_value));
    }

    #[test]
    fn test_zip_empty_store() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        {
            let builder = ZipStoreBuilder::create(path).unwrap();
            builder.finish().unwrap();
        }

        let store = ZipStore::open(path).unwrap();
        assert_eq!(store.len(), 0);
        assert!(store.is_empty());
    }

    #[test]
    fn test_hex_encode_decode() {
        let original = b"hello world";
        let encoded = hex::encode(original);
        let decoded = hex::decode(&encoded).unwrap();
        assert_eq!(decoded, original);
    }

    proptest! {
        #[test]
        fn prop_zip_roundtrip_single(key in prop_vec(any::<u8>(), 1..100), value in prop_vec(any::<u8>(), 0..1000)) {
            let temp_file = NamedTempFile::new().unwrap();
            let path = temp_file.path();

            {
                let mut builder = ZipStoreBuilder::create(path).unwrap();
                builder.insert(&key, &value).unwrap();
                builder.finish().unwrap();
            }

            let store = ZipStore::open(path).unwrap();
            prop_assert_eq!(store.len(), 1);
            prop_assert_eq!(store.get(&key).unwrap(), Some(value));
        }

        #[test]
        fn prop_zip_roundtrip_multiple(entries in prop_vec((prop_vec(any::<u8>(), 1..50), prop_vec(any::<u8>(), 0..500)), 1..30)) {
            let temp_file = NamedTempFile::new().unwrap();
            let path = temp_file.path();

            // Deduplicate keys
            let mut expected: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
            for (key, value) in &entries {
                expected.insert(key.clone(), value.clone());
            }

            {
                let mut builder = ZipStoreBuilder::create(path).unwrap();
                for (key, value) in expected.iter() {
                    builder.insert(key, value).unwrap();
                }
                builder.finish().unwrap();
            }

            let store = ZipStore::open(path).unwrap();
            prop_assert_eq!(store.len(), expected.len());

            for (key, value) in &expected {
                prop_assert_eq!(store.get(key).unwrap(), Some(value.clone()));
            }
        }

        #[test]
        fn prop_zip_missing_keys(
            stored_keys in prop_vec(prop_vec(any::<u8>(), 1..50), 1..20),
            missing_key in prop_vec(any::<u8>(), 1..50)
        ) {
            let temp_file = NamedTempFile::new().unwrap();
            let path = temp_file.path();

            // Deduplicate keys (zip can't have duplicate filenames)
            let unique_keys: std::collections::HashSet<_> = stored_keys.iter().cloned().collect();

            {
                let mut builder = ZipStoreBuilder::create(path).unwrap();
                for key in &unique_keys {
                    builder.insert(key, b"value").unwrap();
                }
                builder.finish().unwrap();
            }

            let store = ZipStore::open(path).unwrap();

            if !unique_keys.contains(&missing_key) {
                prop_assert_eq!(store.get(&missing_key).unwrap(), None);
            }
        }

        #[test]
        fn prop_hex_roundtrip(data in prop_vec(any::<u8>(), 0..200)) {
            let encoded = hex::encode(&data);
            let decoded = hex::decode(&encoded).unwrap();
            prop_assert_eq!(decoded, data);
        }
    }
}
