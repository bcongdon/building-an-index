use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rayon::prelude::*;
use std::io::{self, Write};
use std::sync::atomic::{AtomicUsize, Ordering};

/// Blob size categories for benchmarking
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BlobSize {
    /// ~100 bytes
    Tiny,
    /// ~1 KB
    Small,
    /// ~10 KB
    Medium,
    /// ~100 KB
    Large,
    /// ~1 MB
    Huge,
}

impl BlobSize {
    pub fn all() -> &'static [BlobSize] {
        &[
            BlobSize::Tiny,
            BlobSize::Small,
            BlobSize::Medium,
            BlobSize::Large,
            BlobSize::Huge,
        ]
    }

    pub fn byte_size(&self) -> usize {
        match self {
            BlobSize::Tiny => 100,
            BlobSize::Small => 1_024,
            BlobSize::Medium => 10_240,
            BlobSize::Large => 102_400,
            BlobSize::Huge => 1_048_576,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            BlobSize::Tiny => "100B",
            BlobSize::Small => "1KB",
            BlobSize::Medium => "10KB",
            BlobSize::Large => "100KB",
            BlobSize::Huge => "1MB",
        }
    }
}

/// Configuration for data generation
#[derive(Debug, Clone)]
pub struct DataGenConfig {
    /// Default number of entries per size category
    pub entries_per_size: usize,
    /// Override entries for specific sizes (e.g., fewer 1MB entries)
    pub entries_override: std::collections::HashMap<BlobSize, usize>,
    /// Random seed for reproducibility
    pub seed: u64,
}

impl DataGenConfig {
    /// Get the number of entries for a specific size
    pub fn entries_for_size(&self, size: BlobSize) -> usize {
        self.entries_override
            .get(&size)
            .copied()
            .unwrap_or(self.entries_per_size)
    }
}

impl Default for DataGenConfig {
    fn default() -> Self {
        let mut entries_override = std::collections::HashMap::new();
        // Default to fewer huge entries since they're slow to generate
        entries_override.insert(BlobSize::Huge, 100);

        Self {
            entries_per_size: 1_000,
            entries_override,
            seed: 42,
        }
    }
}

/// A generated key-value entry
#[derive(Debug, Clone)]
pub struct Entry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub size_category: BlobSize,
}

/// Progress information for data generation
#[derive(Debug, Clone)]
pub struct Progress {
    /// Current size category being generated
    pub current_size: BlobSize,
    /// Current entry index within the size category
    pub current_entry: usize,
    /// Total entries per size category
    pub entries_per_size: usize,
    /// Current size category index (0-based)
    pub size_index: usize,
    /// Total number of size categories
    pub total_sizes: usize,
    /// Total bytes generated so far
    pub bytes_generated: usize,
    /// Estimated total bytes
    pub total_bytes: usize,
}

impl Progress {
    /// Get overall progress as a percentage (0.0 - 100.0)
    pub fn percent(&self) -> f64 {
        let total_entries = self.total_sizes * self.entries_per_size;
        let current = self.size_index * self.entries_per_size + self.current_entry;
        (current as f64 / total_entries as f64) * 100.0
    }
}

/// Data generator for benchmarking
pub struct DataGenerator {
    config: DataGenConfig,
}

impl DataGenerator {
    pub fn new(config: DataGenConfig) -> Self {
        Self { config }
    }

    /// Generate a key for a given size category and index (deterministic based on seed + index)
    fn generate_key(seed: u64, size: BlobSize, index: usize) -> Vec<u8> {
        let mut rng = StdRng::seed_from_u64(seed.wrapping_add(index as u64));
        format!("{}_{:08}_{:016x}", size.name(), index, rng.gen::<u64>()).into_bytes()
    }

    /// Generate random blob data of the specified size (deterministic based on seed + index)
    fn generate_value(seed: u64, size: BlobSize, index: usize) -> Vec<u8> {
        // Use a different seed offset for value to avoid correlation with key
        let mut rng = StdRng::seed_from_u64(
            seed.wrapping_add(index as u64)
                .wrapping_add(0x1234567890abcdef),
        );
        let byte_size = size.byte_size();
        let mut data = vec![0u8; byte_size];
        rng.fill(&mut data[..]);
        data
    }

    /// Generate a single entry (can be called in parallel)
    fn generate_entry(seed: u64, size: BlobSize, index: usize) -> Entry {
        Entry {
            key: Self::generate_key(seed, size, index),
            value: Self::generate_value(seed, size, index),
            size_category: size,
        }
    }

    /// Generate all entries for benchmarking (parallel)
    pub fn generate_all(&self) -> Vec<Entry> {
        let total_entries: usize = BlobSize::all()
            .iter()
            .map(|&s| self.config.entries_for_size(s))
            .sum();

        let mut entries = Vec::with_capacity(total_entries);

        for &size in BlobSize::all() {
            let count = self.config.entries_for_size(size);
            let seed = self.config.seed;

            // Generate entries for this size in parallel
            let size_entries: Vec<Entry> = (0..count)
                .into_par_iter()
                .map(|i| Self::generate_entry(seed, size, i))
                .collect();

            entries.extend(size_entries);
        }

        entries
    }

    /// Generate all entries with console progress logging (parallel)
    pub fn generate_all_with_logging(&self) -> Vec<Entry> {
        let total_entries: usize = BlobSize::all()
            .iter()
            .map(|&s| self.config.entries_for_size(s))
            .sum();
        let total_bytes = estimate_total_size(&self.config);

        println!(
            "Generating {} entries across {} size categories (parallel)...",
            total_entries,
            BlobSize::all().len()
        );
        println!(
            "Estimated total size: {:.2} MB",
            total_bytes as f64 / 1_048_576.0
        );
        println!();

        let mut all_entries = Vec::with_capacity(total_entries);
        let mut bytes_generated = 0usize;

        for &size in BlobSize::all() {
            let count = self.config.entries_for_size(size);
            let seed = self.config.seed;

            print!("  Generating {} blobs ({} each)... ", size.name(), count);
            let _ = io::stdout().flush();

            // Track progress with atomic counter
            let progress_counter = AtomicUsize::new(0);
            let total = count;

            // Generate entries in parallel
            let entries: Vec<Entry> = (0..count)
                .into_par_iter()
                .map(|i| {
                    let entry = Self::generate_entry(seed, size, i);

                    // Update progress counter
                    let done = progress_counter.fetch_add(1, Ordering::Relaxed) + 1;

                    // Print progress every 10% for large batches
                    if total >= 100 && done % (total / 10) == 0 {
                        eprint!("{}%.. ", (done * 100) / total);
                    }

                    entry
                })
                .collect();

            let size_bytes = count * size.byte_size();
            bytes_generated += size_bytes;

            println!("done ({:.2} MB)", size_bytes as f64 / 1_048_576.0);

            all_entries.extend(entries);
        }

        println!();
        println!(
            "Generated {:.2} MB total",
            bytes_generated as f64 / 1_048_576.0
        );

        all_entries
    }

    /// Generate entries for a specific size category (parallel)
    pub fn generate_for_size(&self, size: BlobSize) -> Vec<Entry> {
        let count = self.config.entries_for_size(size);
        let seed = self.config.seed;

        (0..count)
            .into_par_iter()
            .map(|i| Self::generate_entry(seed, size, i))
            .collect()
    }
}

/// Estimate total data size in bytes
pub fn estimate_total_size(config: &DataGenConfig) -> usize {
    BlobSize::all()
        .iter()
        .map(|&s| s.byte_size() * config.entries_for_size(s))
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_generation() {
        let config = DataGenConfig {
            entries_per_size: 10,
            entries_override: std::collections::HashMap::new(),
            seed: 42,
        };
        let gen = DataGenerator::new(config);
        let entries = gen.generate_all();

        assert_eq!(entries.len(), 50); // 5 sizes * 10 entries

        // Verify each size category has correct number of entries
        for size in BlobSize::all() {
            let count = entries.iter().filter(|e| e.size_category == *size).count();
            assert_eq!(count, 10);

            // Verify value sizes
            for entry in entries.iter().filter(|e| e.size_category == *size) {
                assert_eq!(entry.value.len(), size.byte_size());
            }
        }
    }

    #[test]
    fn test_reproducibility() {
        let config = DataGenConfig {
            entries_per_size: 5,
            entries_override: std::collections::HashMap::new(),
            seed: 123,
        };

        let gen1 = DataGenerator::new(config.clone());
        let gen2 = DataGenerator::new(config);

        let entries1 = gen1.generate_all();
        let entries2 = gen2.generate_all();

        for (e1, e2) in entries1.iter().zip(entries2.iter()) {
            assert_eq!(e1.key, e2.key);
            assert_eq!(e1.value, e2.value);
        }
    }

    #[test]
    fn test_entries_override() {
        let mut entries_override = std::collections::HashMap::new();
        entries_override.insert(BlobSize::Huge, 5);
        entries_override.insert(BlobSize::Large, 20);

        let config = DataGenConfig {
            entries_per_size: 10,
            entries_override,
            seed: 42,
        };
        let gen = DataGenerator::new(config);
        let entries = gen.generate_all();

        // Should have: 10 tiny + 10 small + 10 medium + 20 large + 5 huge = 55
        assert_eq!(entries.len(), 55);

        assert_eq!(
            entries
                .iter()
                .filter(|e| e.size_category == BlobSize::Huge)
                .count(),
            5
        );
        assert_eq!(
            entries
                .iter()
                .filter(|e| e.size_category == BlobSize::Large)
                .count(),
            20
        );
    }
}
