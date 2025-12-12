use anyhow::{Context, Result};
use build_an_index::backends::{
    HashDatStore, HashDatStoreBuilder, SqliteRowidStore, SqliteRowidStoreBuilder,
    SqliteWithoutRowidStore, SqliteWithoutRowidStoreBuilder, ZipStore, ZipStoreBuilder,
};
use build_an_index::benchmark::{
    print_results, run_benchmark_with_logging, AggregateResults, BenchmarkConfig,
};
use build_an_index::chart::generate_charts;
use build_an_index::data_gen::{BlobSize, DataGenConfig, DataGenerator};
use build_an_index::store::{BlobStore, BlobStoreBuilder};
use clap::{Parser, Subcommand};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

#[derive(Parser)]
#[command(name = "build-an-index")]
#[command(about = "Benchmark comparing on-disk key-value index implementations")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Build index files from generated data
    Build {
        /// Output directory for index files
        #[arg(short, long, default_value = "./data")]
        output: PathBuf,

        /// Number of entries per blob size category
        #[arg(short, long, default_value = "1000")]
        entries: usize,

        /// Random seed for data generation
        #[arg(short, long, default_value = "42")]
        seed: u64,
    },

    /// Run benchmarks on existing index files
    Bench {
        /// Directory containing index files
        #[arg(short, long, default_value = "./data")]
        input: PathBuf,

        /// Output directory for charts
        #[arg(short, long, default_value = "./output")]
        output: PathBuf,

        /// Number of random lookups per size category
        #[arg(short, long, default_value = "10000")]
        lookups: usize,

        /// Random seed for benchmark
        #[arg(short, long, default_value = "42")]
        seed: u64,

        /// Enable verbose logging during benchmark
        #[arg(short, long, default_value = "false")]
        verbose: bool,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Build {
            output,
            entries,
            seed,
        } => {
            build_indices(&output, entries, seed)?;
        }
        Commands::Bench {
            input,
            output,
            lookups,
            seed,
            verbose,
        } => {
            run_benchmarks(&input, &output, lookups, seed, verbose)?;
        }
    }

    Ok(())
}

fn build_indices(output_dir: &Path, entries_per_size: usize, seed: u64) -> Result<()> {
    std::fs::create_dir_all(output_dir).context("Failed to create output directory")?;

    // Use fewer entries for huge blobs (1MB) to speed up generation
    let mut entries_override = std::collections::HashMap::new();
    entries_override.insert(BlobSize::Huge, std::cmp::min(entries_per_size, 100));

    let config = DataGenConfig {
        entries_per_size,
        entries_override,
        seed,
    };

    let generator = DataGenerator::new(config.clone());
    let entries = generator.generate_all_with_logging();

    // Build SQLite indices
    println!("\nBuilding SQLite index (WITHOUT ROWID)...");
    let sqlite_without_rowid_path = output_dir.join("index_sqlite_without_rowid.sqlite");
    build_store::<SqliteWithoutRowidStoreBuilder>(&sqlite_without_rowid_path, &entries)?;
    println!(
        "  Created: {} ({:.2} MB)",
        sqlite_without_rowid_path.display(),
        file_size_mb(&sqlite_without_rowid_path)?
    );
    verify_store::<SqliteWithoutRowidStore>(&sqlite_without_rowid_path, &entries)?;

    println!("\nBuilding SQLite index (ROWID)...");
    let sqlite_rowid_path = output_dir.join("index_sqlite_rowid.sqlite");
    build_store::<SqliteRowidStoreBuilder>(&sqlite_rowid_path, &entries)?;
    println!(
        "  Created: {} ({:.2} MB)",
        sqlite_rowid_path.display(),
        file_size_mb(&sqlite_rowid_path)?
    );
    verify_store::<SqliteRowidStore>(&sqlite_rowid_path, &entries)?;

    // Build Hash DAT index
    println!("\nBuilding Hash DAT index...");
    let hash_path = output_dir.join("index_hash.dat");
    build_store::<HashDatStoreBuilder>(&hash_path, &entries)?;
    println!(
        "  Created: {} ({:.2} MB)",
        hash_path.display(),
        file_size_mb(&hash_path)?
    );
    verify_store::<HashDatStore>(&hash_path, &entries)?;

    // Build Zip index
    println!("\nBuilding Zip index...");
    let zip_path = output_dir.join("index.zip");
    build_store::<ZipStoreBuilder>(&zip_path, &entries)?;
    println!(
        "  Created: {} ({:.2} MB)",
        zip_path.display(),
        file_size_mb(&zip_path)?
    );
    verify_store::<ZipStore>(&zip_path, &entries)?;

    // Save keys for benchmarking
    println!("\nSaving key index...");
    let keys_path = output_dir.join("keys.json");
    let keys_by_size: HashMap<String, Vec<String>> = BlobSize::all()
        .iter()
        .map(|size| {
            let size_keys: Vec<String> = entries
                .iter()
                .filter(|e| e.size_category == *size)
                .map(|e| base64_encode(&e.key))
                .collect();
            (size.name().to_string(), size_keys)
        })
        .collect();

    let keys_json = serde_json::to_string_pretty(&keys_by_size)?;
    std::fs::write(&keys_path, keys_json)?;
    println!("  Created: {}", keys_path.display());

    println!("\nBuild complete!");
    Ok(())
}

fn build_store<B: BlobStoreBuilder>(
    path: &Path,
    entries: &[build_an_index::data_gen::Entry],
) -> Result<()> {
    let mut builder = B::create(path)?;
    for entry in entries {
        builder.insert(&entry.key, &entry.value)?;
    }
    builder.finish()?;
    Ok(())
}

fn file_size_mb(path: &Path) -> Result<f64> {
    let metadata = std::fs::metadata(path)?;
    Ok(metadata.len() as f64 / 1_048_576.0)
}

/// Verify that all entries can be read back correctly from a store
fn verify_store<S: BlobStore>(
    path: &Path,
    entries: &[build_an_index::data_gen::Entry],
) -> Result<()> {
    use std::io::Write;

    print!("  Verifying {} entries... ", entries.len());
    std::io::stdout().flush()?;

    let store = S::open(path)?;

    // Verify entry count
    if store.len() != entries.len() {
        anyhow::bail!(
            "Entry count mismatch: expected {}, got {}",
            entries.len(),
            store.len()
        );
    }

    // Verify each entry can be retrieved with correct value
    let mut errors = 0;
    let mut verified = 0;
    let total = entries.len();
    let check_interval = (total / 10).max(1);

    for entry in entries.iter() {
        match store.get(&entry.key)? {
            Some(value) => {
                if value != entry.value {
                    if errors < 5 {
                        eprintln!(
                            "\n    Value mismatch for key {:?}: expected {} bytes, got {} bytes",
                            String::from_utf8_lossy(&entry.key[..entry.key.len().min(32)]),
                            entry.value.len(),
                            value.len()
                        );
                    }
                    errors += 1;
                }
            }
            None => {
                if errors < 5 {
                    eprintln!(
                        "\n    Missing key: {:?}",
                        String::from_utf8_lossy(&entry.key[..entry.key.len().min(32)])
                    );
                }
                errors += 1;
            }
        }
        verified += 1;

        // Progress indicator
        if verified % check_interval == 0 {
            print!("{}%.. ", (verified * 100) / total);
            std::io::stdout().flush()?;
        }
    }

    if errors > 0 {
        println!("FAILED");
        anyhow::bail!("{} verification errors out of {} entries", errors, total);
    }

    println!("OK");
    Ok(())
}

fn run_benchmarks(
    input_dir: &Path,
    output_dir: &Path,
    num_lookups: usize,
    seed: u64,
    verbose: bool,
) -> Result<()> {
    // Load keys
    let keys_path = input_dir.join("keys.json");
    let keys_json = std::fs::read_to_string(&keys_path)
        .context("Failed to read keys.json. Did you run 'build' first?")?;
    let keys_by_size_str: HashMap<String, Vec<String>> = serde_json::from_str(&keys_json)?;

    let keys_by_size: HashMap<BlobSize, Vec<Vec<u8>>> = BlobSize::all()
        .iter()
        .map(|size| {
            let keys = keys_by_size_str
                .get(size.name())
                .map(|ks| ks.iter().filter_map(|k| base64_decode(k)).collect())
                .unwrap_or_default();
            (*size, keys)
        })
        .collect();

    let all_keys: Vec<Vec<u8>> = keys_by_size.values().flatten().cloned().collect();

    println!("\nBenchmark Configuration:");
    println!("  Lookups per size: {}", num_lookups);
    println!("  Warmup iterations: 1000");
    println!("  Random seed: {}", seed);
    println!("  Total keys loaded: {}", all_keys.len());
    for size in BlobSize::all() {
        if let Some(keys) = keys_by_size.get(size) {
            println!("    {}: {} keys", size.name(), keys.len());
        }
    }

    let config = BenchmarkConfig {
        num_lookups,
        warmup_iterations: 1000,
        seed,
    };

    let mut all_results = Vec::new();

    // Benchmark SQLite (WITHOUT ROWID)
    println!("\nBenchmarking SQLite (WITHOUT ROWID)...");
    let sqlite_without_rowid_path = input_dir.join("index_sqlite_without_rowid.sqlite");
    if sqlite_without_rowid_path.exists() {
        let results = benchmark_store::<SqliteWithoutRowidStore>(
            &sqlite_without_rowid_path,
            &all_keys,
            &keys_by_size,
            &config,
            verbose,
        )?;
        all_results.extend(results);
    } else {
        println!("  Skipped (file not found)");
    }

    // Benchmark SQLite (ROWID)
    println!("\nBenchmarking SQLite (ROWID)...");
    let sqlite_rowid_path = input_dir.join("index_sqlite_rowid.sqlite");
    if sqlite_rowid_path.exists() {
        let results = benchmark_store::<SqliteRowidStore>(
            &sqlite_rowid_path,
            &all_keys,
            &keys_by_size,
            &config,
            verbose,
        )?;
        all_results.extend(results);
    } else {
        println!("  Skipped (file not found)");
    }

    // Benchmark Hash DAT
    println!("\nBenchmarking Hash DAT...");
    let hash_path = input_dir.join("index_hash.dat");
    if hash_path.exists() {
        let results = benchmark_store::<HashDatStore>(
            &hash_path,
            &all_keys,
            &keys_by_size,
            &config,
            verbose,
        )?;
        all_results.extend(results);
    } else {
        println!("  Skipped (file not found)");
    }

    // Benchmark Zip
    println!("\nBenchmarking Zip...");
    let zip_path = input_dir.join("index.zip");
    if zip_path.exists() {
        let results =
            benchmark_store::<ZipStore>(&zip_path, &all_keys, &keys_by_size, &config, verbose)?;
        all_results.extend(results);
    } else {
        println!("  Skipped (file not found)");
    }

    // Print results
    print_results(&all_results);

    // Generate charts
    println!("\nGenerating charts...");
    let aggregate = AggregateResults::new(all_results);
    generate_charts(&aggregate, output_dir)?;

    println!("\nBenchmark complete!");
    Ok(())
}

fn benchmark_store<S: BlobStore>(
    path: &Path,
    all_keys: &[Vec<u8>],
    keys_by_size: &HashMap<BlobSize, Vec<Vec<u8>>>,
    config: &BenchmarkConfig,
    verbose: bool,
) -> Result<Vec<build_an_index::benchmark::BenchmarkResult>> {
    let store = S::open(path)?;
    let file_size = std::fs::metadata(path)?.len();
    run_benchmark_with_logging(&store, all_keys, keys_by_size, config, file_size, verbose)
}

// Simple base64 encoding for storing keys in JSON
fn base64_encode(data: &[u8]) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    let mut result = String::new();
    let mut i = 0;

    while i < data.len() {
        let b0 = data[i];
        let b1 = if i + 1 < data.len() { data[i + 1] } else { 0 };
        let b2 = if i + 2 < data.len() { data[i + 2] } else { 0 };

        result.push(ALPHABET[(b0 >> 2) as usize] as char);
        result.push(ALPHABET[(((b0 & 0x03) << 4) | (b1 >> 4)) as usize] as char);

        if i + 1 < data.len() {
            result.push(ALPHABET[(((b1 & 0x0f) << 2) | (b2 >> 6)) as usize] as char);
        } else {
            result.push('=');
        }

        if i + 2 < data.len() {
            result.push(ALPHABET[(b2 & 0x3f) as usize] as char);
        } else {
            result.push('=');
        }

        i += 3;
    }

    result
}

fn base64_decode(s: &str) -> Option<Vec<u8>> {
    fn char_to_val(c: char) -> Option<u8> {
        match c {
            'A'..='Z' => Some(c as u8 - b'A'),
            'a'..='z' => Some(c as u8 - b'a' + 26),
            '0'..='9' => Some(c as u8 - b'0' + 52),
            '+' => Some(62),
            '/' => Some(63),
            '=' => Some(0),
            _ => None,
        }
    }

    let chars: Vec<char> = s.chars().collect();
    if chars.len() % 4 != 0 {
        return None;
    }

    let mut result = Vec::new();

    for chunk in chars.chunks(4) {
        let v0 = char_to_val(chunk[0])?;
        let v1 = char_to_val(chunk[1])?;
        let v2 = char_to_val(chunk[2])?;
        let v3 = char_to_val(chunk[3])?;

        result.push((v0 << 2) | (v1 >> 4));

        if chunk[2] != '=' {
            result.push((v1 << 4) | (v2 >> 2));
        }

        if chunk[3] != '=' {
            result.push((v2 << 6) | v3);
        }
    }

    Some(result)
}
