use crate::data_gen::BlobSize;
use crate::store::BlobStore;
use anyhow::Result;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use std::io::{self, Write};
use std::time::{Duration, Instant};

/// Memory usage snapshot
#[derive(Debug, Clone, Default)]
pub struct MemoryStats {
    /// Physical memory used by the process in bytes
    pub physical_mem: usize,
    /// Virtual memory used by the process in bytes
    pub virtual_mem: usize,
}

impl MemoryStats {
    pub fn capture() -> Self {
        if let Some(usage) = memory_stats::memory_stats() {
            Self {
                physical_mem: usage.physical_mem,
                virtual_mem: usage.virtual_mem,
            }
        } else {
            Self::default()
        }
    }
}

/// Results from a single benchmark run
#[derive(Debug, Clone)]
pub struct BenchmarkResult {
    /// Name of the backend
    pub backend_name: String,
    /// Size category being benchmarked
    pub blob_size: BlobSize,
    /// All individual latencies in nanoseconds
    pub latencies_ns: Vec<u64>,
    /// File size on disk in bytes
    pub file_size: u64,
    /// Memory usage after opening the store
    pub memory_stats: MemoryStats,
}

impl BenchmarkResult {
    /// Calculate percentile latency (p is 0-100)
    pub fn percentile(&self, p: f64) -> Duration {
        if self.latencies_ns.is_empty() {
            return Duration::ZERO;
        }

        let mut sorted = self.latencies_ns.clone();
        sorted.sort_unstable();

        let idx = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
        Duration::from_nanos(sorted[idx])
    }

    pub fn p50(&self) -> Duration {
        self.percentile(50.0)
    }

    pub fn p90(&self) -> Duration {
        self.percentile(90.0)
    }

    pub fn p95(&self) -> Duration {
        self.percentile(95.0)
    }

    pub fn p99(&self) -> Duration {
        self.percentile(99.0)
    }

    pub fn min(&self) -> Duration {
        self.latencies_ns
            .iter()
            .min()
            .map(|&ns| Duration::from_nanos(ns))
            .unwrap_or(Duration::ZERO)
    }

    pub fn max(&self) -> Duration {
        self.latencies_ns
            .iter()
            .max()
            .map(|&ns| Duration::from_nanos(ns))
            .unwrap_or(Duration::ZERO)
    }

    pub fn mean(&self) -> Duration {
        if self.latencies_ns.is_empty() {
            return Duration::ZERO;
        }
        let sum: u64 = self.latencies_ns.iter().sum();
        Duration::from_nanos(sum / self.latencies_ns.len() as u64)
    }

    pub fn ops_per_second(&self) -> f64 {
        if self.latencies_ns.is_empty() {
            return 0.0;
        }
        let mean_ns = self.mean().as_nanos() as f64;
        if mean_ns > 0.0 {
            1_000_000_000.0 / mean_ns
        } else {
            0.0
        }
    }
}

/// Configuration for benchmark runs
#[derive(Debug, Clone)]
pub struct BenchmarkConfig {
    /// Number of random lookups to perform
    pub num_lookups: usize,
    /// Number of warmup iterations
    pub warmup_iterations: usize,
    /// Random seed for reproducibility
    pub seed: u64,
}

impl Default for BenchmarkConfig {
    fn default() -> Self {
        Self {
            num_lookups: 10_000,
            warmup_iterations: 1000,
            seed: 42,
        }
    }
}

/// Run a benchmark for a single backend
pub fn run_benchmark<S: BlobStore>(
    store: &S,
    keys: &[Vec<u8>],
    keys_by_size: &std::collections::HashMap<BlobSize, Vec<Vec<u8>>>,
    config: &BenchmarkConfig,
    file_size: u64,
) -> Result<Vec<BenchmarkResult>> {
    run_benchmark_with_logging(store, keys, keys_by_size, config, file_size, false)
}

/// Run a benchmark for a single backend with optional verbose logging
pub fn run_benchmark_with_logging<S: BlobStore>(
    store: &S,
    keys: &[Vec<u8>],
    keys_by_size: &std::collections::HashMap<BlobSize, Vec<Vec<u8>>>,
    config: &BenchmarkConfig,
    file_size: u64,
    verbose: bool,
) -> Result<Vec<BenchmarkResult>> {
    let mut rng = StdRng::seed_from_u64(config.seed);
    let mut results = Vec::new();

    let backend_name = S::backend_name();

    if verbose {
        println!("  [{}] Starting benchmark...", backend_name);
        println!("    File size: {:.2} MB", file_size as f64 / 1_048_576.0);
        println!("    Total keys available: {}", keys.len());
        println!();
    }

    // Warmup phase - access random keys to populate caches
    if verbose {
        print!("    Warmup: {} iterations... ", config.warmup_iterations);
        let _ = io::stdout().flush();
    }

    let warmup_start = Instant::now();
    for i in 0..config.warmup_iterations {
        if let Some(key) = keys.choose(&mut rng) {
            let _ = store.get(key)?;
        }
        // Progress indicator every 25%
        if verbose && config.warmup_iterations >= 100 && i % (config.warmup_iterations / 4) == 0 {
            print!("{}%.. ", (i * 100) / config.warmup_iterations);
            let _ = io::stdout().flush();
        }
    }
    let warmup_duration = warmup_start.elapsed();

    if verbose {
        println!("done ({:.2?})", warmup_duration);
    }

    // Capture memory stats after warmup
    let memory_stats = MemoryStats::capture();

    if verbose {
        println!(
            "    Memory after warmup: {:.2} MB (physical), {:.2} MB (virtual)",
            memory_stats.physical_mem as f64 / 1_048_576.0,
            memory_stats.virtual_mem as f64 / 1_048_576.0
        );
        println!();
    }

    // Benchmark each size category separately
    for &size in BlobSize::all() {
        if let Some(size_keys) = keys_by_size.get(&size) {
            if size_keys.is_empty() {
                if verbose {
                    println!("    [{}] Skipping (no keys)", size.name());
                }
                continue;
            }

            if verbose {
                print!(
                    "    [{}] Running {} lookups across {} keys... ",
                    size.name(),
                    config.num_lookups,
                    size_keys.len()
                );
                let _ = io::stdout().flush();
            }

            let mut latencies = Vec::with_capacity(config.num_lookups);
            let size_start = Instant::now();

            for i in 0..config.num_lookups {
                let key = size_keys.choose(&mut rng).unwrap();

                let start = Instant::now();
                let _ = store.get(key)?;
                let elapsed = start.elapsed();

                latencies.push(elapsed.as_nanos() as u64);

                // Progress indicator every 25% for verbose mode
                if verbose
                    && config.num_lookups >= 100
                    && i > 0
                    && i % (config.num_lookups / 4) == 0
                {
                    print!("{}%.. ", (i * 100) / config.num_lookups);
                    let _ = io::stdout().flush();
                }
            }

            let size_duration = size_start.elapsed();

            let result = BenchmarkResult {
                backend_name: backend_name.to_string(),
                blob_size: size,
                latencies_ns: latencies,
                file_size,
                memory_stats: memory_stats.clone(),
            };

            if verbose {
                println!("done ({:.2?})", size_duration);
                println!(
                    "      -> P50: {:?}, P95: {:?}, P99: {:?}",
                    result.p50(),
                    result.p95(),
                    result.p99()
                );
                println!(
                    "      -> Min: {:?}, Max: {:?}, Mean: {:?}",
                    result.min(),
                    result.max(),
                    result.mean()
                );
                println!(
                    "      -> Throughput: {:.0} ops/sec",
                    result.ops_per_second()
                );
            }

            results.push(result);
        }
    }

    if verbose {
        println!();
        println!("  [{}] Benchmark complete!", backend_name);
    }

    Ok(results)
}

/// Print benchmark results to console
pub fn print_results(results: &[BenchmarkResult]) {
    println!("\n{:=<80}", "");
    println!("Benchmark Results");
    println!("{:=<80}\n", "");

    // Group by backend
    let mut by_backend: std::collections::HashMap<&str, Vec<&BenchmarkResult>> =
        std::collections::HashMap::new();
    for result in results {
        by_backend
            .entry(&result.backend_name)
            .or_default()
            .push(result);
    }

    for (backend, backend_results) in &by_backend {
        println!("Backend: {}", backend);
        println!("{:-<60}", "");

        if let Some(first) = backend_results.first() {
            println!(
                "  File size: {:.2} MB",
                first.file_size as f64 / 1_048_576.0
            );
            println!(
                "  Memory (physical): {:.2} MB",
                first.memory_stats.physical_mem as f64 / 1_048_576.0
            );
        }

        println!(
            "\n  {:>8} {:>12} {:>12} {:>12} {:>12}",
            "Size", "P50", "P95", "P99", "Ops/sec"
        );
        println!("  {:-<60}", "");

        for result in backend_results.iter() {
            println!(
                "  {:>8} {:>12.2?} {:>12.2?} {:>12.2?} {:>12.0}",
                result.blob_size.name(),
                result.p50(),
                result.p95(),
                result.p99(),
                result.ops_per_second()
            );
        }
        println!();
    }
}

/// Aggregate results for comparison
#[derive(Debug)]
pub struct AggregateResults {
    pub results: Vec<BenchmarkResult>,
}

impl AggregateResults {
    pub fn new(results: Vec<BenchmarkResult>) -> Self {
        Self { results }
    }

    /// Get results grouped by backend name
    pub fn by_backend(&self) -> std::collections::HashMap<&str, Vec<&BenchmarkResult>> {
        let mut map = std::collections::HashMap::new();
        for result in &self.results {
            map.entry(result.backend_name.as_str())
                .or_insert_with(Vec::new)
                .push(result);
        }
        map
    }

    /// Get results grouped by blob size
    pub fn by_size(&self) -> std::collections::HashMap<BlobSize, Vec<&BenchmarkResult>> {
        let mut map = std::collections::HashMap::new();
        for result in &self.results {
            map.entry(result.blob_size)
                .or_insert_with(Vec::new)
                .push(result);
        }
        map
    }
}
