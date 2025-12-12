# building-an-index

Benchmark comparing different on-disk key/value index implementations.

## Usage

Generate data and build index files (defaults to `./data`):

```bash
cargo run --release -- build
```

Run benchmarks against the index files and generate charts (defaults to `./output`):

```bash
cargo run --release -- bench
```

## Notes

- `build` produces index artifacts like `data/index_sqlite_rowid.sqlite`, `data/index_hash.dat`, and `data/index.zip`, plus `data/keys.json`.
- `bench` performs random lookups and writes SVG charts to `output/`.
- Run `cargo run -- --help` (or `... -- build --help` / `... -- bench --help`) to see all options.
