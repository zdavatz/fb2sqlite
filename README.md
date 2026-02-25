# fb2sqlite

A Rust CLI tool that downloads product data from the [GS1 Switzerland](https://id.gs1.ch/) barcode registry as CSV, converts it to a SQLite database, and uploads it via SCP.

## Usage

```bash
cargo run
```

This will:
1. Download the CSV from `https://id.gs1.ch/01/07612345000961`
2. Save it as `firstbase.csv`
3. Convert it to `firstbase.db` (SQLite)
4. SCP the database to the remote server

## Build

```bash
cargo build --release
```

## Dependencies

- [reqwest](https://crates.io/crates/reqwest) — HTTP client
- [csv](https://crates.io/crates/csv) — CSV parsing
- [rusqlite](https://crates.io/crates/rusqlite) — SQLite interface
- [chrono](https://crates.io/crates/chrono) — Date/time handling
