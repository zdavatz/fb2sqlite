# fb2sqlite

A Rust CLI tool that downloads product data from the [GS1 Switzerland](https://id.gs1.ch/) barcode registry as CSV, converts it to a SQLite database, and uploads it via SCP. Optionally maps products to [MiGeL](https://www.bag.admin.ch/) (Mittel- und Gegenständeliste) codes.

## Usage

```bash
cargo run                          # download CSV, create firstbase.db, SCP upload
cargo run -- --migel               # also download MiGeL XLSX and map codes to products
cargo run -- --migel --local-csv   # use cached firstbase.csv instead of downloading
```

### Default mode

1. Downloads the CSV from `https://id.gs1.ch/01/07612345000961`
2. Saves it as `firstbase.csv`
3. Converts it to `firstbase.db` (SQLite, all ~189K products)
4. SCPs the database to the remote server

### --migel mode

1. Downloads (or reads local) CSV from GS1
2. Downloads MiGeL XLSX from BAG (3 language sheets: DE, FR, IT)
3. Parses MiGeL items and builds a keyword index from Bezeichnung + Limitation text
4. Matches each product against MiGeL items using multi-language keyword scoring (DE, FR, IT product descriptions + BrandName)
5. **Only matched products** are written to `firstbase_migel_dd.mm.yyyy.db` (date-stamped) with added `migel_code`, `migel_bezeichnung`, `migel_limitation` columns
6. SCPs the database to the remote server

Matching uses parallel processing via [rayon](https://crates.io/crates/rayon) across all CPU cores.

### --local-csv

Use a previously downloaded `firstbase.csv` instead of fetching from GS1 (useful when the server is slow or unavailable).

## Build

```bash
cargo build --release
```

## Architecture

- `src/main.rs` — CLI args, CSV parsing, parallel matching dispatch, SQLite writing, SCP upload
- `src/migel.rs` — MiGeL XLSX parsing, keyword extraction, word-level matching engine

### MiGeL matching algorithm

- Keywords are extracted from MiGeL Bezeichnung (all lines) and Limitation text in DE/FR/IT
- Product descriptions are scored per-language against the same language's MiGeL keywords (prevents cross-language false positives)
- German: compound word suffix matching + fuzzy inflection (e.g., "katheter" in "verweilkatheter")
- French/Italian: exact word matching only
- Secondary keywords (long terms from additional Bezeichnung lines) provide bonus matches gated by at least one primary keyword match
- Stop words filter generic cross-type terms (e.g., "compression", "ecarteur", "system")

## Dependencies

- [reqwest](https://crates.io/crates/reqwest) — HTTP client (blocking)
- [csv](https://crates.io/crates/csv) — CSV parsing
- [rusqlite](https://crates.io/crates/rusqlite) — SQLite interface
- [calamine](https://crates.io/crates/calamine) — XLSX parsing (MiGeL)
- [rayon](https://crates.io/crates/rayon) — Parallel processing
- [clap](https://crates.io/crates/clap) — CLI argument parsing
- [chrono](https://crates.io/crates/chrono) — Date/time formatting
