# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

fb2sqlite is a Rust CLI tool that downloads a CSV from GS1 (Swiss product barcode registry), converts it to a SQLite database, and SCPs the result to a remote server. With `--migel`, it maps products to MiGeL (Mittel- und Gegenständeliste) codes and only outputs matched products.

## Build & Run

```bash
cargo build           # debug build
cargo build --release # release build
cargo run             # build and run (downloads CSV, creates firstbase.db, SCPs to remote)
cargo run -- --migel  # download CSV + MiGeL XLSX, map migel codes/limitations, output only matched products
cargo run -- --migel --local-csv  # use cached firstbase.csv instead of downloading from GS1
```

No tests are configured.

## Architecture

Two-file application with a producer/consumer pipeline:

- `src/main.rs` — CLI (`clap`), CSV download/parsing, parallel matching dispatch (`rayon`), SQLite writing (`mpsc` channel + thread), SCP upload
- `src/migel.rs` — MiGeL XLSX parsing (`calamine`), keyword extraction (multi-line + limitation text), word-level matching engine with per-language scoring

### Default mode

1. **Download** — fetches CSV from `https://id.gs1.ch/01/07612345000961` via `reqwest::blocking`
2. **Parse (producer)** — main thread reads CSV with the `csv` crate, sends rows (capped at 15 columns) through an `mpsc` channel
3. **SQLite (consumer)** — spawned thread receives rows, dynamically creates a `data` table from CSV headers (all TEXT columns), inserts rows in a single transaction via `rusqlite`
4. **SCP upload** — shells out to `scp` to transfer `firstbase.db` to `zdavatz@65.109.137.20:/var/www/pillbox.oddb.org/`

### --migel mode

1. Downloads MiGeL XLSX from BAG (3 language sheets: DE, FR, IT)
2. Parses items with position numbers, extracts keywords from full Bezeichnung text + Limitation text
3. Builds inverted keyword index for candidate finding
4. Matches each CSV product in parallel (rayon) using TradeItemDescription DE/FR/IT + BrandName
5. **Only matched products** are written to `firstbase_migel_dd.mm.yyyy.db` with added `migel_code`, `migel_bezeichnung`, `migel_limitation` columns

### Matching details (src/migel.rs)

- Per-language scoring: DE keywords scored against DE product text only, FR against FR, IT against IT
- German: compound word suffix matching + fuzzy inflection (e.g., "katheter" in "verweilkatheter")
- French/Italian: exact word matching only (prevents cross-type false positives)
- Secondary keywords (>= 8 chars from additional Bezeichnung lines): bonus matches gated by at least one primary keyword match
- Stop words filter generic cross-type terms (compression, ecarteur, system, etc.)
- Thresholds: 2+ keywords: score >= 0.3, max len >= 6; single keyword: score >= 0.5, len >= 10

## Key Dependencies

- `reqwest` (blocking) — HTTP download
- `csv` — CSV parsing
- `rusqlite` — SQLite database creation
- `calamine` — XLSX parsing (MiGeL)
- `rayon` — parallel matching across CPU cores
- `clap` — CLI argument parsing
- `chrono` — date/time formatting for output filename
