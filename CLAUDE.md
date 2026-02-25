# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

fb2sqlite is a Rust CLI tool that downloads a CSV from GS1 (Swiss product barcode registry), converts it to a SQLite database, and SCPs the result to a remote server.

## Build & Run

```bash
cargo build           # debug build
cargo build --release # release build
cargo run             # build and run (downloads CSV, creates firstbase.db, SCPs to remote)
cargo run -- --migel  # download CSV + MiGeL XLSX, map migel codes/limitations, output firstbase_migel_dd.mm.yyyy.db
```

No tests are configured.

## Architecture

Single-file application (`src/main.rs`) with a producer/consumer pipeline:

1. **Download** — fetches CSV from `https://id.gs1.ch/01/07612345000961` via `reqwest::blocking`
2. **Parse (producer)** — main thread reads CSV with the `csv` crate, sends rows (capped at 15 columns) through an `mpsc` channel
3. **SQLite (consumer)** — spawned thread receives rows, dynamically creates a `data` table from CSV headers (all TEXT columns), inserts rows in a single transaction via `rusqlite`
4. **SCP upload** — shells out to `scp` to transfer DB to `zdavatz@65.109.137.20:/var/www/pillbox.oddb.org/`

### --migel mode

When `--migel` is passed, the tool additionally:

1. Downloads MiGeL XLSX from BAG (Mittel- und Gegenständeliste)
2. Parses all 3 sheets (DE, FR, IT) to extract MiGeL items with position numbers
3. Builds a keyword index using normalized text from Bezeichnung + category hierarchy
4. Matches each CSV product (using TradeItemDescription DE/FR/IT + BrandName) to the best MiGeL item via substring keyword matching (handles German compound words)
5. Adds `migel_code`, `migel_bezeichnung`, `migel_limitation` columns to the output
6. Outputs to `firstbase_migel_dd.mm.yyyy.db` (date-stamped)

## Key Dependencies

- `reqwest` (blocking) — HTTP download
- `csv` — CSV parsing
- `rusqlite` — SQLite database creation
- `calamine` — XLSX parsing (MiGeL)
- `clap` — CLI argument parsing
- `chrono` — date/time formatting for output filename
