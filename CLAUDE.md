# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

fb2sqlite is a Rust CLI tool that downloads a CSV from GS1 (Swiss product barcode registry), converts it to a SQLite database, and SCPs the result to a remote server. With `--migel`, it maps products to MiGeL (Mittel- und Gegenständeliste) codes and only outputs matched products.

## Build & Run

```bash
cargo build           # debug build
cargo build --release # release build
cargo run             # build and run (downloads CSV, creates firstbase.db, SCPs to remote)
cargo run -- --migel  # download CSV + MiGeL XLSX, map migel codes/limitations, save as firstbase_migel_dd.mm.yyyy.db locally
cargo run -- --migel --deploy  # same as --migel but saves as firstbase_migel.db and SCPs to remote server
cargo run -- --migel --local-csv  # use cached firstbase.csv instead of downloading from GS1
```

No tests are configured.

## Architecture

Two-file application with a producer/consumer pipeline:

- `src/main.rs` — CLI (`clap`), CSV download/parsing, parallel matching dispatch (`rayon`), SQLite writing (`mpsc` channel + thread), SCP upload
- `src/migel.rs` — MiGeL XLSX parsing (`calamine`), keyword extraction, Aho-Corasick candidate finding, IDF-weighted word-level matching engine with per-language scoring

### Default mode

1. **Download** — fetches CSV from `https://id.gs1.ch/01/07612345000961` via `reqwest::blocking`
2. **Parse (producer)** — main thread reads CSV with the `csv` crate, sends rows (capped at 15 columns) through an `mpsc` channel
3. **SQLite (consumer)** — spawned thread receives rows, dynamically creates a `data` table from CSV headers (all TEXT columns), inserts rows in a single transaction via `rusqlite`
4. **SCP upload** — shells out to `scp` to transfer `firstbase.db` to `zdavatz@65.109.137.20:/var/www/pillbox.oddb.org/`

### --migel mode

1. Downloads MiGeL XLSX from BAG (skips if `migel.xlsx` already exists locally)
2. Parses items with position numbers, extracts keywords from full Bezeichnung text + Limitation text + category hierarchy
3. Builds Aho-Corasick automaton + IDF weights (capped at 5.0) for candidate finding and ranking
4. Matches each CSV product in parallel (rayon) using TradeItemDescription DE/FR/IT + BrandName
5. **Only matched products** are written to SQLite with added `migel_code`, `migel_bezeichnung`, `migel_limitation` columns
6. Without `--deploy`: saves as `firstbase_migel_dd.mm.yyyy.db` locally (no SCP). With `--deploy`: saves as `firstbase_migel.db` and SCPs to remote

### Matching details (src/migel.rs)

- **Aho-Corasick candidate finding**: single-pass scan of combined DE+FR+IT text finds all matching keywords (including fuzzy truncated variants)
- **Per-language scoring**: DE keywords scored against DE product text only, FR against FR, IT against IT
- **English-only detection**: if DE/FR/IT fields are identical, FR/IT scoring is skipped to prevent cross-language false positives
- **IDF-weighted ranking**: keywords weighted by inverse document frequency (capped at 5.0) for choosing the best match among passing candidates
- **Category hierarchy keywords**: parent category text from XLSX hierarchy (>= 8 chars) boosts IDF ranking (0.5 weight) but does NOT count toward match thresholds
- German: compound word suffix matching + fuzzy inflection >= 6 chars (e.g., "binde" matches "binden", "katheter" in "verweilkatheter") + compound prefix decomposition via whitelist (e.g., "blasen" from "blasenkatheter")
- French/Italian: exact word matching only (prevents cross-type false positives)
- Secondary keywords (>= 8 chars from additional Bezeichnung lines): bonus matches gated by at least one primary keyword match
- **Bidirectional coverage**: rewards matches where keywords cover a large fraction of product text (short names rank higher than verbose descriptions)
- **Phrase matching**: exact MiGeL Bezeichnung (>= 8 chars) substring match in product text gets 1.0 ranking boost
- **Length penalty**: verbose DE descriptions (15+ significant words) require single-keyword score >= 0.7
- **Company exclusions**: surgical implant (JJHCS/DePuy Synthes, Waldemar Link, Mathys) and lab diagnostics (Roche) companies excluded — near-100% false positive rate
- **Stop words**: filter generic cross-category terms (dimensions, anatomical terms, generic device types, FR/IT generic terms)
- **Universal exclusions**: block interventional/surgical devices (PTA, stent, ERCP, ablation, ureteral, etc.) from all MiGeL matching
- **Negative keywords**: per MiGeL code prefix exclusions (orthesis body-part, dressing types, catheter-vs-handle, surgical instruments)
- **Unicode NFC normalization** + uppercase accent handling for FR/IT text
- Thresholds: 2+ keywords: score >= 0.3, max len >= 6; single keyword: score >= 0.5, len >= 8 (>= 0.7 for verbose)
- Note: ~480 of 786 MiGeL codes have no matches (capital equipment, services, or very specific subcategories not in GS1)

## Key Dependencies

- `reqwest` (blocking) — HTTP download
- `csv` — CSV parsing
- `rusqlite` — SQLite database creation
- `calamine` — XLSX parsing (MiGeL)
- `rayon` — parallel matching across CPU cores
- `clap` — CLI argument parsing
- `chrono` — date/time formatting for output filename
- `aho-corasick` — multi-pattern string matching for fast candidate finding
- `unicode-normalization` — Unicode NFC normalization for accent handling
