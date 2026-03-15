# fb2sqlite

A Rust CLI tool that downloads product data from the [GS1 Switzerland](https://id.gs1.ch/) barcode registry as CSV, converts it to a SQLite database, and uploads it via SCP. Optionally maps products to [MiGeL](https://www.bag.admin.ch/) (Mittel- und Gegenständeliste) codes.

## Usage

```bash
cargo run                                  # download CSV, create firstbase.db, SCP upload
cargo run -- --migel                       # download CSV + MiGeL XLSX, save firstbase_migel_dd.mm.yyyy.db locally
cargo run -- --migel --deploy              # same as --migel but saves as firstbase_migel.db and SCPs to remote
cargo run -- --migel --local-csv           # use cached firstbase.csv instead of downloading
cargo run -- --migel --deploy --local-csv  # deploy with cached CSV
```

### Default mode

1. Downloads the CSV from `https://id.gs1.ch/01/07612345000961`
2. Saves it as `firstbase.csv`
3. Converts it to `firstbase.db` (SQLite, all ~189K products)
4. SCPs the database to the remote server

### --migel mode

1. Downloads (or reads local) CSV from GS1
2. Downloads MiGeL XLSX from BAG (skips download if `migel.xlsx` already exists locally)
3. Parses MiGeL items (786 items with position numbers across 3 language sheets: DE, FR, IT)
4. Builds an Aho-Corasick automaton from all keywords for fast candidate finding
5. Matches each product against MiGeL items in parallel using IDF-weighted multi-language keyword scoring
6. **Only matched products** are written to SQLite with added `migel_code`, `migel_bezeichnung`, `migel_limitation` columns
7. Without `--deploy`: saves as `firstbase_migel_dd.mm.yyyy.db` (date-stamped) locally. With `--deploy`: saves as `firstbase_migel.db` and SCPs to remote server

### --deploy

When used with `--migel`, names the output file `firstbase_migel.db` (without date stamp) and uploads it to the remote server via SCP. Without `--deploy`, the file is saved locally with a date-stamped name and no upload occurs.

Matching uses parallel processing via [rayon](https://crates.io/crates/rayon) across all CPU cores.

### --local-csv

Use a previously downloaded `firstbase.csv` instead of fetching from GS1 (useful when the server is slow or unavailable).

## Build

```bash
cargo build --release
```

## Architecture

- `src/main.rs` — CLI args, CSV parsing, parallel matching dispatch, SQLite writing, SCP upload
- `src/migel.rs` — MiGeL XLSX parsing, keyword extraction, Aho-Corasick candidate finding, IDF-weighted word-level matching engine

### MiGeL matching algorithm

**Candidate finding** — Aho-Corasick automaton scans the combined DE+FR+IT product text in a single pass to find all matching keywords (including fuzzy truncated variants), then maps matched keywords to candidate MiGeL items.

**Per-language scoring** — Each candidate is scored per language (DE keywords against DE product text, FR against FR, IT against IT):
- German: compound word suffix matching + fuzzy inflection >= 6 chars (e.g., "binde" matches "binden", "katheter" in "verweilkatheter") + compound prefix decomposition via whitelist (e.g., "blasen" from "blasenkatheter")
- French/Italian: exact word matching only (prevents cross-type false positives)
- English-only detection: if all language fields are identical, FR/IT scoring is skipped
- English-to-German enrichment: ~60 common English medical terms (e.g., "knee" → "knie knieorthese", "catheter" → "katheter") are translated and appended to the DE text before matching, enabling products with English-only descriptions to match German MiGeL keywords

**IDF weighting** — Keywords are weighted by inverse document frequency (how rare they are across MiGeL items, capped at 5.0). Specific keywords like "verweilkatheter" get higher weight than generic ones like "katheter". IDF is used for ranking among passing candidates (choosing the best MiGeL match), while length-based scoring is used for threshold decisions.

**Category hierarchy** — Parent category text from the MiGeL XLSX hierarchy (e.g., "Injektions- und Infusionsmaterialien" → "Kanülen") is extracted as category keywords (>= 8 chars). These boost the IDF ranking score (with 0.5 weight) to prefer MiGeL items whose category context matches the product, but do NOT count toward match count thresholds.

**Ranking signals:**
- Bidirectional coverage: rewards matches where keywords cover a large fraction of the product's significant words (short focused names rank higher than verbose descriptions with incidental keyword overlap)
- Phrase matching: if the MiGeL Bezeichnung (>= 8 chars) appears as a substring in the product text, a strong ranking boost (1.0) is applied

**Precision filters:**
- Company exclusions: surgical implant manufacturers (DePuy Synthes/JJHCS, Waldemar Link, Mathys) and lab diagnostics (Roche Diagnostics) are excluded — near-100% false positive rates
- Universal exclusions block surgical gloves, interventional/surgical devices (PTA catheters, stent systems, ERCP, ablation catheters, etc.) from all MiGeL matching
- Stop words filter generic cross-category terms (dimensions, anatomical terms, generic device types)
- Negative keywords per MiGeL code prefix prevent specific false positive patterns (orthesis body-part confusion, dressing type confusion, catheter-vs-handle, surgical instruments vs patient devices)
- Secondary keywords (long terms from additional Bezeichnung lines) provide bonus matches gated by at least one primary keyword match
- Length penalty: verbose DE descriptions (15+ significant words) require higher single-keyword score (>= 0.7)

**Thresholds:** 2+ keywords: score >= 0.3, max len >= 6; single keyword: score >= 0.5, len >= 8 (>= 0.7 for verbose descriptions)

**Unmatched MiGeL codes:** ~480 of 786 MiGeL codes find no products. Most are capital equipment (pumps, lamps, ventilators) not in GS1 barcode registries, maintenance/rental services, or very specific subcategories where products match sibling codes instead.

## Dependencies

- [reqwest](https://crates.io/crates/reqwest) — HTTP client (blocking)
- [csv](https://crates.io/crates/csv) — CSV parsing
- [rusqlite](https://crates.io/crates/rusqlite) — SQLite interface
- [calamine](https://crates.io/crates/calamine) — XLSX parsing (MiGeL)
- [rayon](https://crates.io/crates/rayon) — Parallel processing
- [clap](https://crates.io/crates/clap) — CLI argument parsing
- [chrono](https://crates.io/crates/chrono) — Date/time formatting
- [aho-corasick](https://crates.io/crates/aho-corasick) — Multi-pattern string matching
- [unicode-normalization](https://crates.io/crates/unicode-normalization) — Unicode NFC normalization
