use calamine::{open_workbook, Reader, Xlsx};
use chrono::Local;
use clap::Parser;
use csv::ReaderBuilder;
use rusqlite::Connection;
use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::io::{Cursor, Write};
use std::process::Command;
use std::sync::mpsc;
use std::thread;

#[derive(Parser)]
#[command(name = "fb2sqlite")]
struct Args {
    /// Download MiGeL XLSX and map migel codes/limitations to products
    #[arg(long)]
    migel: bool,
}

struct MigelItem {
    position_nr: String,
    bezeichnung: String,
    limitation: String,
    search_keywords: Vec<String>,
}

const STOP_WORDS: &[&str] = &[
    "der", "die", "das", "den", "dem", "des", "ein", "eine", "eines", "einem", "einen", "einer",
    "fuer", "mit", "von", "und", "oder", "bei", "auf", "nach", "ueber", "unter", "aus", "bis",
    "pro", "als", "inkl", "exkl", "max", "min", "per", "zur", "zum", "ins", "vom",
    "kauf", "miete", "tag", "jahr", "monate", "stueck", "set", "alle", "nur",
    "wird", "ist", "kann", "sind", "werden", "wurde", "hat", "haben",
    "les", "des", "pour", "avec", "par", "une", "dans", "sur", "qui", "que",
    "the", "for", "and", "with", "per",
    "achat", "location", "piece",
    "acquisto", "noleggio", "pezzo",
];

/// Normalize German umlauts so ALL-CAPS text (e.g. ABSAUGGERAETE) matches
/// proper text (e.g. Absauggeräte).
fn normalize_german(text: &str) -> String {
    text.replace('ä', "ae")
        .replace('ö', "oe")
        .replace('ü', "ue")
        .replace('ß', "ss")
        .replace('Ä', "Ae")
        .replace('Ö', "Oe")
        .replace('Ü', "Ue")
        .replace('é', "e")
        .replace('è', "e")
        .replace('ê', "e")
        .replace('à', "a")
        .replace('â', "a")
        .replace('ù', "u")
        .replace('û', "u")
        .replace('ô', "o")
        .replace('î', "i")
        .replace('ç', "c")
}

/// Extract search keywords from text: normalize, lowercase, split on non-alphanum,
/// filter short words and stop words.
fn extract_keywords(text: &str) -> Vec<String> {
    let first_line = text.lines().next().unwrap_or(text);
    let normalized = normalize_german(first_line).to_lowercase();
    normalized
        .split(|c: char| !c.is_alphanumeric())
        .filter(|w| w.len() >= 4)
        .filter(|w| !STOP_WORDS.contains(w))
        .map(|w| w.to_string())
        .collect()
}

/// Read a cell from a calamine row as a trimmed string.
fn cell_str(row: &[calamine::Data], idx: usize) -> String {
    row.get(idx)
        .map(|d| d.to_string())
        .unwrap_or_default()
        .trim()
        .to_string()
}

/// Parse all MiGeL items (rows with a Positions-Nr.) from the XLSX file.
/// Uses all 3 sheets (DE, FR, IT) for keyword enrichment.
fn parse_migel_items(path: &str) -> Result<Vec<MigelItem>, Box<dyn Error>> {
    let mut workbook: Xlsx<_> = open_workbook(path)?;
    let sheet_names: Vec<String> = workbook.sheet_names().to_vec();

    // --- Pass 1: Parse German sheet (index 0) ---
    let range_de = workbook.worksheet_range(&sheet_names[0])?;

    // Track category hierarchy descriptions (levels B through G = indices 1..7)
    let mut category_texts: Vec<String> = vec![String::new(); 7];
    let mut items: Vec<MigelItem> = Vec::new();

    for (row_idx, row) in range_de.rows().enumerate() {
        if row_idx == 0 {
            continue; // skip header
        }

        let pos_nr = cell_str(row, 7); // H = Positions-Nr.
        let bezeichnung = cell_str(row, 9); // J = Bezeichnung
        let limitation = cell_str(row, 10); // K = Limitation

        if pos_nr.is_empty() {
            // Category header row — update hierarchy
            // Find which level this header belongs to (deepest non-empty B..G)
            for i in (1..7).rev() {
                let val = cell_str(row, i);
                if !val.is_empty() {
                    category_texts[i] =
                        bezeichnung.lines().next().unwrap_or("").trim().to_string();
                    // Clear deeper levels
                    for j in (i + 1)..7 {
                        category_texts[j] = String::new();
                    }
                    break;
                }
            }
        } else {
            // Item with position number
            let first_line = bezeichnung.lines().next().unwrap_or("").trim().to_string();

            // Build search text: item description + parent category descriptions
            let mut parts: Vec<&str> = vec![&first_line];
            for cat in &category_texts {
                if !cat.is_empty() {
                    parts.push(cat);
                }
            }
            let search_text = parts.join(" ");
            let keywords = extract_keywords(&search_text);

            items.push(MigelItem {
                position_nr: pos_nr,
                bezeichnung: first_line,
                limitation,
                search_keywords: keywords,
            });
        }
    }

    // --- Pass 2: Enrich with French and Italian keywords ---
    let pos_map: HashMap<String, usize> = items
        .iter()
        .enumerate()
        .map(|(i, item)| (item.position_nr.clone(), i))
        .collect();

    for sheet_idx in 1..sheet_names.len().min(3) {
        let range = workbook.worksheet_range(&sheet_names[sheet_idx])?;
        for (row_idx, row) in range.rows().enumerate() {
            if row_idx == 0 {
                continue;
            }
            let pos_nr = cell_str(row, 7);
            if let Some(&item_idx) = pos_map.get(&pos_nr) {
                let bezeichnung = cell_str(row, 9);
                let extra_kw = extract_keywords(&bezeichnung);
                items[item_idx].search_keywords.extend(extra_kw);
            }
        }
    }

    // Deduplicate keywords per item
    for item in &mut items {
        item.search_keywords.sort();
        item.search_keywords.dedup();
    }

    Ok(items)
}

/// Build an inverted index: keyword → list of MigelItem indices.
fn build_keyword_index(items: &[MigelItem]) -> HashMap<String, Vec<usize>> {
    let mut index: HashMap<String, Vec<usize>> = HashMap::new();
    for (i, item) in items.iter().enumerate() {
        for kw in &item.search_keywords {
            index.entry(kw.clone()).or_default().push(i);
        }
    }
    index
}

/// Find the best-matching MiGeL item for a product description.
/// Uses substring matching (handles German compound words) and scores by
/// keyword overlap ratio. Returns None if no match above threshold.
fn find_best_migel_match<'a>(
    product_text: &str,
    migel_items: &'a [MigelItem],
    keyword_index: &HashMap<String, Vec<usize>>,
) -> Option<&'a MigelItem> {
    let product_lower = normalize_german(product_text).to_lowercase();

    // Accumulate matched keyword weight per candidate item
    let mut candidate_scores: HashMap<usize, (f64, usize)> = HashMap::new(); // (weight, count)

    for (keyword, indices) in keyword_index {
        if product_lower.contains(keyword.as_str()) {
            let weight = keyword.len() as f64;
            for &idx in indices {
                let entry = candidate_scores.entry(idx).or_insert((0.0, 0));
                entry.0 += weight;
                entry.1 += 1;
            }
        }
    }

    // Normalize scores, filter by threshold, pick best
    candidate_scores
        .iter()
        .filter_map(|(&idx, &(matched_weight, matched_count))| {
            let total_weight: f64 = migel_items[idx]
                .search_keywords
                .iter()
                .map(|k| k.len() as f64)
                .sum();
            if total_weight == 0.0 {
                return None;
            }
            let score = matched_weight / total_weight;
            // Require at least 40% keyword weight overlap AND at least 1 keyword match
            if score >= 0.4 && matched_count >= 1 {
                Some((idx, score, matched_count))
            } else {
                None
            }
        })
        .max_by(|a, b| {
            a.1.partial_cmp(&b.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.2.cmp(&b.2))
        })
        .map(|(idx, _, _)| &migel_items[idx])
}

fn run_normal(csv_content: &str) -> Result<(), Box<dyn Error>> {
    let db_filename = "firstbase.db";

    let (tx, rx) = mpsc::channel::<Vec<String>>();

    let db_handle = thread::spawn(move || -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut conn = Connection::open("firstbase.db")?;
        let tx_db = conn.transaction()?;

        if let Ok(headers) = rx.recv() {
            let create_cols = headers
                .iter()
                .map(|h| {
                    format!(
                        "\"{}\" TEXT",
                        h.replace(|c: char| !c.is_alphanumeric(), "_")
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");

            tx_db.execute("DROP TABLE IF EXISTS data", [])?;
            tx_db.execute(&format!("CREATE TABLE data ({})", create_cols), [])?;

            let placeholders = vec!["?"; headers.len()].join(", ");
            let query = format!("INSERT INTO data VALUES ({})", placeholders);

            {
                let mut stmt = tx_db.prepare(&query)?;
                while let Ok(row) = rx.recv() {
                    stmt.execute(rusqlite::params_from_iter(row))?;
                }
            }
        }
        tx_db.commit()?;
        Ok(())
    });

    let mut reader = ReaderBuilder::new()
        .has_headers(false)
        .from_reader(Cursor::new(csv_content));

    let mut line_count = 0;

    for result in reader.records() {
        let record = result?;
        let row_data: Vec<String> = record.iter().take(15).map(|s| s.to_string()).collect();
        tx.send(row_data)?;
        line_count += 1;
    }

    drop(tx);

    db_handle
        .join()
        .map_err(|_| "The database thread panicked")?
        .map_err(|e| e.to_string())?;

    println!("Database {} created successfully.", db_filename);
    println!("Total CSV lines processed: {}", line_count);

    // SCP Transfer
    let remote_dest = "zdavatz@65.109.137.20:/var/www/pillbox.oddb.org/";
    println!("Transferring {} to {}...", db_filename, remote_dest);

    let status = Command::new("scp")
        .arg(db_filename)
        .arg(remote_dest)
        .status()?;

    if status.success() {
        println!("SCP transfer complete.");
    } else {
        return Err(format!("SCP failed with exit code: {:?}", status.code()).into());
    }

    Ok(())
}

fn run_migel(csv_content: &str) -> Result<(), Box<dyn Error>> {
    let migel_url = "https://www.bag.admin.ch/dam/de/sd-web/77j5rwUTzbkq/Mittel-%20und%20Gegenst%C3%A4ndeliste%20per%2001.01.2026%20in%20Excel-Format.xlsx";
    let migel_file = "migel.xlsx";

    // 1. Download MiGeL XLSX
    println!("Downloading MiGeL XLSX...");
    let client = reqwest::blocking::Client::builder()
        .user_agent("fb2sqlite/0.1")
        .build()?;
    let response = client.get(migel_url).send()?;
    if !response.status().is_success() {
        return Err(format!(
            "Failed to download MiGeL XLSX: HTTP {}",
            response.status()
        )
        .into());
    }
    let bytes = response.bytes()?;
    fs::write(migel_file, &bytes)?;
    println!("MiGeL XLSX saved ({} bytes)", bytes.len());

    // 2. Parse MiGeL items
    println!("Parsing MiGeL items...");
    let migel_items = parse_migel_items(migel_file)?;
    println!(
        "Found {} MiGeL items with position numbers",
        migel_items.len()
    );

    let keyword_index = build_keyword_index(&migel_items);
    println!("Built keyword index with {} unique keywords", keyword_index.len());

    // 3. Generate date-stamped output filename
    let now = Local::now();
    let db_filename = now.format("firstbase_migel_%d.%m.%Y.db").to_string();

    // 4. Parse CSV and match products to MiGeL items
    let mut reader = ReaderBuilder::new()
        .has_headers(false)
        .from_reader(Cursor::new(csv_content));

    let (tx, rx) = mpsc::channel::<Vec<String>>();

    let db_fn = db_filename.clone();
    let db_handle = thread::spawn(move || -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut conn = Connection::open(&db_fn)?;
        let tx_db = conn.transaction()?;

        if let Ok(headers) = rx.recv() {
            let create_cols = headers
                .iter()
                .map(|h| {
                    format!(
                        "\"{}\" TEXT",
                        h.replace(|c: char| !c.is_alphanumeric(), "_")
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");

            tx_db.execute("DROP TABLE IF EXISTS data", [])?;
            tx_db.execute(&format!("CREATE TABLE data ({})", create_cols), [])?;

            let placeholders = vec!["?"; headers.len()].join(", ");
            let query = format!("INSERT INTO data VALUES ({})", placeholders);

            {
                let mut stmt = tx_db.prepare(&query)?;
                while let Ok(row) = rx.recv() {
                    stmt.execute(rusqlite::params_from_iter(row))?;
                }
            }
        }
        tx_db.commit()?;
        Ok(())
    });

    let mut line_count = 0;
    let mut match_count = 0;
    let mut first_row = true;

    for result in reader.records() {
        let record = result?;
        let row_data: Vec<String> = record.iter().take(15).map(|s| s.to_string()).collect();

        if first_row {
            // Header row — append MiGeL column names
            let mut headers = row_data;
            headers.push("migel_code".to_string());
            headers.push("migel_bezeichnung".to_string());
            headers.push("migel_limitation".to_string());
            tx.send(headers)?;
            first_row = false;
            line_count += 1;
            continue;
        }

        // Combine product descriptions for matching:
        // col 5 = TradeItemDescription_DE, 6 = FR, 7 = IT, 8 = BrandName
        let desc_de = row_data.get(5).cloned().unwrap_or_default();
        let desc_fr = row_data.get(6).cloned().unwrap_or_default();
        let desc_it = row_data.get(7).cloned().unwrap_or_default();
        let brand = row_data.get(8).cloned().unwrap_or_default();
        let product_text = format!("{} {} {} {}", desc_de, desc_fr, desc_it, brand);

        let mut row_with_migel = row_data;

        if let Some(migel) = find_best_migel_match(&product_text, &migel_items, &keyword_index) {
            row_with_migel.push(migel.position_nr.clone());
            row_with_migel.push(migel.bezeichnung.clone());
            row_with_migel.push(migel.limitation.clone());
            match_count += 1;
        } else {
            row_with_migel.push(String::new());
            row_with_migel.push(String::new());
            row_with_migel.push(String::new());
        }

        tx.send(row_with_migel)?;
        line_count += 1;
    }

    drop(tx);

    db_handle
        .join()
        .map_err(|_| "Database thread panicked")?
        .map_err(|e| e.to_string())?;

    println!("Database {} created successfully.", db_filename);
    println!(
        "Total CSV lines: {} (incl. header), MiGeL matches: {}",
        line_count, match_count
    );

    // 5. SCP Transfer
    let remote_dest = "zdavatz@65.109.137.20:/var/www/pillbox.oddb.org/";
    println!("Transferring {} to {}...", db_filename, remote_dest);

    let status = Command::new("scp")
        .arg(&db_filename)
        .arg(remote_dest)
        .status()?;

    if status.success() {
        println!("SCP transfer complete.");
    } else {
        return Err(format!("SCP failed with exit code: {:?}", status.code()).into());
    }

    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    let url = "https://id.gs1.ch/01/07612345000961";
    let csv_filename = "firstbase.csv";

    // 1. Download and save CSV
    println!("Downloading CSV to {}...", csv_filename);
    let response = reqwest::blocking::get(url)?;
    let content = response.text()?;
    {
        let mut file = std::fs::File::create(csv_filename)?;
        file.write_all(content.as_bytes())?;
    }

    if args.migel {
        run_migel(&content)?;
    } else {
        run_normal(&content)?;
    }

    Ok(())
}
