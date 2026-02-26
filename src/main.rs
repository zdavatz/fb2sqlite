mod migel;

use chrono::Local;
use clap::Parser;
use csv::ReaderBuilder;
use migel::{build_keyword_index, find_best_migel_match, parse_migel_items, MigelItem};
use rayon::prelude::*;
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

    /// Use local firstbase.csv instead of downloading (useful when GS1 server is slow)
    #[arg(long)]
    local_csv: bool,

    /// Deploy: SCP the database to the remote server (uses plain filename without date)
    #[arg(long)]
    deploy: bool,
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

/// Match a single product row against the MiGeL index.
/// Returns (row_with_migel_columns, matched).
fn match_product_row(
    row_data: Vec<String>,
    migel_items: &[MigelItem],
    keyword_index: &HashMap<String, Vec<usize>>,
) -> (Vec<String>, bool) {
    // col 5 = TradeItemDescription_DE, 6 = FR, 7 = IT, 8 = BrandName
    let desc_de = row_data.get(5).cloned().unwrap_or_default();
    let desc_fr = row_data.get(6).cloned().unwrap_or_default();
    let desc_it = row_data.get(7).cloned().unwrap_or_default();
    let brand = row_data.get(8).cloned().unwrap_or_default();

    let mut row_with_migel = row_data;

    if let Some(migel) =
        find_best_migel_match(&desc_de, &desc_fr, &desc_it, &brand, migel_items, keyword_index)
    {
        row_with_migel.push(migel.position_nr.clone());
        row_with_migel.push(migel.bezeichnung.clone());
        row_with_migel.push(migel.limitation.clone());
        (row_with_migel, true)
    } else {
        row_with_migel.push(String::new());
        row_with_migel.push(String::new());
        row_with_migel.push(String::new());
        (row_with_migel, false)
    }
}

fn run_migel(csv_content: &str, deploy: bool) -> Result<(), Box<dyn Error>> {
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
    println!(
        "Built keyword index with {} unique keywords",
        keyword_index.len()
    );

    // 3. Generate output filename
    let db_filename = if deploy {
        "firstbase_migel.db".to_string()
    } else {
        let now = Local::now();
        now.format("firstbase_migel_%d.%m.%Y.db").to_string()
    };

    // 4. Parse CSV — collect all rows first for parallel processing
    println!("Reading CSV rows...");
    let mut reader = ReaderBuilder::new()
        .has_headers(false)
        .from_reader(Cursor::new(csv_content));

    let mut headers: Option<Vec<String>> = None;
    let mut data_rows: Vec<Vec<String>> = Vec::new();

    for result in reader.records() {
        let record = result?;
        let row_data: Vec<String> = record.iter().take(15).map(|s| s.to_string()).collect();

        if headers.is_none() {
            // First row is the header
            let mut h = row_data;
            h.push("migel_code".to_string());
            h.push("migel_bezeichnung".to_string());
            h.push("migel_limitation".to_string());
            headers = Some(h);
        } else {
            data_rows.push(row_data);
        }
    }

    let headers = headers.ok_or("CSV has no rows")?;
    let total_rows = data_rows.len();
    println!("Collected {} data rows, matching in parallel...", total_rows);

    // 5. Match products to MiGeL items IN PARALLEL using rayon
    let results: Vec<(Vec<String>, bool)> = data_rows
        .into_par_iter()
        .map(|row| match_product_row(row, &migel_items, &keyword_index))
        .collect();

    let match_count = results.iter().filter(|(_, matched)| *matched).count();

    // 6. Write matched results to SQLite (sequential — SQLite is single-writer)
    println!("Writing {} matched rows to database...", match_count);
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

    tx.send(headers)?;
    for (row, matched) in results {
        if matched {
            tx.send(row)?;
        }
    }
    drop(tx);

    db_handle
        .join()
        .map_err(|_| "Database thread panicked")?
        .map_err(|e| e.to_string())?;

    println!("Database {} created successfully.", db_filename);
    println!(
        "Total data rows: {}, MiGeL matches: {}",
        total_rows, match_count
    );

    // 7. SCP Transfer (only when deploying)
    if deploy {
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
    }

    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    let url = "https://id.gs1.ch/01/07612345000961";
    let csv_filename = "firstbase.csv";

    let content = if args.local_csv {
        // Use locally cached CSV file
        println!("Reading local CSV from {}...", csv_filename);
        fs::read_to_string(csv_filename)?
    } else {
        // Download and save CSV
        println!("Downloading CSV to {}...", csv_filename);
        let client = reqwest::blocking::Client::builder()
            .timeout(std::time::Duration::from_secs(300))
            .build()?;
        let response = client.get(url).send()?;
        let content = response.text()?;
        {
            let mut file = std::fs::File::create(csv_filename)?;
            file.write_all(content.as_bytes())?;
        }
        content
    };

    if args.migel {
        run_migel(&content, args.deploy)?;
    } else {
        run_normal(&content)?;
    }

    Ok(())
}
