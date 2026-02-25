use csv::ReaderBuilder;
use rusqlite::Connection;
use std::error::Error;
use std::fs::File;
use std::io::{Cursor, Write};
use std::process::Command; // For executing SCP
use std::sync::mpsc;
use std::thread;

fn main() -> Result<(), Box<dyn Error>> {
    let url = "https://id.gs1.ch/01/07612345000961";
    let csv_filename = "firstbase.csv";
    let db_filename = "firstbase.db";

    // 1. Download and save CSV
    println!("Downloading to {}...", csv_filename);
    let response = reqwest::blocking::get(url)?;
    let content = response.text()?;
    let mut file = File::create(csv_filename)?;
    file.write_all(content.as_bytes())?;

    // 2. Setup Channel (Producer/Consumer)
    let (tx, rx) = mpsc::channel::<Vec<String>>();

    // 3. Database Thread (Consumer)
    let db_handle = thread::spawn(move || -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut conn = Connection::open("firstbase.db")?;
        let tx_db = conn.transaction()?;
        
        if let Ok(headers) = rx.recv() {
            let create_cols = headers.iter()
                .map(|h| format!("\"{}\" TEXT", h.replace(|c: char| !c.is_alphanumeric(), "_")))
                .collect::<Vec<_>>().join(", ");
            
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

    // 4. Parsing Thread (Producer - Main Thread)
    let mut reader = ReaderBuilder::new()
        .has_headers(false) 
        .from_reader(Cursor::new(content));

    let mut line_count = 0; // Track record count

    for result in reader.records() {
        let record = result?;
        let row_data: Vec<String> = record.iter()
            .take(15)
            .map(|s| s.to_string())
            .collect();
        tx.send(row_data)?;
        
        line_count += 1; // Increment for every row sent to DB
    }

    drop(tx); 

    // 5. Wait for DB thread to finish
    db_handle.join()
        .map_err(|_| "The database thread panicked")?
        .map_err(|e| e.to_string())?;

    println!("Database {} created successfully.", db_filename);
    println!("Total CSV lines processed: {}", line_count);

    // 6. SCP Transfer
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
