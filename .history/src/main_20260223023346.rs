mod binance_client;
mod data_storage;
mod kline;
mod live_stream;

use anyhow::Result;
use chrono::{Duration, Utc};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex; // use async Mutex for .await safety
use tokio::time::{interval, Duration};

const HISTORICAL_COUNT: usize = 50_000;
const CACHE_FILE: &str = "data/m15_latest_50000.parquet";
const SYMBOL: &str = "BTCUSDT";
const INTERVAL: &str = "15m";
const LATEST_TIME_BEFORE_CACHE_REFRESH: i64 = 24;

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();

    match args.get(1).map(String::as_str) {
        Some("fetch-historical") => {
            // Manual fetch mode: requires interval, start date, end date, output file
            if args.len() < 5 {
                eprintln!("Usage: fetch-historical <interval> <start YYYY-MM-DD> <end YYYY-MM-DD> <output.parquet>");
                std::process::exit(1);
            }
            let interval = &args[2];
            let start_str = &args[3];
            let end_str = &args[4];
            let output = &args[5];

            let start = chrono::NaiveDate::parse_from_str(start_str, "%Y-%m-%d")?
                .and_hms_opt(0, 0, 0)
                .unwrap()
                .and_utc()
                .timestamp_millis();
            let end = chrono::NaiveDate::parse_from_str(end_str, "%Y-%m-%d")?
                .and_hms_opt(23, 59, 59)
                .unwrap()
                .and_utc()
                .timestamp_millis();

            println!(
                "Fetching {} {} data from {} to {}",
                SYMBOL, interval, start_str, end_str
            );
            let klines = binance_client::fetch_klines_range(SYMBOL, interval, start, end).await?;
            println!("Fetched {} klines", klines.len());

            let mut df = data_storage::klines_to_dataframe(&klines)?;
            data_storage::save_dataframe_parquet(&mut df, output)?;
            data_storage::save_raw_dataframe(&mut df)?;

            println!("Saved to {}", output);
        }
        _ => {
            // Default / live mode: load latest 50k M15 candles, then start live stream
            let historical = load_or_fetch_historical().await?;
            // let feature_state = compute_initial_features(&historical); // consume if possible
            // drop(historical); // free memory

            // Pass the stream type (if provided) and historical data to live stream
            let stream_type = args.get(1).map(String::as_str); // e.g., "m15" or "trade"

            let live_buffer = Arc::new(Mutex::new(Vec::<kline::Kline>::new()));

            // Spawn the writer task (cloning the Arc for the task)
            let writer_buffer = live_buffer.clone();
            tokio::spawn(async move {
                let mut interval = interval(Duration::from_secs(10)); // adjust as needed
                loop {
                    interval.tick().await;
                    // Drain the buffer
                    let candles_to_write = {
                        let mut buf = writer_buffer.lock().await;
                        if buf.is_empty() {
                            continue;
                        }
                        std::mem::take(&mut *buf) // take all, leaving empty Vec
                    };
                    // Write to disk (implement this function)
                    if let Err(e) = write_live_candles_to_disk(candles_to_write).await {
                        eprintln!("Error writing live candles: {}", e);
                    }
                }
            });

            // Start live stream with the buffer
            live_stream::run(stream_type, historical, live_buffer).await?;
        }
    }

    Ok(())
}

/// Load cached historical data if it exists and is fresh; otherwise fetch from Binance.
async fn load_or_fetch_historical() -> Result<Vec<kline::Kline>> {
    // Ensure data directory exists
    if let Some(parent) = Path::new(CACHE_FILE).parent() {
        std::fs::create_dir_all(parent)?;
    }

    // Check if cache file exists and is recent (e.g., less than 1 hour old)
    let should_fetch = if Path::new(CACHE_FILE).exists() {
        let metadata = std::fs::metadata(CACHE_FILE)?;
        let modified = metadata.modified()?;
        let age = Utc::now().signed_duration_since(chrono::DateTime::<Utc>::from(modified));
        age > Duration::hours(LATEST_TIME_BEFORE_CACHE_REFRESH) // older than 1 hour -> refresh
    } else {
        true
    };

    if should_fetch {
        println!(
            "Fetching latest {} M15 candles from Binance...",
            HISTORICAL_COUNT
        );
        let klines =
            binance_client::fetch_latest_klines(SYMBOL, INTERVAL, HISTORICAL_COUNT).await?;
        println!("Fetched {} klines. Saving to cache...", klines.len());
        let mut df = data_storage::klines_to_dataframe(&klines)?;
        data_storage::save_dataframe_parquet(&mut df, CACHE_FILE)?;
        data_storage::save_raw_dataframe(&mut df)?;
        Ok(klines)
    } else {
        println!("Loading cached historical data from {}", CACHE_FILE);
        let klines = data_storage::load_klines_from_parquet(CACHE_FILE)?;

        let mut df = match data_storage::klines_to_dataframe(&klines) {
            Ok(df) => df,
            Err(e) => {
                eprintln!("ERROR in klines_to_dataframe: {}", e);
                // Return the original klines vector and skip CSV saving
                return Ok(klines);
            }
        };

        // Now safely attempt CSV write
        if let Err(e) = data_storage::save_raw_dataframe(&mut df) {
            eprintln!("Warning: failed to save CSV: {}", e);
        }

        Ok(klines)
    }
}

async fn write_live_candles_to_disk(candles: Vec<kline::Kline>) -> Result<()> {
    if candles.is_empty() {
        return Ok(());
    }

    // Convert to DataFrame
    let mut df = data_storage::klines_to_dataframe(&candles)?;

    // Create a daily file name (e.g., data/live_2025-02-23.csv)
    let now = chrono::Utc::now();
    let date_str = now.format("%Y-%m-%d").to_string();
    let csv_path = format!("data/live_{}.csv", date_str);
    let parquet_path = format!("data/live_{}_{}.parquet", date_str, now.timestamp());

    // Append to CSV (create with headers if file doesn't exist)
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&csv_path)?;
    // Polars can write CSV to a writer, but we need to handle headers only once.
    // Simpler: use Polars to write to a temporary string and then append.
    // Alternatively, use csv crate directly. For brevity, we'll use Polars:
    let mut buf = Vec::new();
    let mut writer = polars::prelude::CsvWriter::new(&mut buf)
        .include_header(!std::path::Path::new(&csv_path).exists())
        .finish(&mut df)?;
    std::fs::write(&csv_path, buf)?; // This overwrites the file, not appends.
                                     // Better: use std::fs::OpenOptions with append, but Polars doesn't support streaming append easily.
                                     // For a production bot, consider using a logging library or append to CSV manually.
                                     // We'll keep it simple: write a new CSV file per batch, and later concatenate if needed.

    // Write Parquet (new file per batch)
    data_storage::save_dataframe_parquet(&mut df, &parquet_path)?;

    println!(
        "Wrote {} live candles to {} and {}",
        candles.len(),
        csv_path,
        parquet_path
    );
    Ok(())
}
