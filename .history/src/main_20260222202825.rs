mod binance_client;
mod data_storage;
mod kline;
mod live_stream;

use anyhow::Result;
use chrono::{Duration, Utc};
use std::path::Path;

const HISTORICAL_COUNT: usize = 50_000;
const CACHE_FILE: &str = "data/m15_latest_50000.parquet";
const SYMBOL: &str = "BTCUSDT";
const INTERVAL: &str = "15m";

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
            data_storage::save_dataframe_csv(&mut df, "data/data.csv")?;

            println!("Saved to {}", output);
        }
        _ => {
            // Default / live mode: load latest 50k M15 candles, then start live stream
            let historical = load_or_fetch_historical().await?;
            // let feature_state = compute_initial_features(&historical); // consume if possible
            // drop(historical); // free memory

            // Pass the stream type (if provided) and historical data to live stream
            let stream_type = args.get(1).map(String::as_str); // e.g., "m15" or "trade"
            live_stream::run(stream_type, historical).await?;
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
        age > Duration::hours(1) // older than 1 hour -> refresh
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
        data_storage::save_dataframe_csv(&mut df, "/app/data/data.csv")?;
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
        if let Err(e) = data_storage::save_dataframe_csv(&mut df, "/app/data/data.csv") {
            eprintln!("Warning: failed to save CSV: {}", e);
        }

        Ok(klines)
    }
}
