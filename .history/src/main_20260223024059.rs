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
const LATEST_TIME_BEFORE_CACHE_REFRESH: i64 = 24;

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();

    match args.get(1).map(String::as_str) {
        Some("fetch-historical") => { /* unchanged */ }
        _ => {
            // Determine interval from stream type
            let stream_type = args.get(1).map(String::as_str).unwrap_or("trade");
            let (interval, cache_file, csv_file) = match stream_type {
                "m5" => ("5m", "data/m5_latest_50000.parquet", "data/m5_live.csv"),
                "m15" => ("15m", "data/m15_latest_50000.parquet", "data/m15_live.csv"),
                "trade" => ("", "", ""), // no historical for trade stream
                _ => {
                    eprintln!("Unknown stream type. Use 'trade', 'm5', or 'm15'.");
                    std::process::exit(1);
                }
            };

            let historical = if stream_type == "m5" || stream_type == "m15" {
                load_or_fetch_historical(interval, cache_file).await?
            } else {
                Vec::new() // no historical data for trade stream
            };

            live_stream::run(stream_type, historical, cache_file, csv_file).await?;
        }
    }
    Ok(())
}

/// Load cached historical data if it exists and is fresh; otherwise fetch from Binance.
async fn load_or_fetch_historical(interval: &str, cache_file: &str) -> Result<Vec<kline::Kline>> {
    // Ensure data directory exists
    if let Some(parent) = Path::new(cache_file).parent() {
        std::fs::create_dir_all(parent)?;
    }

    let should_fetch = if Path::new(cache_file).exists() {
        let metadata = std::fs::metadata(cache_file)?;
        let modified = metadata.modified()?;
        let age = Utc::now().signed_duration_since(chrono::DateTime::<Utc>::from(modified));
        age > Duration::hours(LATEST_TIME_BEFORE_CACHE_REFRESH)
    } else {
        true
    };

    if should_fetch {
        println!(
            "Fetching latest {} {} candles from Binance...",
            HISTORICAL_COUNT, interval
        );
        let klines =
            binance_client::fetch_latest_klines(SYMBOL, interval, HISTORICAL_COUNT).await?;
        println!("Fetched {} klines. Saving to cache...", klines.len());
        data_storage::save_klines_to_parquet(&klines, cache_file)?;
        // Also write initial CSV (overwrite) to have a baseline
        data_storage::save_klines_to_csv(
            &klines,
            cache_file.replace(".parquet", "_raw.csv").as_str(),
        )?;
        Ok(klines)
    } else {
        println!("Loading cached historical data from {}", cache_file);
        let klines = data_storage::load_klines_from_parquet(cache_file)?;
        Ok(klines)
    }
}
