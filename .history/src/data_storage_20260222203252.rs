use crate::kline::Kline;
use anyhow::Result;
use chrono::{DateTime, Utc};
use polars::prelude::*;
use std::fs::File;

/// Convert milliseconds to a human‑readable UTC string (e.g., "2025-03-21 14:32:17.456 UTC").
fn timestamp_to_string(ms: i64) -> String {
    DateTime::<Utc>::from_timestamp_millis(ms)
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S%.3f UTC").to_string())
        .unwrap_or_else(|| format!("Invalid({})", ms))
}

/// Convert a slice of Klines into a Polars DataFrame (timestamps as i64).
pub fn klines_to_dataframe(klines: &[Kline]) -> Result<DataFrame> {
    let open_time: Vec<i64> = klines.iter().map(|k| k.open_time).collect();
    let open: Vec<f64> = klines.iter().map(|k| k.open).collect();
    let high: Vec<f64> = klines.iter().map(|k| k.high).collect();
    let low: Vec<f64> = klines.iter().map(|k| k.low).collect();
    let close: Vec<f64> = klines.iter().map(|k| k.close).collect();
    let volume: Vec<f64> = klines.iter().map(|k| k.volume).collect();
    let close_time: Vec<i64> = klines.iter().map(|k| k.close_time).collect();

    let df = df!(
        "open_time" => open_time,
        "open" => open,
        "high" => high,
        "low" => low,
        "close" => close,
        "volume" => volume,
        "close_time" => close_time,
    )?;
    Ok(df)
}

/// Save a DataFrame to a Parquet file (takes mutable reference because ParquetWriter::finish requires &mut DataFrame).
/// Timestamps are stored as i64 (milliseconds) for efficiency.
pub fn save_dataframe_parquet(df: &mut DataFrame, path: &str) -> Result<()> {
    let file = File::create(path)?;
    ParquetWriter::new(file).finish(df)?;
    Ok(())
}

/// Save a DataFrame to a CSV file with human‑readable timestamps.
/// The original timestamp columns are converted to strings in the format
/// "YYYY-MM-DD HH:MM:SS.fff UTC". The input DataFrame is not modified.
pub fn save_dataframe_csv(df: &DataFrame, path: &str) -> Result<()> {
    println!("Saving DataFrame to CSV: {}", path);
    let mut df_csv = df.clone();

    // Replace open_time with human‑readable strings
    if let Ok(open_time_series) = df_csv.column("open_time") {
        if let Ok(ca) = open_time_series.i64() {
            let str_vals: Vec<String> = ca
                .into_iter()
                .map(|opt| opt.map_or(String::new(), timestamp_to_string))
                .collect();
            df_csv.replace("open_time", Series::new("open_time", str_vals))?;
        }
    }

    // Replace close_time with human‑readable strings
    if let Ok(close_time_series) = df_csv.column("close_time") {
        if let Ok(ca) = close_time_series.i64() {
            let str_vals: Vec<String> = ca
                .into_iter()
                .map(|opt| opt.map_or(String::new(), timestamp_to_string))
                .collect();
            df_csv.replace("close_time", Series::new("close_time", str_vals))?;
        }
    }

    let file = File::create(path)?;
    let mut writer = CsvWriter::new(file);
    writer.finish(&mut df_csv)?;
    Ok(())
}

/// Load a DataFrame from a Parquet file.
pub fn load_dataframe(path: &str) -> Result<DataFrame> {
    let file = File::open(path)?;
    let df = ParquetReader::new(file).finish()?;
    Ok(df)
}

/// Load klines from a Parquet file (returns Vec<Kline> for convenience).
pub fn load_klines_from_parquet(path: &str) -> Result<Vec<Kline>> {
    let df = load_dataframe(path)?;
    // Convert back to Vec<Kline>
    let open_time = df.column("open_time")?.i64()?;
    let open = df.column("open")?.f64()?;
    let high = df.column("high")?.f64()?;
    let low = df.column("low")?.f64()?;
    let close = df.column("close")?.f64()?;
    let volume = df.column("volume")?.f64()?;
    let close_time = df.column("close_time")?.i64()?;

    let mut klines = Vec::with_capacity(open_time.len());
    for i in 0..open_time.len() {
        klines.push(Kline {
            open_time: open_time.get(i).unwrap(),
            open: open.get(i).unwrap(),
            high: high.get(i).unwrap(),
            low: low.get(i).unwrap(),
            close: close.get(i).unwrap(),
            volume: volume.get(i).unwrap(),
            close_time: close_time.get(i).unwrap(),
        });
    }
    Ok(klines)
}
