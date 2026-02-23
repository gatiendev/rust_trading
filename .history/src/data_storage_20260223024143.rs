use crate::kline::Kline;
use anyhow::Result;
use chrono::{DateTime, Utc};
use polars::prelude::*;
use std::fs::{File, OpenOptions};
use std::io::Write;

/// Helper to convert timestamp to string (same as in save_dataframe_csv_to_path)
fn timestamp_to_string(ms: i64) -> String {
    DateTime::<Utc>::from_timestamp_millis(ms)
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S%.3f UTC").to_string())
        .unwrap_or_else(|| format!("Invalid({})", ms))
}

/// Save a slice of Klines to a Parquet file (overwrites if exists).
pub fn save_klines_to_parquet(klines: &[Kline], path: &str) -> Result<()> {
    let mut df = klines_to_dataframe(klines)?;
    let file = File::create(path)?;
    ParquetWriter::new(file).finish(&mut df)?;
    Ok(())
}

/// Append a single Kline to a CSV file. If the file does not exist, headers are written first.
pub fn append_kline_to_csv(kline: &Kline, path: &str) -> Result<()> {
    let file_exists = std::path::Path::new(path).exists();

    let mut file = OpenOptions::new().create(true).append(true).open(path)?;

    if !file_exists {
        // Write header
        writeln!(file, "open_time,open,high,low,close,volume,close_time")?;
    }

    // Write data line
    writeln!(
        file,
        "{},{},{},{},{},{},{}",
        timestamp_to_string(kline.open_time),
        kline.open,
        kline.high,
        kline.low,
        kline.close,
        kline.volume,
        timestamp_to_string(kline.close_time)
    )?;

    Ok(())
}

/// Optional: Save all klines to CSV (overwrite) – useful for initial baseline.
pub fn save_klines_to_csv(klines: &[Kline], path: &str) -> Result<()> {
    let mut file = File::create(path)?;
    // Write header
    writeln!(file, "open_time,open,high,low,close,volume,close_time")?;
    for k in klines {
        writeln!(
            file,
            "{},{},{},{},{},{},{}",
            timestamp_to_string(k.open_time),
            k.open,
            k.high,
            k.low,
            k.close,
            k.volume,
            timestamp_to_string(k.close_time)
        )?;
    }
    Ok(())
}
