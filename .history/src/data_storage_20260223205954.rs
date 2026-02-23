use crate::kline::Kline;
use anyhow::Result;
use chrono::{DateTime, Utc};
use polars::prelude::*;

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};

/// Convert milliseconds to a human‑readable UTC string (e.g., "2025-03-21 14:32:17.456 UTC").
fn timestamp_to_string(ms: i64) -> String {
    DateTime::<Utc>::from_timestamp_millis(ms)
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
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

    // Row numbers: 1..=len
    let row_numbers: Vec<u32> = (1..=klines.len() as u32).collect();

    let df = df!(
        "index" => row_numbers,
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

pub fn append_features_row_to_csv(df: &DataFrame, path: &str) -> Result<()> {
    let file_exists = std::path::Path::new(path).exists();
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;

    // Get the last row (as a DataFrame with one row)
    let last_row_df = df.tail(Some(1));
    if last_row_df.height() == 0 {
        return Ok(()); // nothing to append
    }

    // Write header if file is new
    if !file_exists {
        let headers: Vec<&str> = last_row_df
            .get_column_names()
            .iter()
            .map(|name| name.as_str())
            .collect();
        writeln!(file, "{}", headers.join(","))?;
    }

    // Collect values from the single row, converting timestamps to readable strings
    let mut values = Vec::new();
    for col_name in last_row_df.get_column_names() {
        let series = last_row_df.column(col_name)?;
        let val = series.get(0)?;

        let s = if col_name == "open_time" || col_name == "close_time" {
            match val {
                AnyValue::Int64(ts) => timestamp_to_string(ts),
                _ => format!("{}", val), // fallback (should not happen)
            }
        } else {
            format!("{}", val)
        };
        values.push(s);
    }

    writeln!(file, "{}", values.join(","))?;
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

/// Save a DataFrame to a Parquet file (overwrites).
pub fn save_dataframe_parquet(df: &mut DataFrame, path: &str) -> Result<()> {
    let file = File::create(path)?;
    ParquetWriter::new(file).finish(df)?;
    Ok(())
}

/// Save a DataFrame to a CSV file with human‑readable timestamps (overwrites).
/// Converts `open_time` and `close_time` columns (if present) to readable strings.
pub fn save_dataframe_csv_to_path(df: &DataFrame, path: &str) -> Result<()> {
    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);

    // Write header
    let headers: Vec<&str> = df.get_column_names().iter().map(|s| s.as_str()).collect();
    writeln!(writer, "{}", headers.join(","))?;

    // Determine indices of timestamp columns (if present)
    let open_time_idx = df.get_column_index("open_time");
    let close_time_idx = df.get_column_index("close_time");

    let height = df.height();

    for row_idx in 0..height {
        let mut values = Vec::with_capacity(headers.len());

        for (col_idx, col_name) in headers.iter().enumerate() {
            let series = df.column(col_name)?;

            // If this column is open_time or close_time, format as string
            if Some(col_idx) == open_time_idx || Some(col_idx) == close_time_idx {
                if let Ok(ca) = series.i64() {
                    let opt = ca.get(row_idx);
                    let s = opt.map_or(String::new(), |ms| timestamp_to_string(ms));
                    values.push(s);
                } else {
                    // Fallback: use debug representation
                    values.push(format!("{:?}", series.get(row_idx)?));
                }
            } else {
                // For other columns, get the value and convert to string
                values.push(format!("{}", series.get(row_idx)?));
            }
        }

        writeln!(writer, "{}", values.join(","))?;
    }

    writer.flush()?;
    Ok(())
}
