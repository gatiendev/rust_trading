use crate::kline::Kline;
use anyhow::Result;
use polars::prelude::*;
use std::fs::File;

/// Convert a slice of Klines into a Polars DataFrame.
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

/// Save a DataFrame to a Parquet file (takes mutable reference).
pub fn save_dataframe(df: &mut DataFrame, path: &str) -> Result<()> {
    let file = File::create(path)?;
    ParquetWriter::new(file).finish(df)?;
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
