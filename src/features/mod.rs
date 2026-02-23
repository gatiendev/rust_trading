//! Feature engineering: combines all feature groups (EMA, pivots, etc.)
//! and returns a DataFrame enriched with computed indicators.

use crate::kline::Kline;
use anyhow::Result;
use polars::prelude::*;
use std::time::Instant;

mod ema;
mod pivots;

/// Compute all features on a slice of klines and return a DataFrame with added columns.
/// Currently adds EMA50/200 for M15, H1, H4. Pivot points will be added later.
pub fn compute_features(klines: &[Kline]) -> Result<DataFrame> {
    let start = Instant::now();

    // Convert klines to DataFrame and add a proper datetime column
    let mut df = crate::data_storage::klines_to_dataframe(klines)?;
    df = df
        .lazy()
        .with_column(
            col("open_time")
                .cast(DataType::Datetime(TimeUnit::Milliseconds, None))
                .alias("datetime"),
        )
        .sort(vec!["datetime"], Default::default())
        .collect()?;

    // Add EMA features
    df = ema::add_ema_features(df)?;

    // Placeholder for pivot points:
    df = pivots::add_pivot_features(df)?;

    let elapsed = start.elapsed();
    println!(
        "Feature computation took: {:.2} ms",
        elapsed.as_secs_f64() * 1000.0
    );

    Ok(df)
}
