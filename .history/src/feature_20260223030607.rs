// src/features.rs
use anyhow::Result;
use polars::prelude::*;
use crate::kline::Kline;

/// Compute features on a slice of klines and return a DataFrame with added feature columns.
/// `window_size` is the number of candles used for rolling indicators (e.g., 20 for SMA).
pub fn compute_features(klines: &[Kline], window_sizes: &[usize]) -> Result<DataFrame> {
    // Convert to DataFrame
    let mut df = crate::data_storage::klines_to_dataframe(klines)?;

    // Example: Simple Moving Average (SMA) for each window size
    for &window in window_sizes {
        let sma_col_name = format!("sma_{}", window);
        // Use Polars' rolling mean
        let sma = df.column("close")?
            .f64()?
            .rolling_mean(window, false, None, None)?;
        df.with_column(sma.with_name(sma_col_name.as_str()))?;
    }

    // Add more indicators: RSI, MACD, Bollinger Bands, etc.
    // ...

    Ok(df)
}

/// If you need to update features incrementally (to save recomputation time),
/// you can implement a struct that maintains state.
pub struct FeatureEngine {
    // Store necessary state for incremental updates
}

impl FeatureEngine {
    pub fn new() -> Self { ... }
    pub fn update(&mut self, new_kline: &Kline) -> Result<Vec<f64>> { ... }
}