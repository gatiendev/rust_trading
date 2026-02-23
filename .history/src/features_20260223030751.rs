// src/features.rs
use crate::kline::Kline;
use anyhow::Result;
use polars::prelude::*;

/// Compute features on a slice of klines and return a DataFrame with added feature columns.
/// This version adds:
/// - EMA50 and EMA200 for M15 (directly on close)
/// - EMA50 and EMA200 for H1 (resampled from M15)
/// - EMA50 and EMA200 for H4 (resampled from M15)
pub fn compute_features(klines: &[Kline]) -> Result<DataFrame> {
    // Convert to DataFrame and add a proper datetime column
    let mut df = crate::data_storage::klines_to_dataframe(klines)?;
    df = df
        .lazy()
        .with_column(
            col("open_time")
                .cast(DataType::Datetime(TimeUnit::Milliseconds, None))
                .alias("datetime"),
        )
        .sort("datetime", Default::default())
        .collect()?;

    // ---- 1. M15 EMAs (direct on close) ----
    df = df
        .lazy()
        .with_column(
            col("close")
                .ewm_mean(50, false, false, None)
                .alias("ema50_m15"),
        )
        .with_column(
            col("close")
                .ewm_mean(200, false, false, None)
                .alias("ema200_m15"),
        )
        .collect()?;

    // ---- 2. H1 EMAs (resample to 1 hour) ----
    let h1_emails = compute_resampled_ema(&df, "1h", 50, "ema50_h1")?;
    let h1_ema200 = compute_resampled_ema(&df, "1h", 200, "ema200_h1")?;

    // ---- 3. H4 EMAs (resample to 4 hours) ----
    let h4_emails = compute_resampled_ema(&df, "4h", 50, "ema50_h4")?;
    let h4_ema200 = compute_resampled_ema(&df, "4h", 200, "ema200_h4")?;

    // Join all new columns back to the original DataFrame (by datetime, forward‑filled)
    let mut result = df.clone();
    for ema_df in &[h1_emails, h1_ema200, h4_emails, h4_ema200] {
        result = result
            .lazy()
            .join_asof(
                ema_df.clone().lazy(),
                left_on(col("datetime")),
                right_on(col("datetime")),
                JoinAsofOptions {
                    strategy: AsofStrategy::Forward, // fill forward
                    tolerance: None,
                    allow_parallel: true,
                    ..Default::default()
                },
            )
            .collect()?;
    }

    Ok(result)
}

/// Helper: resample M15 data to `interval` (e.g., "1h"), compute EMA with given `span`,
/// and return a DataFrame with columns ["datetime", col_name] where datetime is the end of each interval.
fn compute_resampled_ema(
    df: &DataFrame,
    interval: &str,
    span: usize,
    col_name: &str,
) -> Result<DataFrame> {
    // Resample: take last close of each interval
    let resampled = df
        .lazy()
        .group_by_dynamic(
            col("datetime"),
            [], // no partition columns
            DynamicGroupOptions {
                every: Duration::parse(interval),
                period: Duration::parse(interval),
                offset: Duration::parse("0"),
                ..Default::default()
            },
        )
        .agg([col("close").last().alias("close")])
        .collect()?;

    // Compute EMA on resampled close
    let mut ema = resampled
        .lazy()
        .with_column(
            col("close")
                .ewm_mean(span, false, false, None)
                .alias(col_name),
        )
        .select([col("datetime"), col(col_name)])
        .collect()?;

    Ok(ema)
}
