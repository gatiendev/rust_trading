use crate::kline::Kline;
use anyhow::Result;
use polars::prelude::*;

/// Helper: create EWMOptions from a span (typical for EMA).
fn ewma_opts_from_span(span: usize) -> EWMOptions {
    let alpha = 2.0 / (span as f64 + 1.0);
    EWMOptions {
        alpha: alpha,
        adjust: true,
        bias: false,
        min_periods: 1,
        ignore_nulls: false,
    }
}

/// Compute features on a slice of klines and return a DataFrame with added feature columns.
/// Adds EMA50 and EMA200 for M15, H1, and H4 (all derived from M15 data).
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
        .sort(vec!["datetime"], Default::default())
        .collect()?;

    // ---- 1. M15 EMAs (direct on close) ----
    let ema50_opts = ewma_opts_from_span(50);
    let ema200_opts = ewma_opts_from_span(200);

    df = df
        .lazy()
        .with_column(col("close").ewm_mean(ema50_opts).alias("ema50_m15"))
        .with_column(col("close").ewm_mean(ema200_opts).alias("ema200_m15"))
        .collect()?;

    // ---- 2. H1 EMAs (resample to 1 hour) ----
    let h1_ema50 = compute_resampled_ema(&df, "1h", 50, "ema50_h1")?;
    let h1_ema200 = compute_resampled_ema(&df, "1h", 200, "ema200_h1")?;

    // ---- 3. H4 EMAs (resample to 4 hours) ----
    let h4_ema50 = compute_resampled_ema(&df, "4h", 50, "ema50_h4")?;
    let h4_ema200 = compute_resampled_ema(&df, "4h", 200, "ema200_h4")?;

    // Join all resampled EMAs back using asof join (forward fill)
    let mut result = df.clone();
    for ema_df in &[h1_ema50, h1_ema200, h4_ema50, h4_ema200] {
        result = result
            .lazy()
            .join(
                ema_df.clone().lazy(),
                [col("datetime")], // left_on
                [col("datetime")], // right_on
                JoinArgs::new(JoinType::Asof).with_asof_options(JoinAsofOptions {
                    strategy: AsofStrategy::Forward,
                    tolerance: None,
                    allow_parallel: true,
                }),
            )
            .collect()?;
    }

    Ok(result)
}

/// Helper: resample M15 data to `interval` (e.g., "1h"), compute EMA with given `span`,
/// and return a DataFrame with columns ["datetime", col_name].
fn compute_resampled_ema(
    df: &DataFrame,
    interval: &str,
    span: usize,
    col_name: &str,
) -> Result<DataFrame> {
    // Parse interval string into polars::Duration
    let every = Duration::parse(interval);
    let period = Duration::parse(interval);

    // Resample: take last close of each interval using group_by_dynamic
    let resampled = df
        .lazy()
        .group_by_dynamic(
            col("datetime"),
            [], // no partition columns
            every,
            period,
            Duration::parse("0"),
            ClosedWindow::Right,
            StartBy::DataPoint,
        )
        .agg([col("close").last().alias("close")])
        .collect()?;

    // Compute EMA on resampled close
    let ema_opts = ewma_opts_from_span(span);
    let ema = resampled
        .lazy()
        .with_column(col("close").ewm_mean(ema_opts).alias(col_name))
        .select([col("datetime"), col(col_name)])
        .collect()?;

    Ok(ema)
}
