//! Exponential Moving Average (EMA) calculations for multiple timeframes.

use anyhow::Result;
use polars::prelude::*;

/// Helper: create EWMOptions from a span (typical for EMA).
fn ewma_opts_from_span(span: usize) -> EWMOptions {
    let alpha = 2.0 / (span as f64 + 1.0);
    EWMOptions {
        alpha,
        adjust: true,
        bias: false,
        min_periods: span,
        ignore_nulls: false,
    }
}

/// Add EMA50 and EMA200 columns for M15, H1, and H4 to the input DataFrame.
/// Expects the DataFrame to have columns "datetime" and "close".
pub fn add_ema_features(mut df: DataFrame) -> Result<DataFrame> {
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

    // Left join each resampled EMA on datetime (exact match)
    let ema_dfs = [h1_ema50, h1_ema200, h4_ema50, h4_ema200];
    let mut result = df.clone();
    for ema_df in &ema_dfs {
        result = result
            .lazy()
            .join(
                ema_df.clone().lazy(),
                [col("datetime")],
                [col("datetime")],
                JoinArgs::new(JoinType::Left),
            )
            .collect()?;
    }

    // Forward‑fill all EMA columns so every M15 row has the most recent H1/H4 values
    let ema_cols = [
        "ema50_h1",
        "ema200_h1",
        "ema50_h4",
        "ema200_h4",
        "ema50_m15",
        "ema200_m15",
    ];
    for col_name in &ema_cols {
        let s = result.column(col_name)?.clone();
        let filled = s.fill_null(FillNullStrategy::Forward(None))?;
        result.replace(col_name, filled)?;
    }

    Ok(result)
}

/// Helper: resample M15 data to `interval`, compute EMA with given `span`,
/// and return a DataFrame with columns ["datetime", col_name].
fn compute_resampled_ema(
    df: &DataFrame,
    interval: &str,
    span: usize,
    col_name: &str,
) -> Result<DataFrame> {
    let every = Duration::parse(interval);
    let period = Duration::parse(interval);
    let offset = Duration::parse("0ns");

    let options = DynamicGroupOptions {
        every,
        period,
        offset,
        closed_window: ClosedWindow::Right,
        start_by: StartBy::DataPoint,
        include_boundaries: false,
        ..Default::default()
    };

    let resampled = df
        .clone()
        .lazy()
        .group_by_dynamic(col("datetime"), [], options)
        .agg([col("close").last().alias("close")])
        .collect()?;

    let ema_opts = ewma_opts_from_span(span);
    let ema = resampled
        .lazy()
        .with_column(col("close").ewm_mean(ema_opts).alias(col_name))
        .select([col("datetime"), col(col_name)])
        .collect()?;

    Ok(ema)
}
