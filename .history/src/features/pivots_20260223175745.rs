//! Pivot point calculations.
//! For each candle we compute the number of consecutive candles to the left/right
//! that satisfy the condition for a pivot high or pivot low.

use anyhow::Result;
use polars::prelude::*;

/// Maximum number of candles to look on each side when computing pivot strength.
const PIVOT_WINDOW: usize = 5000;

/// Add pivot strength columns to the DataFrame.
/// Expects columns "high" and "low" to exist.
pub fn add_pivot_features(mut df: DataFrame) -> Result<DataFrame> {
    // Extract high and low as vectors of f64 (NaNs become f64::NAN)
    let high_prices: Vec<f64> = df
        .column("high")?
        .f64()?
        .into_iter()
        .map(|opt| opt.unwrap_or(f64::NAN))
        .collect();
    let low_prices: Vec<f64> = df
        .column("low")?
        .f64()?
        .into_iter()
        .map(|opt| opt.unwrap_or(f64::NAN))
        .collect();

    // Compute strengths for highs: left = previous high < current high, right = next high < current high
    let (left_high, right_high) = consecutive_strength_vec(
        &high_prices,
        |prev, curr| prev < curr, // left condition: previous high is lower
        |next, curr| next < curr, // right condition: next high is lower
        PIVOT_WINDOW,
    );

    // Compute strengths for lows: left = previous low > current low, right = next low > current low
    let (left_low, right_low) = consecutive_strength_vec(
        &low_prices,
        |prev, curr| prev > curr, // left condition: previous low is higher
        |next, curr| next > curr, // right condition: next low is higher
        PIVOT_WINDOW,
    );

    let high_strength: Vec<u32> = left_high
        .iter()
        .zip(right_high.iter())
        .map(|(&l, &r)| l.min(r))
        .collect();
    let low_strength: Vec<u32> = left_low
        .iter()
        .zip(right_low.iter())
        .map(|(&l, &r)| l.min(r))
        .collect();

    // Add new columns to the DataFrame (in‑place modifications)
    df.with_column(Series::new("pivot_high_left".into(), left_high).into())?;
    df.with_column(Series::new("pivot_high_right".into(), right_high).into())?;
    df.with_column(Series::new("pivot_low_left".into(), left_low).into())?;
    df.with_column(Series::new("pivot_low_right".into(), right_low).into())?;
    df.with_column(Series::new("pivot_high_max".into(), high_max).into())?;
    df.with_column(Series::new("pivot_low_max".into(), low_max).into())?;

    Ok(df)
}

/// Helper: compute left and right consecutive counts for a price series.
/// Returns two vectors of u32 (left strengths, right strengths).
fn consecutive_strength_vec(
    prices: &[f64],
    left_cond: impl Fn(f64, f64) -> bool,
    right_cond: impl Fn(f64, f64) -> bool,
    window: usize,
) -> (Vec<u32>, Vec<u32>) {
    let n = prices.len();
    let mut left_strength = Vec::with_capacity(n);
    let mut right_strength = Vec::with_capacity(n);

    for i in 0..n {
        let curr = prices[i];
        if curr.is_nan() {
            left_strength.push(0);
            right_strength.push(0);
            continue;
        }

        // Count consecutive left candles that satisfy left_cond
        let mut left_count = 0;
        for j in (0..i).rev().take(window) {
            let prev = prices[j];
            if prev.is_nan() || !left_cond(prev, curr) {
                break;
            }
            left_count += 1;
        }
        left_strength.push(left_count);

        // Count consecutive right candles that satisfy right_cond
        let mut right_count = 0;
        for j in i + 1..n.min(i + 1 + window) {
            let next = prices[j];
            if next.is_nan() || !right_cond(next, curr) {
                break;
            }
            right_count += 1;
        }
        right_strength.push(right_count);
    }

    (left_strength, right_strength)
}
