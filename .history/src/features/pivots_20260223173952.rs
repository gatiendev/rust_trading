//! Pivot point calculations (placeholder).
//! Future implementation: detect pivot highs/lows based on `n` left/right bars.

use anyhow::Result;
use polars::prelude::*;

/// Add pivot point columns to the DataFrame.
/// This is a stub – actual logic will be added later.
pub fn add_pivot_features(_df: DataFrame) -> Result<DataFrame> {
    // TODO: implement pivot detection
    // For now, return the input unchanged.
    Ok(_df)
}
