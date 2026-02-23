//! Utility functions for monitoring and debugging.

use anyhow::Result;
use memory_stats::memory_stats;

/// Print current memory usage (RSS) in MB to stdout.
/// If memory stats are unavailable, prints a warning.
pub fn print_memory_usage() {
    match memory_stats() {
        Some(ms) => {
            let rss_mb = ms.physical_mem as f64 / (1024.0 * 1024.0);
            let vms_mb = ms.virtual_mem as f64 / (1024.0 * 1024.0);
            println!("[Memory] RSS: {:.2} MB, Virtual: {:.2} MB", rss_mb, vms_mb);
        }
        None => {
            eprintln!("[Memory] Unable to obtain memory stats on this platform.");
        }
    }
}

use crate::kline::Kline;
use polars::prelude::DataFrame;

/// Log estimated memory usage of key data structures.
pub fn log_memory_breakdown(
    raw_window: &VecDeque<Kline>,
    feature_window: &Vec<Kline>,
    df: &DataFrame,
) {
    // Size of a single Kline (stack size; Kline has no heap allocations)
    let kline_size = std::mem::size_of::<Kline>(); // typically 56 bytes (7 f64 + 2 i64)

    // Raw window: capacity vs used
    let raw_cap = raw_window.capacity();
    let raw_used = raw_window.len();
    let raw_allocated = raw_cap * kline_size;
    let raw_used_bytes = raw_used * kline_size;

    // Feature window
    let feature_cap = feature_window.capacity();
    let feature_used = feature_window.len();
    let feature_allocated = feature_cap * kline_size;
    let feature_used_bytes = feature_used * kline_size;

    // DataFrame – rough estimate (rows × cols × 8 bytes per cell, ignoring strings/overhead)
    let (rows, cols) = df.shape();
    let df_approx = rows * cols * 8;

    println!("=== Memory Breakdown ===");
    println!("Kline struct size: {} bytes", kline_size);
    println!(
        "Raw window: {} used / {} capacity → {} bytes allocated (used: {} bytes)",
        raw_used, raw_cap, raw_allocated, raw_used_bytes
    );
    println!(
        "Feature window: {} used / {} capacity → {} bytes allocated (used: {} bytes)",
        feature_used, feature_cap, feature_allocated, feature_used_bytes
    );
    println!(
        "DataFrame approx: {} bytes ({} rows × {} cols)",
        df_approx, rows, cols
    );
    println!(
        "Total approx (excl. overhead): {} bytes",
        raw_allocated + feature_allocated + df_approx
    );
}
