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

/// Get current RSS (resident set size) in bytes, if available.
pub fn current_rss_bytes() -> Option<u64> {
    memory_stats().map(|ms| ms.physical_mem)
}

use crate::kline::Kline;
use polars::prelude::DataFrame;

/// Log estimated memory usage of key data structures.
pub fn log_memory_breakdown(raw_window: &[Kline], feature_window: &[Kline], df: &DataFrame) {
    // Size of a single Kline (shallow)
    let kline_size = std::mem::size_of::<Kline>(); // usually 56 bytes (7 f64 + 2 i64)

    // Raw window: capacity * size
    let raw_cap = raw_window.capacity();
    let raw_used = raw_window.len();
    let raw_allocated = raw_cap * kline_size;
    let raw_used_bytes = raw_used * kline_size;

    // Feature window
    let feature_cap = feature_window.capacity();
    let feature_used = feature_window.len();
    let feature_allocated = feature_cap * kline_size;
    let feature_used_bytes = feature_used * kline_size;

    // DataFrame – Polars does not expose a direct size method, but you can approximate
    // by summing the size of its columns. This is non‑trivial; a rough estimate:
    let df_approx = df.shape().0 * df.shape().1 * 8; // rows * cols * 8 bytes per f64 (oversimplified)

    println!("=== Memory Breakdown ===");
    println!("Kline struct size: {} bytes", kline_size);
    println!(
        "Raw window: {} used / {} capacity → {} bytes allocated",
        raw_used, raw_cap, raw_allocated
    );
    println!(
        "Feature window: {} used / {} capacity → {} bytes allocated",
        feature_used, feature_cap, feature_allocated
    );
    println!(
        "DataFrame approx: {} bytes ({} rows × {} cols)",
        df_approx,
        df.shape().0,
        df.shape().1
    );
    println!(
        "Total approx (excl. overhead): {} bytes",
        raw_allocated + feature_allocated + df_approx
    );
}
