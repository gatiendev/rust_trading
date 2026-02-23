//! Utility functions for monitoring and debugging.

use memory_stats::memory_stats;
use std::collections::VecDeque;
use std::future::Future;
use std::time::Instant;

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

/// Log estimated memory usage of key data structures, with values in MB.
pub fn log_memory_breakdown(raw_window: &VecDeque<Kline>, df: &DataFrame) {
    // Size of a single Kline (stack size; Kline has no heap allocations)
    let kline_size = std::mem::size_of::<Kline>(); // typically 72 bytes (7 f64 + 2 i64)

    // Raw window: capacity vs used
    let raw_cap = raw_window.capacity();
    let raw_used = raw_window.len();
    let raw_allocated = raw_cap * kline_size;
    let raw_used_bytes = raw_used * kline_size;

    // DataFrame – rough estimate (rows × cols × 8 bytes per cell, ignoring strings/overhead)
    let (rows, cols) = df.shape();
    let df_approx = rows * cols * 8;

    // Conversion factor: bytes to MB
    let to_mb = |bytes: usize| bytes as f64 / (1024.0 * 1024.0);

    println!("=== Memory Breakdown (MB) ===");
    println!("Kline struct size: {:.2} MB", to_mb(kline_size));
    println!(
        "Raw window: {} used / {} capacity → {:.2} MB allocated (used: {:.2} MB)",
        raw_used,
        raw_cap,
        to_mb(raw_allocated),
        to_mb(raw_used_bytes)
    );
    println!(
        "DataFrame approx: {:.2} MB ({} rows × {} cols)",
        to_mb(df_approx),
        rows,
        cols
    );
    println!(
        "Total approx (excl. overhead): {:.2} MB",
        to_mb(raw_allocated + df_approx)
    );
}

/// Measure the execution time of a closure and print it with a label.
/// Returns the value returned by the closure.
pub fn measure_time<T, F: FnOnce() -> T>(label: &str, f: F) -> T {
    let start = Instant::now();
    let result = f();
    let elapsed = start.elapsed();
    println!("{} took: {:.2} ms", label, elapsed.as_secs_f64() * 1000.0);
    result
}

pub async fn measure_time_async<F, T>(label: &str, f: F) -> T
where
    F: Future<Output = T>,
{
    let start = Instant::now();
    let result = f.await;
    let elapsed = start.elapsed();
    println!("{} took: {:.2} ms", label, elapsed.as_secs_f64() * 1000.0);
    result
}
