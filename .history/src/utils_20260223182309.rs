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
