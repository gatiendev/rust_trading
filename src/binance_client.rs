use crate::kline::Kline;
use anyhow::Result;
use chrono::{DateTime, Utc};
use reqwest::Client;
use std::time::Duration;

/// Convert milliseconds to human-readable UTC time.
fn format_time(ms: i64) -> String {
    DateTime::<Utc>::from_timestamp_millis(ms)
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S%.3f UTC").to_string())
        .unwrap_or_else(|| format!("Invalid timestamp {}", ms))
}

/// Fetch historical klines between start_time and end_time (milliseconds).
/// Automatically paginates with 1000 candles per request.
/// Logs progress to stdout.
pub async fn fetch_klines_range(
    symbol: &str,
    interval: &str,
    start_time: i64,
    end_time: i64,
) -> Result<Vec<Kline>> {
    println!(
        "Fetching {} {} klines from {} to {}",
        symbol,
        interval,
        format_time(start_time),
        format_time(end_time)
    );

    let client = Client::new();
    let mut all = Vec::new();
    let limit = 1000;
    let mut current_start = start_time;
    let mut batch_num = 0;

    loop {
        batch_num += 1;
        let url = format!(
            "https://api.binance.com/api/v3/klines?symbol={}&interval={}&startTime={}&endTime={}&limit={}",
            symbol, interval, current_start, end_time, limit
        );

        let response = client.get(&url).send().await?;
        let klines: Vec<Kline> = response.json().await?;

        if klines.is_empty() {
            println!("No more klines returned, stopping.");
            break;
        }

        let batch_len = klines.len();
        // Clone the last kline before moving `klines`
        let last = klines.last().unwrap().clone();

        all.extend(klines);
        let total = all.len();

        println!(
            "Batch {}: fetched {} klines (total so far: {})",
            batch_num, batch_len, total
        );

        // If we got fewer than the limit, this is the last batch
        if batch_len < limit {
            println!(
                "Received less than limit ({} < {}), assuming end of data.",
                batch_len, limit
            );
            break;
        }

        // Next batch starts after the last candle's close time
        current_start = last.close_time + 1;
        if current_start >= end_time {
            println!("Reached end time, stopping.");
            break;
        }

        // Polite delay to avoid rate limits
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    println!("Fetched total {} klines.", all.len());
    Ok(all)
}

/// Fetch the latest `count` candles for the given interval.
/// Uses an approximate start time based on interval duration.
pub async fn fetch_latest_klines(symbol: &str, interval: &str, count: usize) -> Result<Vec<Kline>> {
    let interval_ms = match interval {
        "15m" => 15 * 60 * 1000,
        "5m" => 5 * 60 * 1000,
        "1h" => 60 * 60 * 1000,
        _ => anyhow::bail!("Unsupported interval: {}", interval),
    };

    let now = Utc::now().timestamp_millis();
    let start_time = now - (count as i64 * interval_ms);

    println!(
        "Fetching latest {} {} klines (approx. from {} to {})",
        count,
        interval,
        format_time(start_time),
        format_time(now)
    );

    let mut klines = fetch_klines_range(symbol, interval, start_time, now).await?;

    if klines.len() > count {
        println!(
            "Trimmed {} klines to the most recent {}.",
            klines.len(),
            count
        );
        klines = klines.split_off(klines.len() - count);
    }

    println!("Successfully fetched {} klines.", klines.len());
    Ok(klines)
}
