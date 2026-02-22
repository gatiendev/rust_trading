use crate::kline::Kline;
use anyhow::Result;
use reqwest::Client;
use std::time::Duration;

/// Fetch historical klines between start_time and end_time (milliseconds).
/// Automatically paginates with 1000 candles per request.
pub async fn fetch_klines_range(
    symbol: &str,
    interval: &str,
    mut start_time: i64,
    end_time: i64,
) -> Result<Vec<Kline>> {
    let client = Client::new();
    let mut all = Vec::new();
    let limit = 1000;

    loop {
        let url = format!(
            "https://api.binance.com/api/v3/klines?symbol={}&interval={}&startTime={}&endTime={}&limit={}",
            symbol, interval, start_time, end_time, limit
        );

        let response = client.get(&url).send().await?;
        let klines: Vec<Kline> = response.json().await?;

        if klines.is_empty() {
            break;
        }

        let last = klines.last().unwrap();
        all.extend(klines);

        if klines.len() < limit {
            break;
        }

        // Next batch starts after the last candle's close time
        start_time = last.close_time + 1;
        if start_time >= end_time {
            break;
        }

        // Polite delay to avoid rate limits
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    Ok(all)
}

/// Fetch the latest `count` candles for the given interval.
/// Works by computing an approximate start time and fetching forward.
pub async fn fetch_latest_klines(symbol: &str, interval: &str, count: usize) -> Result<Vec<Kline>> {
    let interval_ms = match interval {
        "15m" => 15 * 60 * 1000,
        "5m" => 5 * 60 * 1000,
        "1h" => 60 * 60 * 1000,
        // add other intervals as needed
        _ => anyhow::bail!("Unsupported interval: {}", interval),
    };

    let now = chrono::Utc::now().timestamp_millis();
    // Approximate start time: count intervals ago
    let start_time = now - (count as i64 * interval_ms);

    let mut klines = fetch_klines_range(symbol, interval, start_time, now).await?;

    // Binance returns candles in ascending order. If we got more than needed,
    // keep only the last 'count' candles (the most recent ones).
    if klines.len() > count {
        klines = klines.split_off(klines.len() - count);
    }

    Ok(klines)
}
