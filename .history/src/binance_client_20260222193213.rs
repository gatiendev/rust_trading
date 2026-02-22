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

        // Clone the last kline before we might move `klines`
        let last = klines.last().unwrap().clone();
        let batch_len = klines.len();

        // Add this batch to our collection (moves `klines`)
        all.extend(klines);

        // If we got fewer than the limit, this is the last batch
        if batch_len < limit {
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
