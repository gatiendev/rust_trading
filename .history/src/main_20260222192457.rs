use chrono::{DateTime, Duration, Utc};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use url::Url;

use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Kline {
    open_time: i64, // in milliseconds
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
    close_time: i64, // in milliseconds
                     // ignore the rest (quote asset volume, number of trades, etc.)
}

// Custom deserializer because Binance returns an array, not a named object.
impl<'de> Deserialize<'de> for Kline {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // Visit a sequence of 12 elements
        struct KlineVisitor;

        impl<'de> serde::de::Visitor<'de> for KlineVisitor {
            type Value = Kline;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("an array of 12 values")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Kline, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let open_time: i64 = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(0, &self))?;
                let open: String = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(1, &self))?;
                let high: String = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(2, &self))?;
                let low: String = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(3, &self))?;
                let close: String = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(4, &self))?;
                let volume: String = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(5, &self))?;
                let close_time: i64 = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(6, &self))?;
                // Skip the remaining 5 fields (quote asset volume, number of trades, etc.)
                for _ in 0..5 {
                    let _: serde_json::Value = seq
                        .next_element()?
                        .ok_or_else(|| serde::de::Error::invalid_length(7, &self))?;
                }

                Ok(Kline {
                    open_time,
                    open: open.parse().map_err(serde::de::Error::custom)?,
                    high: high.parse().map_err(serde::de::Error::custom)?,
                    low: low.parse().map_err(serde::de::Error::custom)?,
                    close: close.parse().map_err(serde::de::Error::custom)?,
                    volume: volume.parse().map_err(serde::de::Error::custom)?,
                    close_time,
                })
            }
        }

        deserializer.deserialize_seq(KlineVisitor)
    }
}

fn format_time(ms: u64) -> String {
    let seconds = (ms / 1000) as i64;
    let nanos = ((ms % 1000) * 1_000_000) as u32;
    DateTime::<Utc>::from_timestamp(seconds, nanos)
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S%.3f UTC").to_string())
        .unwrap_or_else(|| "Invalid timestamp".to_string())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let stream_type = args.get(1).map(|s| s.as_str()).unwrap_or("trade");

    let stream_name = match stream_type {
        "trade" => "btcusdt@trade",
        "m5" => "btcusdt@kline_5m",
        "m15" => "btcusdt@kline_15m",
        _ => {
            eprintln!("Unknown stream type. Use 'trade', 'm5', or 'm15'.");
            std::process::exit(1);
        }
    };

    // Determine candle duration in minutes (for nominal end time calculation)
    let interval_minutes = match stream_type {
        "m5" => 5,
        "m15" => 15,
        _ => 0,
    };

    let url_str = format!("wss://stream.binance.com:9443/ws/{}", stream_name);
    let url = Url::parse(&url_str)?;

    println!("Connecting to Binance WebSocket: {}", url);
    let (ws_stream, _) = connect_async(url).await?;
    println!("Connected! Streaming '{}'", stream_name);

    let (mut write, mut read) = ws_stream.split();

    while let Some(message) = read.next().await {
        match message? {
            Message::Text(text) => {
                let data: Value = serde_json::from_str(&text)?;

                match stream_type {
                    "trade" => {
                        if let (Some(price), Some(qty), Some(time)) =
                            (data["p"].as_str(), data["q"].as_str(), data["T"].as_u64())
                        {
                            let time_str = format_time(time);
                            println!(
                                "Trade | Time: {} | Price: {} | Qty: {}",
                                time_str, price, qty
                            );
                        }
                    }
                    "m5" | "m15" => {
                        if let Some(kline) = data["k"].as_object() {
                            // Only process closed candles
                            if let Some(is_closed) = kline["x"].as_bool() {
                                if !is_closed {
                                    continue;
                                }
                            }

                            if let (
                                Some(open),
                                Some(high),
                                Some(low),
                                Some(close),
                                Some(volume),
                                Some(open_time),
                                Some(close_time),
                            ) = (
                                kline["o"].as_str(),
                                kline["h"].as_str(),
                                kline["l"].as_str(),
                                kline["c"].as_str(),
                                kline["v"].as_str(),
                                kline["t"].as_u64(),
                                kline["T"].as_u64(),
                            ) {
                                let open_time_str = format_time(open_time);
                                let close_time_str = format_time(close_time);

                                // Compute nominal end time = open_time + interval
                                let nominal_end = DateTime::<Utc>::from_timestamp(
                                    (open_time / 1000) as i64,
                                    ((open_time % 1000) * 1_000_000) as u32,
                                )
                                .map(|dt| dt + Duration::minutes(interval_minutes))
                                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S%.3f UTC").to_string())
                                .unwrap_or_else(|| "Invalid".to_string());

                                println!(
                                    "Kline | Open: {} | Close (actual): {} | Nominal End: {} | High: {} | Low: {} | ClosePrice: {} | Volume: {}",
                                    open_time_str, close_time_str, nominal_end, high, low, close, volume
                                );
                            }
                        }
                    }
                    _ => unreachable!(),
                }
            }
            Message::Ping(payload) => {
                write.send(Message::Pong(payload)).await?;
            }
            _ => {}
        }
    }

    Ok(())
}
