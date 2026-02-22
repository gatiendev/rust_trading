use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use url::Url;

/// Convert milliseconds since UNIX epoch to a human‑readable UTC string.
fn format_time(ms: u64) -> String {
    // chrono expects nanoseconds, so multiply by 1,000,000
    let seconds = (ms / 1000) as i64;
    let nanos = ((ms % 1000) * 1_000_000) as u32;
    DateTime::<Utc>::from_timestamp(seconds, nanos)
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S%.3f UTC").to_string())
        .unwrap_or_else(|| "Invalid timestamp".to_string())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Read command-line argument to choose stream type
    let args: Vec<String> = std::env::args().collect();
    let stream_type = args.get(1).map(|s| s.as_str()).unwrap_or("trade");

    // Build the WebSocket URL based on the chosen stream
    let stream_name = match stream_type {
        "trade" => "btcusdt@trade",
        "m5" => "btcusdt@kline_5m",
        "m15" => "btcusdt@kline_15m",
        _ => {
            eprintln!("Unknown stream type. Use 'trade', 'm5', or 'm15'.");
            std::process::exit(1);
        }
    };
    let url_str = format!("wss://stream.binance.com:9443/ws/{}", stream_name);
    let url = Url::parse(&url_str)?;

    println!("Connecting to Binance WebSocket: {}", url);
    let (ws_stream, _) = connect_async(url).await?;
    println!("Connected! Streaming '{}'", stream_name);

    let (mut write, mut read) = ws_stream.split();

    // Process incoming messages
    while let Some(message) = read.next().await {
        match message? {
            Message::Text(text) => {
                // Parse JSON
                let data: Value = serde_json::from_str(&text)?;

                // Handle different stream types
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
                        // Kline data is inside the "k" object
                        if let Some(kline) = data["k"].as_object() {
                            if let (
                                Some(open),
                                Some(high),
                                Some(low),
                                Some(close),
                                Some(volume),
                                Some(close_time),
                            ) = (
                                kline["o"].as_str(),
                                kline["h"].as_str(),
                                kline["l"].as_str(),
                                kline["c"].as_str(),
                                kline["v"].as_str(),
                                kline["T"].as_u64(), // close time of the kline
                            ) {
                                let time_str = format_time(close_time);
                                println!(
                                    "Kline | CloseTime: {} | Open: {} | High: {} | Low: {} | Close: {} | Volume: {}",
                                    time_str, open, high, low, close, volume
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
