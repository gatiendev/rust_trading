use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use url::Url;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Binance WebSocket URL for real-time trades (BTCUSDT)
    let url = Url::parse("wss://stream.binance.com:9443/ws/btcusdt@trade")?;

    println!("Connecting to Binance WebSocket: {}", url);
    let (ws_stream, _) = connect_async(url).await?;
    println!("Connected!");

    let (mut write, mut read) = ws_stream.split();

    // Optional: send a subscription message (Binance streams don't require it for public streams,
    // but you can send a custom message if needed)
    // let subscribe_msg = serde_json::json!({
    //     "method": "SUBSCRIBE",
    //     "params": ["btcusdt@trade"],
    //     "id": 1
    // }).to_string();
    // write.send(Message::Text(subscribe_msg)).await?;

    // Process incoming messages
    while let Some(message) = read.next().await {
        match message? {
            Message::Text(text) => {
                // Parse JSON
                let data: Value = serde_json::from_str(&text)?;
                // Extract fields (Binance trade stream format)
                if let (Some(price), Some(qty), Some(time)) = (
                    data["p"].as_str(),  // price
                    data["q"].as_str(),  // quantity
                    data["T"].as_u64(),  // timestamp
                ) {
                    println!(
                        "Time: {} | Price: {} | Qty: {}",
                        time, price, qty
                    );
                }
            }
            Message::Ping(payload) => {
                // Respond to ping to keep connection alive
                write.send(Message::Pong(payload)).await?;
            }
            _ => {}
        }
    }

    Ok(())
}
