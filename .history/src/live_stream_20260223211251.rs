use crate::data_storage;
use crate::features;
use crate::kline::Kline;
use crate::utils;
use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use futures_util::future::join_all;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::collections::VecDeque;
use std::time::Instant;
use tokio;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use url::Url;

const HISTORICAL_COUNT: usize = 50_000;
const FEATURE_WINDOW_SIZE: usize = 50000;

fn format_time(ms: u64) -> String {
    let seconds = (ms / 1000) as i64;
    let nanos = ((ms % 1000) * 1_000_000) as u32;
    DateTime::<Utc>::from_timestamp(seconds, nanos)
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S%.3f UTC").to_string())
        .unwrap_or_else(|| "Invalid timestamp".to_string())
}

/// Run the live stream.
/// - `raw_window` – initial raw data window (50k)
/// - `raw_cache_file` – Parquet file for raw rolling window (overwritten)
/// - `raw_csv_file` – CSV file for raw data (appended)
/// - `feature_parquet` – Parquet file for feature‑enriched window (overwritten)
/// - `feature_csv` – CSV file for feature‑enriched window (overwritten)
pub async fn run(
    stream_type: &str,
    mut raw_window: VecDeque<Kline>, // was Vec<Kline>
    raw_cache_file: &str,
    raw_csv_file: &str,
    feature_parquet: &str,
    feature_csv: &str,
    streaming_csv_file: &str,
) -> Result<()> {
    let start = Instant::now();

    // Build initial feature window
    let feature_slice = utils::measure_time("collect feature slice", || {
        raw_window
            .iter()
            .skip(raw_window.len().saturating_sub(FEATURE_WINDOW_SIZE))
            .cloned()
            .collect::<Vec<Kline>>()
    });

    let features_df =
        utils::measure_time("features", || features::compute_features(&feature_slice))?;

    println!(
        "Initial features computed, shape: {:?}",
        features_df.shape()
    );

    // Save initial features

    let saving_start = Instant::now();

    utils::measure_time("save feature parquet", || {
        data_storage::save_dataframe_parquet(&mut features_df, feature_parquet)
    })?;

    utils::measure_time("save feature csv", || {
        data_storage::save_dataframe_csv_to_path(&mut features_df, feature_csv)
    })?;

    let saving_elapsed = saving_start.elapsed();

    println!(
        "saving took: {:.2} ms",
        saving_elapsed.as_secs_f64() * 1000.0
    );

    let tail = features_df.tail(Some(1));
    println!("Latest features: {:?}", tail);

    let stream_name = match stream_type {
        "trade" => "btcusdt@trade",
        "m5" => "btcusdt@kline_5m",
        "m15" => "btcusdt@kline_15m",
        _ => unreachable!(),
    };

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
    println!("Loaded {} historical klines for context.", raw_window.len());

    let initial_elapsed = start.elapsed();
    println!(
        "starting streamer took: {:.2} ms",
        initial_elapsed.as_secs_f64() * 1000.0
    );
    utils::print_memory_usage();

    utils::log_memory_breakdown(&raw_window, &features_df);

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
                                Some(open_str),
                                Some(high_str),
                                Some(low_str),
                                Some(close_str),
                                Some(volume_str),
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
                                let message_start = Instant::now();

                                let open_time_ms = open_time as i64;
                                let close_time_ms = close_time as i64;

                                let open = open_str.parse::<f64>()?;
                                let high = high_str.parse::<f64>()?;
                                let low = low_str.parse::<f64>()?;
                                let close = close_str.parse::<f64>()?;
                                let volume = volume_str.parse::<f64>()?;

                                let new_kline = Kline {
                                    open_time: open_time_ms,
                                    open,
                                    high,
                                    low,
                                    close,
                                    volume,
                                    close_time: close_time_ms,
                                };

                                // --- Update raw rolling window (50k) ---

                                raw_window.push_back(new_kline.clone());
                                if raw_window.len() > HISTORICAL_COUNT {
                                    raw_window.pop_front();
                                }

                                // --- Compute features on the last FEATURE_WINDOW_SIZE of raw_window ---
                                let feature_slice =
                                    utils::measure_time("collect feature slice", || {
                                        raw_window
                                            .iter()
                                            .skip(
                                                raw_window
                                                    .len()
                                                    .saturating_sub(FEATURE_WINDOW_SIZE),
                                            )
                                            .cloned()
                                            .collect::<Vec<Kline>>()
                                    });

                                let mut features_df =
                                    utils::measure_time("compute features", || {
                                        features::compute_features(&feature_slice)
                                    })?;

                                let saving_start = Instant::now();

                                let mut handles = Vec::new();

                                // 1. Save feature parquet (clone DataFrame)
                                let df_clone = features_df.clone();
                                let path = feature_parquet.to_string();
                                handles.push(tokio::spawn(async move {
                                    if let Err(e) =
                                        data_storage::save_dataframe_parquet_async(df_clone, path)
                                            .await
                                    {
                                        eprintln!("Error saving feature parquet: {}", e);
                                    }
                                }));

                                // 2. Save feature CSV (optional, but if you keep it)
                                let df_clone = features_df.clone();
                                let path = feature_csv.to_string();
                                handles.push(tokio::spawn(async move {
                                    if let Err(e) = data_storage::save_dataframe_csv_to_path_async(
                                        df_clone, path,
                                    )
                                    .await
                                    {
                                        eprintln!("Error saving feature CSV: {}", e);
                                    }
                                }));

                                // 3. Append feature row to streaming log
                                let df_clone = features_df.clone();
                                let path = streaming_csv_file.to_string();
                                handles.push(tokio::spawn(async move {
                                    if let Err(e) = data_storage::append_features_row_to_csv_async(
                                        df_clone, path,
                                    )
                                    .await
                                    {
                                        eprintln!("Error appending feature row: {}", e);
                                    }
                                }));

                                // 4. Append raw kline to raw CSV
                                let kline_clone = new_kline.clone();
                                let path = raw_csv_file.to_string();
                                handles.push(tokio::spawn(async move {
                                    if let Err(e) =
                                        data_storage::append_kline_to_csv_async(kline_clone, path)
                                            .await
                                    {
                                        eprintln!("Error appending raw kline: {}", e);
                                    }
                                }));

                                // 5. Save raw Parquet cache (collect raw window into Vec first)
                                let raw_slice: Vec<Kline> = raw_window.iter().cloned().collect();
                                let path = raw_cache_file.to_string();
                                handles.push(tokio::spawn(async move {
                                    if let Err(e) =
                                        data_storage::save_klines_to_parquet_async(raw_slice, path)
                                            .await
                                    {
                                        eprintln!("Error saving raw Parquet: {}", e);
                                    }
                                }));

                                // Wait for all spawned tasks to complete
                                join_all(handles).await;

                                let saving_elapsed = saving_start.elapsed();

                                println!(
                                    "saving took: {:.2} ms",
                                    saving_elapsed.as_secs_f64() * 1000.0
                                );

                                // --- Print basic kline info ---
                                let open_time_str = format_time(open_time);
                                let close_time_str = format_time(close_time);
                                let nominal_end =
                                    DateTime::<Utc>::from_timestamp_millis(open_time_ms)
                                        .map(|dt| dt + Duration::minutes(interval_minutes))
                                        .map(|dt| {
                                            dt.format("%Y-%m-%d %H:%M:%S%.3f UTC").to_string()
                                        })
                                        .unwrap_or_else(|| "Invalid".to_string());

                                let message_elapsed = message_start.elapsed();

                                println!(
                                    "Kline | Open: {} | Close (actual): {} | Nominal End: {} | High: {} | Low: {} | ClosePrice: {} | Volume: {}",
                                    open_time_str, close_time_str, nominal_end, high, low, close, volume
                                );
                                println!(
                                    "new message took: {:.2} ms",
                                    message_elapsed.as_secs_f64() * 1000.0
                                );
                                utils::print_memory_usage();

                                utils::log_memory_breakdown(&raw_window, &features_df);
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
