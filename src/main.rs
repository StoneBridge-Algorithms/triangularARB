use std::time::Instant;

use anyhow::Result;
use env_logger::Builder;
use futures_util::StreamExt;
use log::info;
use serde::Deserialize;
use serde_json::Value;

use binance_spot_connector_rust::{
    market_stream::agg_trade::AggTradeStream,
    tokio_tungstenite::BinanceWebSocketClient,
};

#[derive(Debug, Deserialize)]
struct TradeData {
    s: String,
    p: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    // initialize logger
    Builder::from_default_env()
        .filter(None, log::LevelFilter::Info)
        .init();

    // connect and subscribe
    let (mut ws, _) = BinanceWebSocketClient::connect_async_default()
        .await
        .expect("WS connect failed");
    ws.subscribe(vec![
        &AggTradeStream::new("BTCUSDT").into(),
        &AggTradeStream::new("ETHBTC").into(),
        &AggTradeStream::new("ETHUSDT").into(),
    ]).await;

    // print column headers
    info!(
        "{:<10} {:<15} {:<15} {:<15} {:<10} {:<10} {:<15} {:<12}",
        "Δµs", "BTCUSDT", "ETHBTC", "ETHUSDT", "Y", "Profit", "CumProfit", "CumReturn%"
    );

    // timing + state
    let mut last = Instant::now();
    let mut btc_price = "-".to_string();
    let mut eth_btc_price = "-".to_string();
    let mut eth_usdt_price = "-".to_string();

    // triangular-arb setup
    let initial_capital = 1000.0_f64;       // in USDT; adjust as you like
    let mut cumulative_profit = 0.0_f64;

    while let Some(msg) = ws.as_mut().next().await {
        match msg {
            Ok(frame) => {
                if let Ok(txt) = std::str::from_utf8(&frame.into_data()) {
                    if let Ok(wrapper) = serde_json::from_str::<Value>(txt) {
                        if let Some(data_val) = wrapper.get("data") {
                            if data_val.is_object() {
                                if let Ok(trade) =
                                    serde_json::from_value::<TradeData>(data_val.clone())
                                {
                                    match trade.s.as_str() {
                                        "BTCUSDT" => btc_price = trade.p.clone(),
                                        "ETHBTC"  => eth_btc_price = trade.p.clone(),
                                        "ETHUSDT" => eth_usdt_price = trade.p.clone(),
                                        _ => {},
                                    }

                                    // Δµs
                                    let now = Instant::now();
                                    let delta_us = now.duration_since(last).as_micros();
                                    last = now;
                                    let delta_str = format!("+{}µs", delta_us);

                                    // attempt parse all three prices
                                    if let (Ok(p1), Ok(p2), Ok(p3)) = (
                                        btc_price.parse::<f64>(),
                                        eth_btc_price.parse::<f64>(),
                                        eth_usdt_price.parse::<f64>(),
                                    ) {
                                        // compute Y
                                        let y = (1.0 / p1) * (1.0 / p2) * p3;

                                        // if opportunity
                                        if y > 1.005 {
                                            let profit = (y - 1.0) * initial_capital;
                                            cumulative_profit += profit;
                                            let cumulative_return = cumulative_profit / initial_capital * 100.0;

                                            info!(
                                                "{:<10} {:<15} {:<15} {:<15} {:<10.6} {:<10.6} {:<15.6} {:<12.3}",
                                                delta_str,
                                                btc_price,
                                                eth_btc_price,
                                                eth_usdt_price,
                                                y,
                                                profit,
                                                cumulative_profit,
                                                cumulative_return
                                            );
                                            continue;
                                        }
                                    }

                                    // otherwise just log the prices & Δµs
                                    info!(
                                        "{:<10} {:<15} {:<15} {:<15} {:<10} {:<10} {:<15} {:<12}",
                                        delta_str, btc_price, eth_btc_price, eth_usdt_price,
                                        "-", "-", "-", "-"
                                    );
                                }
                            }
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("WebSocket error: {}", e);
                break;
            }
        }
    }

    ws.close().await.expect("WS close failed");
    Ok(())
}
