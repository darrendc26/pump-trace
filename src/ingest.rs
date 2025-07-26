use tokio_tungstenite::connect_async;
use futures::{SinkExt, StreamExt};
use serde::{Deserialize};
use serde_json::{json};
use url::Url;
use tracing::{info, warn, Value};

#[derive(Debug, Deserialize)]
// #[serde(tag = "txType")]
pub enum PumpEvent {
    #[serde(rename = "create")]
    TokenLaunch {
        signature: String,
        traderPublicKey: String,
        txType: String,
        mint: String,
        solInPool: f64,
        tokensInPool: f64,
        initialBuy: f64,
        solAmount: f64,
        newTokenBalance: f64,
        marketCapSol: f64,
        name: String,
        symbol: String,
        uri: String,
        pool: String,
    },

    #[serde(rename = "buy")]
    Trade {
        signature: String,
        mint: String,
        traderPublicKey: String,
        txType: String,
        tokenAmount: f64,
        solAmount: f64,
        newTokenBalance: f64,
        bondingCurveKey: String,
        vTokensInBondingCurve: f64,
        vSolInBondingCurve: f64,
        marketCapSol: f64,
        pool: String,
    },

    #[serde(other)]
    Unknown,
}


pub async fn ingest_ws_stream() {
    let url = Url::parse("wss://pumpportal.fun/api/data").unwrap();
    let (mut ws_stream, _) = connect_async(url).await.expect("Failed to connect");

    // let sub_msg = json!({
    //     "method": "subscribeNewToken"
    // });

    // ws_stream.send(tokio_tungstenite::tungstenite::Message::Text(sub_msg.to_string()))
    //     .await
    //     .expect("Failed to send subscription");

    
    let sub_msg = json!({
        "method": "subscribeTokenTrade",
        "keys": ["BdNBU4SjC4BuWFQmwwFWcRMfX1Ew78oPBjnkWxbbpump",
            "6o9xFdWCaqghJNbNJW7VxGJaiRN55WRoqZXzjiYopump"
        ]
    });

    ws_stream.send(tokio_tungstenite::tungstenite::Message::Text(sub_msg.to_string()))
        .await
        .expect("Failed to send subscription");


    let (_, mut read) = ws_stream.split();
    while let Some(msg) = read.next().await {
        let msg = msg.expect("Failed to receive message");
        let text = msg.to_text().expect("Expected message to be text");
        // println!("Received message: {}", text);
        let text_json: serde_json::Value = match serde_json::from_str(text) {
            Ok(val) => val,
            Err(e) => {
                warn!("❌ Failed to parse JSON: {:?}\nText: {:?}", e, text);
                continue;
            }
        };
 if let Some(tx_type_val) = text_json.get("txType") {
    match tx_type_val.as_str() {
        Some("create") => {
            let token = PumpEvent::TokenLaunch {
                signature: text_json["signature"].as_str().unwrap_or("").to_string(),
                traderPublicKey: text_json["traderPublicKey"].as_str().unwrap_or("").to_string(),
                txType: text_json["txType"].as_str().unwrap_or("").to_string(),
                mint: text_json["mint"].as_str().unwrap_or("").to_string(),
                solInPool: text_json["solInPool"].as_f64().unwrap_or(0.0),
                tokensInPool: text_json["tokensInPool"].as_f64().unwrap_or(0.0),
                initialBuy: text_json["initialBuy"].as_f64().unwrap_or(0.0),
                solAmount: text_json["solAmount"].as_f64().unwrap_or(0.0),
                newTokenBalance: text_json["newTokenBalance"].as_f64().unwrap_or(0.0),
                marketCapSol: text_json["marketCapSol"].as_f64().unwrap_or(0.0),
                name: text_json["name"].as_str().unwrap_or("").to_string(),
                symbol: text_json["symbol"].as_str().unwrap_or("").to_string(),
                uri: text_json["uri"].as_str().unwrap_or("").to_string(),
                pool: text_json["pool"].as_str().unwrap_or("").to_string(),
            };

            // Now do something with it
            println!("🚀 Token launch: {:#?}", token);
        }

        Some("buy") | Some("sell") => {
            let trade = PumpEvent::Trade {
                signature: text_json["signature"].as_str().unwrap_or("").to_string(),
                mint: text_json["mint"].as_str().unwrap_or("").to_string(),
                traderPublicKey: text_json["traderPublicKey"].as_str().unwrap_or("").to_string(),
                txType: text_json["txType"].as_str().unwrap_or("").to_string(),
                tokenAmount: text_json["tokenAmount"].as_f64().unwrap_or(0.0),
                solAmount: text_json["solAmount"].as_f64().unwrap_or(0.0),
                newTokenBalance: text_json["newTokenBalance"].as_f64().unwrap_or(0.0),
                bondingCurveKey: text_json["bondingCurveKey"].as_str().unwrap_or("").to_string(),
                vTokensInBondingCurve: text_json["vTokensInBondingCurve"].as_f64().unwrap_or(0.0),
                vSolInBondingCurve: text_json["vSolInBondingCurve"].as_f64().unwrap_or(0.0),
                marketCapSol: text_json["marketCapSol"].as_f64().unwrap_or(0.0),
                pool: text_json["pool"].as_str().unwrap_or("").to_string(),
            };
            println!("💸 Trade: {:#?}", trade);
        }

                Some(other) => {
                    warn!("❓ Unhandled txType: {}", other);
                }

                _ => {
                    warn!("txType is not a string");
                }
            }
        } else {
            info!("ℹ️ Non-event/system message: {}", text);
        }
    };

}
