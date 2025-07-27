#![allow(non_snake_case)]
#![allow(dead_code)]
// use arrow::array::RecordBatch;
use tokio_tungstenite::connect_async;
use futures::{SinkExt, StreamExt};
// use serde::{Deserialize};
use serde_json::{json};
use url::Url;
use tracing:: warn;
use crate::process_data::PumpPipeline;

pub async fn ingest_ws_stream() {
    let mut pipeline = PumpPipeline::new("./pump_data", 2).unwrap();
    

    let url = Url::parse("wss://pumpportal.fun/api/data").unwrap();
    let (mut ws_stream, _) = connect_async(url).await.expect("Failed to connect");

    let sub_msg = json!({
        "method": "subscribeNewToken"
    });

    ws_stream.send(tokio_tungstenite::tungstenite::Message::Text(sub_msg.to_string()))
        .await
        .expect("Failed to send subscription");

    
    let sub_msg = json!({
        "method": "subscribeTokenTrade",
        "keys": ["6bfrXdoo8nZFosAER94ihMz7a4rSwu6A8ismAuVtpump",
            "9BadoUuov35qa2fujLdyqDYgMfCDuWuwDhJQ25yrpump"
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
                warn!("Failed to parse JSON: {:?}\nText: {:?}", e, text);
                continue;
            }
        };

       pipeline.process_data(text_json.clone()).await.expect("Failed to process data");
    };

    
}
