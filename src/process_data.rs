#![allow(non_snake_case)]
#![allow(dead_code)]

use serde_json::Value;
use tracing::{info, warn};
use crate::arrow::event_to_record_batch;
use serde::{Deserialize};
use crate::parquet_storage::ParquetStorage;
use arrow::record_batch::RecordBatch;

#[derive(Debug, Deserialize)]

pub enum PumpEvent {
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

    Unknown,
}


pub struct PumpPipeline {
    pub storage: ParquetStorage,
    pub launch_buffer: Vec<RecordBatch>,
    pub trade_buffer: Vec<RecordBatch>,
    pub buffer_size: usize,
}

impl PumpPipeline {
   pub fn new(storage_path: &str, buffer_size: usize) -> Result<Self, Box<dyn std::error::Error>> {
        let storage = ParquetStorage::new(storage_path.to_string())?;
        
            Ok(Self {
            storage,
            launch_buffer: Vec::new(),
            trade_buffer: Vec::new(),
            buffer_size,
        })
    }


pub fn process_data(&mut self, text_json: Value) -> Result<(), Box<dyn std::error::Error>> {
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

            let batch = event_to_record_batch(&token); 
            self.launch_buffer.push(batch?);
            if self.launch_buffer.len() >= self.buffer_size {
                self.storage.write_batch(&self.launch_buffer, "token_launch")?;
                self.launch_buffer.clear();
                println!("Pushed to launch buffer");
            }
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

            let batch = event_to_record_batch(&trade);
            self.trade_buffer.push(batch?);

            if self.trade_buffer.len() >= self.buffer_size {
                self.storage.write_batch(&self.trade_buffer, "trade")?;
                self.trade_buffer.clear();
                println!("Pushed to trade buffer");
            }
        }

                Some(other) => {
                    warn!(" Unhandled txType: {}", other);
                }

                _ => {
                    warn!("txType is not a string");
                }
            }
        } else {
            info!(" Non-event/system message");
        }
        Ok(())
    }

}