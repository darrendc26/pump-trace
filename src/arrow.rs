use arrow::datatypes::{DataType, Field, Schema};
use arrow::array::{Float64Array, StringArray};
use arrow::record_batch::RecordBatch;
use arrow::error::Result;
use std::sync::Arc;

use crate::process_data::PumpEvent;

pub fn launch_schema() -> Schema {
    Schema::new(vec![  // Schema::new, not Schema::from
        Field::new("signature", DataType::Utf8, false),
        Field::new("trader_public_key", DataType::Utf8, false),
        Field::new("tx_type", DataType::Utf8, false),
        Field::new("mint", DataType::Utf8, false),
        Field::new("sol_in_pool", DataType::Float64, false),
        Field::new("tokens_in_pool", DataType::Float64, false),
        Field::new("initial_buy", DataType::Float64, false),
        Field::new("sol_amount", DataType::Float64, false),
        Field::new("new_token_balance", DataType::Float64, false),
        Field::new("market_cap_sol", DataType::Float64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("uri", DataType::Utf8, false),
        Field::new("pool", DataType::Utf8, false),
    ])
}

pub fn trade_schema() -> Schema {
    Schema::new(vec![
        Field::new("signature", DataType::Utf8, false),
        Field::new("mint", DataType::Utf8, false),
        Field::new("trader_public_key", DataType::Utf8, false),
        Field::new("tx_type", DataType::Utf8, false),
        Field::new("token_amount", DataType::Float64, false),
        Field::new("sol_amount", DataType::Float64, false),
        Field::new("new_token_balance", DataType::Float64, false),
        Field::new("bonding_curve_key", DataType::Utf8, false),
        Field::new("v_tokens_in_bonding_curve", DataType::Float64, false),
        Field::new("v_sol_in_bonding_curve", DataType::Float64, false),
        Field::new("market_cap_sol", DataType::Float64, false),
        Field::new("pool", DataType::Utf8, false),
    ])
}

// Single event processing with official arrow
pub fn event_to_record_batch(event: &PumpEvent) -> Result<RecordBatch> {
    match event {
        PumpEvent::TokenLaunch {
            signature,
            traderPublicKey,
            txType,
            mint,
            solInPool,
            tokensInPool,
            initialBuy,
            solAmount,
            newTokenBalance,
            marketCapSol,
            name,
            symbol,
            uri,
            pool,
        } => {
            let schema = Arc::new(launch_schema());
            
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(StringArray::from(vec![signature.as_str()])),
                    Arc::new(StringArray::from(vec![traderPublicKey.as_str()])),
                    Arc::new(StringArray::from(vec![txType.as_str()])),
                    Arc::new(StringArray::from(vec![mint.as_str()])),
                    Arc::new(Float64Array::from(vec![*solInPool])),
                    Arc::new(Float64Array::from(vec![*tokensInPool])),
                    Arc::new(Float64Array::from(vec![*initialBuy])),
                    Arc::new(Float64Array::from(vec![*solAmount])),
                    Arc::new(Float64Array::from(vec![*newTokenBalance])),
                    Arc::new(Float64Array::from(vec![*marketCapSol])),
                    Arc::new(StringArray::from(vec![name.as_str()])),
                    Arc::new(StringArray::from(vec![symbol.as_str()])),
                    Arc::new(StringArray::from(vec![uri.as_str()])),
                    Arc::new(StringArray::from(vec![pool.as_str()])),
                ],
            )
        }
        
        PumpEvent::Trade {
            signature,
            mint,
            traderPublicKey,
            txType,
            tokenAmount,
            solAmount,
            newTokenBalance,
            bondingCurveKey,
            vTokensInBondingCurve,
            vSolInBondingCurve,
            marketCapSol,
            pool,
        } => {
            let schema = Arc::new(trade_schema());
            
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(StringArray::from(vec![signature.as_str()])),
                    Arc::new(StringArray::from(vec![mint.as_str()])),
                    Arc::new(StringArray::from(vec![traderPublicKey.as_str()])),
                    Arc::new(StringArray::from(vec![txType.as_str()])),
                    Arc::new(Float64Array::from(vec![*tokenAmount])),
                    Arc::new(Float64Array::from(vec![*solAmount])),
                    Arc::new(Float64Array::from(vec![*newTokenBalance])),
                    Arc::new(StringArray::from(vec![bondingCurveKey.as_str()])),
                    Arc::new(Float64Array::from(vec![*vTokensInBondingCurve])),
                    Arc::new(Float64Array::from(vec![*vSolInBondingCurve])),
                    Arc::new(Float64Array::from(vec![*marketCapSol])),
                    Arc::new(StringArray::from(vec![pool.as_str()])),
                ],
            )
        }
        
        PumpEvent::Unknown => {
            Err(arrow::error::ArrowError::InvalidArgumentError(
                "Cannot convert Unknown event type to Arrow format".to_string()
            ))
        }
    }
}
