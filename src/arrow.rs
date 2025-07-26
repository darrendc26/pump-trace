use arrow2::datatypes::{DataType, Field, Schema};
use arrow2::array::{Float64Array, Utf8Array};
use arrow2::error::Result;
use arrow2::chunk::Chunk;
use arrow2::array::Array;
use tracing::event;
use std::sync::Arc;
use crate::ingest::PumpEvent;

pub fn launch_schema() -> Schema {
    Schema::from(vec![
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
    Schema::from(vec![
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

// Option 1: Single Event Processing (Recommended)
pub fn event_to_chunk(event: &PumpEvent) -> Result<(Schema, Chunk<Box<dyn Array>>)> {
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
            let schema = launch_schema();
            let chunk = Chunk::new(vec![
                Box::new(Utf8Array::<i32>::from([Some(signature.as_str())])) as Box<dyn Array>,
                Box::new(Utf8Array::<i32>::from([Some(traderPublicKey.as_str())])),
                Box::new(Utf8Array::<i32>::from([Some(txType.as_str())])),
                Box::new(Utf8Array::<i32>::from([Some(mint.as_str())])),
                Box::new(Float64Array::from([Some(*solInPool)])),
                Box::new(Float64Array::from([Some(*tokensInPool)])),
                Box::new(Float64Array::from([Some(*initialBuy)])),
                Box::new(Float64Array::from([Some(*solAmount)])),
                Box::new(Float64Array::from([Some(*newTokenBalance)])),
                Box::new(Float64Array::from([Some(*marketCapSol)])),
                Box::new(Utf8Array::<i32>::from([Some(name.as_str())])),
                Box::new(Utf8Array::<i32>::from([Some(symbol.as_str())])),
                Box::new(Utf8Array::<i32>::from([Some(uri.as_str())])),
                Box::new(Utf8Array::<i32>::from([Some(pool.as_str())])),
            ]);
            Ok((schema, chunk))
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
            let schema = trade_schema();
            let chunk = Chunk::new(vec![
                Box::new(Utf8Array::<i32>::from([Some(signature.as_str())])) as Box<dyn Array>,
                Box::new(Utf8Array::<i32>::from([Some(mint.as_str())])),
                Box::new(Utf8Array::<i32>::from([Some(traderPublicKey.as_str())])),
                Box::new(Utf8Array::<i32>::from([Some(txType.as_str())])),
                Box::new(Float64Array::from([Some(*tokenAmount)])),
                Box::new(Float64Array::from([Some(*solAmount)])),
                Box::new(Float64Array::from([Some(*newTokenBalance)])),
                Box::new(Utf8Array::<i32>::from([Some(bondingCurveKey.as_str())])),
                Box::new(Float64Array::from([Some(*vTokensInBondingCurve)])),
                Box::new(Float64Array::from([Some(*vSolInBondingCurve)])),
                Box::new(Float64Array::from([Some(*marketCapSol)])),
                Box::new(Utf8Array::<i32>::from([Some(pool.as_str())])),
            ]);
            Ok((schema, chunk))
        }
        
        PumpEvent::Unknown => {
            Err(arrow2::error::Error::InvalidArgumentError(
                "Cannot convert Unknown event type to Arrow format".to_string()
            ))
        }
    }
}
