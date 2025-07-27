use sqlx::{PgPool, postgres::PgPoolOptions, Row};
use std::error::Error;
use serde_json::Value;

pub struct PumpPostgres {
    pool: PgPool,  
}

impl PumpPostgres {
    pub async fn new() -> Result<Self, Box<dyn Error>> {
        let database_url ="postgresql://darren:darren@localhost:5432/pumpdb";
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(database_url)
            .await?;

        let postgres = Self { pool };
        
        // Setup tables immediately
        postgres.setup_tables().await?;
        
        Ok(postgres)
    }   

    pub async fn setup_tables(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Create token launches table
        sqlx::query("
            CREATE TABLE IF NOT EXISTS token_launches (
                id SERIAL PRIMARY KEY,
                signature TEXT UNIQUE NOT NULL,
                trader_public_key TEXT NOT NULL,
                tx_type TEXT NOT NULL,
                mint TEXT NOT NULL,
                sol_in_pool DOUBLE PRECISION NOT NULL,
                tokens_in_pool DOUBLE PRECISION NOT NULL,
                initial_buy DOUBLE PRECISION NOT NULL,
                sol_amount DOUBLE PRECISION NOT NULL,
                new_token_balance DOUBLE PRECISION NOT NULL,
                market_cap_sol DOUBLE PRECISION NOT NULL,
                name TEXT NOT NULL,
                symbol TEXT NOT NULL,
                uri TEXT NOT NULL,
                pool TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            )")
        .execute(&self.pool)
        .await?;

        println!("✅ PostgreSQL token launches tables");

        // Create token trades table
        sqlx::query("
            CREATE TABLE IF NOT EXISTS trades (
                id SERIAL PRIMARY KEY,
                signature TEXT UNIQUE NOT NULL,
                mint TEXT NOT NULL,
                trader_public_key TEXT NOT NULL,
                tx_type TEXT NOT NULL,
                token_amount DOUBLE PRECISION NOT NULL,
                sol_amount DOUBLE PRECISION NOT NULL,
                new_token_balance DOUBLE PRECISION NOT NULL,
                bonding_curve_key TEXT NOT NULL,
                v_tokens_in_bonding_curve DOUBLE PRECISION NOT NULL,
                v_sol_in_bonding_curve DOUBLE PRECISION NOT NULL,
                market_cap_sol DOUBLE PRECISION NOT NULL,
                pool TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            )")
        .execute(&self.pool)
        .await?;

        println!("✅ PostgreSQL Trades tables");
        Ok(())
    }

    pub async fn push_token_launch(&self, token_launch: &Value) -> Result<(), Box<dyn std::error::Error>> {
        let signature = token_launch["signature"].as_str().unwrap_or("");
        let trader_public_key = token_launch["traderPublicKey"].as_str().unwrap_or("");
        let tx_type = token_launch["txType"].as_str().unwrap_or("");
        let mint = token_launch["mint"].as_str().unwrap_or("");
        let sol_in_pool = token_launch["solInPool"].as_f64().unwrap_or(0.0);
        let tokens_in_pool = token_launch["tokensInPool"].as_f64().unwrap_or(0.0);
        let initial_buy = token_launch["initialBuy"].as_f64().unwrap_or(0.0);
        let sol_amount = token_launch["solAmount"].as_f64().unwrap_or(0.0);
        let new_token_balance = token_launch["newTokenBalance"].as_f64().unwrap_or(0.0);
        let market_cap_sol = token_launch["marketCapSol"].as_f64().unwrap_or(0.0);
        let name = token_launch["name"].as_str().unwrap_or("");
        let symbol = token_launch["symbol"].as_str().unwrap_or("");
        let uri = token_launch["uri"].as_str().unwrap_or("");
        let pool = token_launch["pool"].as_str().unwrap_or("");

    let result = sqlx::query(
    "INSERT INTO token_launches (
        signature, trader_public_key, tx_type, mint, sol_in_pool,
        tokens_in_pool, initial_buy, sol_amount, new_token_balance,
        market_cap_sol, name, symbol, uri, pool
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
    ON CONFLICT (signature) DO NOTHING"
)
.bind(signature)
.bind(trader_public_key)
.bind(tx_type)
.bind(mint)
.bind(sol_in_pool)
.bind(tokens_in_pool)
.bind(initial_buy)
.bind(sol_amount)
.bind(new_token_balance)
.bind(market_cap_sol)
.bind(name)
.bind(symbol)
.bind(uri)
.bind(pool)
.execute(&self.pool)
.await?;


        if result.rows_affected() > 0 {
            println!("✅ Inserted token launch: {} ({})", symbol, signature);
        } else {
            println!("⚠️ Token launch already exists: {}", signature);
        }

        Ok(())
    }

    pub async fn push_trade(&self, trade: &Value) -> Result<(), Box<dyn std::error::Error>> {
        let signature = trade["signature"].as_str().unwrap_or("");
        let mint = trade["mint"].as_str().unwrap_or("");
        let trader_public_key = trade["traderPublicKey"].as_str().unwrap_or("");
        let tx_type = trade["txType"].as_str().unwrap_or("");
        let token_amount = trade["tokenAmount"].as_f64().unwrap_or(0.0);
        let sol_amount = trade["solAmount"].as_f64().unwrap_or(0.0);
        let new_token_balance = trade["newTokenBalance"].as_f64().unwrap_or(0.0);
        let bonding_curve_key = trade["bondingCurveKey"].as_str().unwrap_or("");
        let v_tokens_in_bonding_curve = trade["vTokensInBondingCurve"].as_f64().unwrap_or(0.0);
        let v_sol_in_bonding_curve = trade["vSolInBondingCurve"].as_f64().unwrap_or(0.0);
        let market_cap_sol = trade["marketCapSol"].as_f64().unwrap_or(0.0);
        let pool = trade["pool"].as_str().unwrap_or("");

    let result = sqlx::query(
    "INSERT INTO trades (
        signature, mint, trader_public_key, tx_type, token_amount,
        sol_amount, new_token_balance, bonding_curve_key,
        v_tokens_in_bonding_curve, v_sol_in_bonding_curve,
        market_cap_sol, pool
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
    ON CONFLICT (signature) DO NOTHING"
)
.bind(signature)
.bind(mint)
.bind(trader_public_key)
.bind(tx_type)
.bind(token_amount)
.bind(sol_amount)
.bind(new_token_balance)
.bind(bonding_curve_key)
.bind(v_tokens_in_bonding_curve)
.bind(v_sol_in_bonding_curve)
.bind(market_cap_sol)
.bind(pool)
.execute(&self.pool)
.await?;


        if result.rows_affected() > 0 {
            println!("✅ Inserted trade: {} ({})", mint, signature);
        } else {
            println!("⚠️ Trade already exists: {}", signature);
        }

        Ok(())
    }

    // Analytics queries using SQLx for future use
    pub async fn _market_summary(&self) -> Result<(f64, f64, f64, f64, f64), Box<dyn std::error::Error>> {
    let row = sqlx::query(
        "SELECT 
            COALESCE(AVG(sol_in_pool), 0) as avg_sol_in_pool,
            COALESCE(SUM(tokens_in_pool), 0) as total_tokens_in_pool,
            COALESCE(SUM(initial_buy), 0) as total_initial_buy,
            COALESCE(SUM(sol_amount), 0) as total_sol_amount,
            COALESCE(MAX(market_cap_sol), 0) as max_market_cap
        FROM token_launches"
    )
    .fetch_one(&self.pool)
    .await?;

    Ok((
        row.try_get("avg_sol_in_pool")?,
        row.try_get("total_tokens_in_pool")?,
        row.try_get("total_initial_buy")?,
        row.try_get("total_sol_amount")?,
        row.try_get("max_market_cap")?,
    ))
}

}
