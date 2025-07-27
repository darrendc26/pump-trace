
mod ingest;
mod process_data;
mod arrow;
mod postgres_db;
mod parquet_storage;
use ingest::ingest_ws_stream;

#[tokio::main]
async fn main() {
    ingest_ws_stream().await;

}
