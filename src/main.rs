
mod ingest;
mod process_data;
mod arrow;
mod datafusion_query;
mod parquet_storage;
use ingest::ingest_ws_stream;

#[tokio::main]
async fn main() {
    ingest_ws_stream().await;
}
