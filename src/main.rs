
mod ingest;
use ingest::ingest_ws_stream;

#[tokio::main]
async fn main() {
    println!("Hello, world!");
    ingest_ws_stream().await;
}
