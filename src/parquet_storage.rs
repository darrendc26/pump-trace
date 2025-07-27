use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use parquet::basic::{Compression, Encoding};
use std::error::Error;
use std::fs::{File, create_dir_all};
use std::sync::Arc;
use chrono; // Add chrono to your Cargo.toml
use uuid;   // Add uuid to your Cargo.toml

pub struct ParquetStorage {
    base_path: String,
}

impl ParquetStorage {
    pub fn new(base_path: String) -> Result<Self, Box<dyn Error>> {
        create_dir_all(&base_path)?;
        Ok(Self { base_path })
    }

    pub fn write_batch(&self, batches: &[RecordBatch], event_type: &str) -> Result<String, Box<dyn Error>> {
        // Check for empty batches
        if batches.is_empty() {
            return Err("No batches to write".into());
        }

        let now = chrono::Utc::now();
        let dir_path = format!("{}/{}/{}", 
            self.base_path, 
            now.format("%Y/%m/%d"), 
            event_type
        );
        create_dir_all(&dir_path)?;

        // Fixed: Create just the filename, not the full path
        let file_name = format!("batch_{}_{}.parquet", 
            now.format("%H%M%S"),
            uuid::Uuid::new_v4().simple()
        );
        
        // Construct the full file path
        let file_path = format!("{}/{}", dir_path, file_name);

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_encoding(Encoding::PLAIN)
            .set_dictionary_enabled(true)
            .build();

        let file = File::create(&file_path)?;
        let mut writer = ArrowWriter::try_new(file, batches[0].schema(), Some(props))?;

        let mut total_rows = 0;
        for batch in batches {
            writer.write(batch)?;
            total_rows += batch.num_rows();
        }
        
        writer.close()?;
        println!("✅ Wrote {} rows to {}", total_rows, file_path);
        Ok(file_path)
    }

    // Alternative method for single batch (cleaner for your use case)
    pub fn write_single_batch(&self, batch: &RecordBatch, event_type: &str) -> Result<String, Box<dyn Error>> {
        self.write_batch(&[batch.clone()], event_type)
    }
}
