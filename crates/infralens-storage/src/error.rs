use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("wal crc mismatch at offset {offset}: expected {expected:#010x}, got {actual:#010x}")]
    WalCrcMismatch { offset: u64, expected: u32, actual: u32 },

    #[error("wal entry truncated at offset {0}")]
    WalTruncated(u64),

    #[error("serialisation error: {0}")]
    Serialisation(String),

    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("partition not found: {0}")]
    PartitionNotFound(String),

    #[error("engine is closed")]
    Closed,

    #[error("compaction error: {0}")]
    Compaction(String),
}

impl From<Box<bincode::ErrorKind>> for StorageError {
    fn from(e: Box<bincode::ErrorKind>) -> Self {
        StorageError::Serialisation(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, StorageError>;
