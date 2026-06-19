use thiserror::Error;

#[derive(Debug, Error)]
pub enum IngestError {
    #[error("back-pressure: ingest buffer full")]
    BufferFull,

    #[error("storage error: {0}")]
    Storage(#[from] infralens_storage::StorageError),

    #[error("normalisation error: {0}")]
    Normalisation(String),
}

impl IngestError {
    pub fn is_retriable(&self) -> bool {
        matches!(self, IngestError::BufferFull)
    }
}

pub type Result<T> = std::result::Result<T, IngestError>;
