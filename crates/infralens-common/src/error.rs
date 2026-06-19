use thiserror::Error;

#[derive(Debug, Error)]
pub enum CommonError {
    #[error("invalid signal type byte: {0}")]
    InvalidSignalType(u8),

    #[error("serialisation error: {0}")]
    Serialisation(#[from] serde_json::Error),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}
