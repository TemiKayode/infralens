use thiserror::Error;

#[derive(Debug, Error)]
pub enum QueryError {
    #[error("lex error: {0}")]
    Lex(String),
    #[error("parse error: {0}")]
    Parse(String),
    #[error("bind error: {0}")]
    Bind(String),
    #[error("optimizer error: {0}")]
    Optimizer(String),
    #[error("execution error: {0}")]
    Execution(String),
    #[error("storage error: {0}")]
    Storage(#[from] infralens_storage::error::StorageError),
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, QueryError>;
