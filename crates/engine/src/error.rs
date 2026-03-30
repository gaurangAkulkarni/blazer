use thiserror::Error;

#[derive(Error, Debug)]
pub enum BlazeError {
    #[error("Column not found: {0}")]
    ColumnNotFound(String),

    #[error("Schema mismatch: {0}")]
    SchemaMismatch(String),

    #[error("Type mismatch: {0}")]
    TypeMismatch(String),

    #[error("Invalid operation: {0}")]
    InvalidOperation(String),

    #[error("Compute error: {0}")]
    ComputeError(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow2::error::Error),

    #[error("Out of bounds: index {index}, length {length}")]
    OutOfBounds { index: usize, length: usize },

    #[error("Parse error: {0}")]
    ParseError(String),

    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, BlazeError>;
