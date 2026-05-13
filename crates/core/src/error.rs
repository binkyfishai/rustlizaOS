use thiserror::Error;
use uuid::Uuid;

#[derive(Error, Debug)]
pub enum RustlizaError {
    #[error("database error: {0}")]
    Database(#[from] anyhow::Error),

    #[error("model provider error: {0}")]
    ModelProvider(String),

    #[error("character load error: {0}")]
    CharacterLoad(String),

    #[error("template error: {0}")]
    Template(String),

    #[error("action not found: {0}")]
    ActionNotFound(String),

    #[error("entity not found: {0}")]
    EntityNotFound(Uuid),

    #[error("room not found: {0}")]
    RoomNotFound(Uuid),

    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, RustlizaError>;
