//! The crate's error type.

/// Errors of the store and the network.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The queue reported a failure.
    #[error("storage error: {0}")]
    Storage(#[from] taquba::Error),
    /// A stored record does not decode: the store was written by an
    /// incompatible version of this crate or of the kernel.
    #[error("stored record does not decode: {0}")]
    Decode(String),
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::Decode(e.to_string())
    }
}

/// Result alias over [`Error`].
pub type Result<T> = std::result::Result<T, Error>;
