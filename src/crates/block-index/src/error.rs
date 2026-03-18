use std::fmt;

/// Errors that can occur when reading the block index.
#[derive(Debug)]
pub enum Error {
    /// LevelDB error.
    LevelDb(rusty_leveldb::Status),
    /// Expected key not found in the index.
    KeyNotFound(&'static str),
    /// Unexpected end of data while deserializing a value.
    UnexpectedEof,
    /// Block exists in the index but has no stored data (header-only or pruned).
    BlockNotStored,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::LevelDb(e) => write!(f, "leveldb: {e}"),
            Error::KeyNotFound(key) => write!(f, "key '{key}' not found in block index"),
            Error::UnexpectedEof => write!(f, "unexpected end of data"),
            Error::BlockNotStored => write!(f, "block has no stored data (pruned or header-only)"),
        }
    }
}

impl std::error::Error for Error {}

impl From<rusty_leveldb::Status> for Error {
    fn from(e: rusty_leveldb::Status) -> Self {
        Error::LevelDb(e)
    }
}
