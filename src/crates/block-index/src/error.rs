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
    /// Requested depth exceeds the number of available blocks in the chain.
    DepthExceedsChain {
        /// The depth that was requested.
        requested: u32,
        /// How many steps were successfully traversed before running out of chain.
        available: u32,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::LevelDb(e) => write!(f, "leveldb: {e}"),
            Error::KeyNotFound(key) => write!(f, "key '{key}' not found in block index"),
            Error::UnexpectedEof => write!(f, "unexpected end of data"),
            Error::BlockNotStored => write!(f, "block has no stored data (pruned or header-only)"),
            Error::DepthExceedsChain {
                requested,
                available,
            } => write!(
                f,
                "requested depth {requested} exceeds chain length (only {available} blocks available)"
            ),
        }
    }
}

impl std::error::Error for Error {}

impl From<rusty_leveldb::Status> for Error {
    fn from(e: rusty_leveldb::Status) -> Self {
        Error::LevelDb(e)
    }
}
