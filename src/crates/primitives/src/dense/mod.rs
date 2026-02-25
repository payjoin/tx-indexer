pub mod parser;
pub mod storage;
pub use parser::{BlockFileError, Parser};
pub use storage::{DenseStorage, IndexPaths, build_indices};

#[cfg(test)]
mod tests;
#[repr(transparent)]
#[derive(PartialEq, Eq, Hash, Copy, Clone, Debug, Ord, PartialOrd)]
pub struct BlockFileId(pub u32);

#[repr(transparent)]
#[derive(PartialEq, Eq, Hash, Copy, Clone, Debug, Ord, PartialOrd)]
pub struct TxId(pub u32);

impl TxId {
    pub fn new(txid: u32) -> Self {
        Self(txid)
    }

    pub fn index(self) -> u32 {
        self.0
    }
}

#[repr(transparent)]
#[derive(PartialEq, Eq, Hash, Copy, Clone, Debug, Ord, PartialOrd)]
pub struct TxOutId(pub u64);

impl TxOutId {
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    pub fn index(self) -> u64 {
        self.0
    }
}

#[repr(transparent)]
#[derive(PartialEq, Eq, Hash, Copy, Clone, Debug, Ord, PartialOrd)]
pub struct TxInId(pub u64);

impl TxInId {
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    pub fn index(self) -> u64 {
        self.0
    }
}
