use crate::abstract_types::{IdFamily, TxInIdOps, TxOutIdOps};

pub mod parser;

pub mod handle;
pub mod storage;
pub use parser::{BlockFileError, Parser};

#[repr(transparent)]
#[derive(PartialEq, Eq, Hash, Copy, Clone, Debug, Ord, PartialOrd)]
pub struct BlockFileId(pub u32);

#[derive(PartialEq, Eq, Hash, Copy, Clone, Debug, Ord, PartialOrd)]
pub struct TxId {
    block_file: BlockFileId,
    byte_offset: u32,
}

impl TxId {
    pub fn new(block_file: BlockFileId, byte_offset: u32) -> Self {
        Self {
            block_file,
            byte_offset,
        }
    }

    pub fn block_file(&self) -> BlockFileId {
        self.block_file
    }

    pub fn byte_offset(&self) -> u32 {
        self.byte_offset
    }

    pub fn txout_id(self, byte_offset: u32) -> TxOutId {
        TxOutId::new(self, byte_offset)
    }

    pub fn txin_id(self, byte_offset: u32) -> TxInId {
        TxInId::new(self, byte_offset)
    }
}

#[derive(PartialEq, Eq, Hash, Copy, Clone, Debug, Ord, PartialOrd)]
pub struct TxOutId {
    tx_id: TxId,
    byte_offset: u32,
}

impl TxOutId {
    pub fn new(tx_id: TxId, byte_offset: u32) -> Self {
        Self { tx_id, byte_offset }
    }

    pub fn txid(&self) -> TxId {
        self.tx_id
    }

    pub fn byte_offset(&self) -> u32 {
        self.byte_offset
    }
}

impl TxOutIdOps<DenseIds> for TxOutId {
    fn containing_txid(self) -> TxId {
        self.txid()
    }
}

#[derive(PartialEq, Eq, Hash, Copy, Clone, Debug, Ord, PartialOrd)]
pub struct TxInId {
    tx_id: TxId,
    byte_offset: u32,
}

impl TxInId {
    pub fn new(tx_id: TxId, byte_offset: u32) -> Self {
        Self { tx_id, byte_offset }
    }

    pub fn txid(&self) -> TxId {
        self.tx_id
    }

    pub fn byte_offset(&self) -> u32 {
        self.byte_offset
    }
}

impl TxInIdOps<DenseIds> for TxInId {
    fn containing_txid(self) -> TxId {
        self.txid()
    }
}

pub struct DenseIds;

impl IdFamily for DenseIds {
    type TxId = TxId;
    type TxInId = TxInId;
    type TxOutId = TxOutId;
}
