pub mod handle;
pub mod storage;

use crate::loose::handle::{LooseIndexedGraph, TxHandle, TxInHandle, TxOutHandle};

// Type defintions for loose txs and their ids

// TBD whether this is a generic or u32 specifically
/// Sum of the short id of the txid and vout.
#[derive(PartialEq, Eq, Hash, Copy, Clone, Debug, Ord, PartialOrd)]
pub struct TxOutId {
    txid: TxId,
    vout: u32,
}

impl TxOutId {
    pub fn new(txid: TxId, vout: u32) -> Self {
        Self { txid, vout }
    }

    pub fn txid(&self) -> TxId {
        self.txid
    }

    pub fn vout(&self) -> u32 {
        self.vout
    }

    pub fn with<'a>(self, index: &'a LooseIndexedGraph) -> TxOutHandle<'a> {
        TxOutHandle::new(self, index)
    }
}

/// Sum of the short id of the txid and vin
#[derive(PartialEq, Eq, Hash, Copy, Clone, Debug, Ord, PartialOrd)]
pub struct TxInId {
    pub(crate) txid: TxId,
    vin: u32,
}

impl TxInId {
    pub fn new(txid: TxId, vin: u32) -> Self {
        Self { txid, vin }
    }

    pub fn txid(&self) -> TxId {
        self.txid
    }

    pub fn vin(&self) -> u32 {
        self.vin
    }

    pub fn with<'a>(self, index: &'a LooseIndexedGraph) -> TxInHandle<'a> {
        TxInHandle::new(self, index)
    }
}

#[derive(PartialEq, Eq, Hash, Copy, Clone, Debug, Ord, PartialOrd)]
pub struct TxId(pub u32);

impl TxId {
    pub fn new(txid: u32) -> Self {
        Self(txid)
    }

    pub fn txout_id(self, vout: u32) -> TxOutId {
        TxOutId::new(self, vout)
    }

    pub fn txin_id(self, vin: u32) -> TxInId {
        TxInId::new(self, vin)
    }

    pub fn with<'a>(self, index: &'a LooseIndexedGraph) -> TxHandle<'a> {
        TxHandle::new(self, index)
    }
}
