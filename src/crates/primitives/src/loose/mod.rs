pub mod storage;

use crate::graph_index::IndexedGraph;
use crate::handle::TxHandle;
use crate::handle::TxInHandle;
use crate::handle::TxOutHandle;

// Type defintions for loose txs and their ids

// TBD whether this is a generic or u32 specifically
/// Sum of the short id of the txid and vout.
#[derive(PartialEq, Eq, Hash, Copy, Clone, Debug, Ord, PartialOrd)]
pub struct TxOutId {
    pub txid: TxId,
    pub vout: u32,
}

impl TxOutId {
    pub fn new(txid: TxId, vout: u32) -> Self {
        Self { txid, vout }
    }
}

impl TxOutId {
    pub fn with<'a>(&self, index: &'a dyn IndexedGraph) -> TxOutHandle<'a> {
        TxOutHandle::new(*self, index)
    }
}

/// Sum of the short id of the txid and vin
#[derive(PartialEq, Eq, Hash, Copy, Clone, Debug, Ord, PartialOrd)]
pub struct TxInId {
    pub(crate) txid: TxId,
    vin: u32,
}

impl TxInId {
    pub fn with<'a>(&self, index: &'a dyn IndexedGraph) -> TxInHandle<'a> {
        TxInHandle::new(*self, index)
    }

    pub fn txid(&self) -> TxId {
        self.txid
    }

    pub fn vin(&self) -> u32 {
        self.vin
    }
}

#[derive(PartialEq, Eq, Hash, Copy, Clone, Debug, Ord, PartialOrd)]
pub struct TxId(pub u32);

impl TxId {
    pub fn with<'a>(&self, index: &'a dyn IndexedGraph) -> TxHandle<'a> {
        TxHandle::new(*self, index)
    }

    // TODO: this should not be pub. Only pub'd for testing purposes.
    pub fn txout_id(&self, vout: u32) -> TxOutId {
        TxOutId { txid: *self, vout }
    }

    pub fn txin_id(&self, vin: u32) -> TxInId {
        TxInId { txid: *self, vin }
    }

    pub fn txout_handle<'a>(&self, index: &'a dyn IndexedGraph, vout: u32) -> TxOutHandle<'a> {
        self.txout_id(vout).with(index)
    }

    #[allow(unused)]
    fn txin_handle<'a>(&self, index: &'a dyn IndexedGraph, vin: u32) -> TxInHandle<'a> {
        self.txin_id(vin).with(index)
    }
}
