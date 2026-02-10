pub mod handle;
pub mod storage;

use crate::abstract_id::LooseIds;
use crate::graph_index::{IndexedGraph, TxIdIndexOps, TxOutIdWithIndex, WithIndex};
use crate::loose::handle::{TxHandle, TxInHandle, TxOutHandle};

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

    pub fn with<'a>(self, index: &'a (dyn IndexedGraph<LooseIds> + 'a)) -> TxOutHandle<'a> {
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

    pub fn with<'a>(self, index: &'a (dyn IndexedGraph<LooseIds> + 'a)) -> TxInHandle<'a> {
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

    pub fn with<'a>(self, index: &'a (dyn IndexedGraph<LooseIds> + 'a)) -> TxHandle<'a> {
        TxHandle::new(self, index)
    }
}

impl WithIndex<LooseIds> for TxId {
    type Handle<'a> = TxHandle<'a>;

    fn with_index<'a>(&self, index: &'a (dyn IndexedGraph<LooseIds> + 'a)) -> TxHandle<'a> {
        TxHandle::new(*self, index)
    }
}

impl TxIdIndexOps<LooseIds> for TxId {
    fn txout_id(self, vout: u32) -> <LooseIds as crate::abstract_id::AbstractId>::TxOutId {
        TxOutId::new(self, vout)
    }
}

impl TxOutIdWithIndex<LooseIds> for TxOutId {
    type Handle<'a> = TxOutHandle<'a>;

    fn with_index<'a>(&self, index: &'a (dyn IndexedGraph<LooseIds> + 'a)) -> TxOutHandle<'a> {
        TxOutHandle::new(*self, index)
    }
}
