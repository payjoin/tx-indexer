pub mod handle;
pub mod storage;

use crate::{
    abstract_types::{
        AbstractTransaction, AbstractTxIn, AbstractTxOut, IdFamily, IntoTxHandle, IntoTxInHandle,
        IntoTxOutHandle, TxInIdOps, TxOutIdOps,
    },
    loose::handle::{LooseIndexedGraph, TxHandle, TxInHandle, TxOutHandle},
};

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

    pub fn with<'a>(self, index: &'a LooseIndexedGraph<'a>) -> TxOutHandle<'a> {
        TxOutHandle::new(self, index)
    }
}

/// Sum of the short id of the txid and vin
#[derive(PartialEq, Eq, Hash, Copy, Clone, Debug, Ord, PartialOrd)]
pub struct TxInId {
    /// The transaction ID of the transaction spending this input
    txid: TxId,
    /// The input index of the transaction spending this input
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

    pub fn with<'a>(self, index: &'a LooseIndexedGraph<'a>) -> TxInHandle<'a> {
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

    pub fn with<'a>(self, index: &'a LooseIndexedGraph<'a>) -> TxHandle<'a> {
        TxHandle::new(self, index)
    }
}

impl IntoTxHandle<LooseIds> for TxId {
    fn with_index<'a>(
        self,
        index: &'a dyn crate::graph_index::IndexedGraph<LooseIds>,
    ) -> Box<dyn AbstractTransaction<I = LooseIds> + 'a> {
        Box::new(TxHandle::new(self, index))
    }
}

impl IntoTxOutHandle<LooseIds> for TxOutId {
    fn with_index<'a>(
        self,
        index: &'a dyn crate::graph_index::IndexedGraph<LooseIds>,
    ) -> Box<dyn AbstractTxOut<I = LooseIds> + 'a> {
        Box::new(TxOutHandle::new(self, index))
    }
}

impl IntoTxInHandle<LooseIds> for TxInId {
    fn with_index<'a>(
        self,
        index: &'a dyn crate::graph_index::IndexedGraph<LooseIds>,
    ) -> Box<dyn AbstractTxIn<I = LooseIds> + 'a> {
        Box::new(TxInHandle::new(self, index))
    }
}

#[derive(Clone, PartialEq)]
pub struct LooseIds;

impl IdFamily for LooseIds {
    type TxId = TxId;
    type TxInId = TxInId;
    type TxOutId = TxOutId;
}

impl TxOutIdOps<LooseIds> for TxOutId {
    fn containing_txid(self) -> TxId {
        self.txid()
    }
}

impl TxInIdOps<LooseIds> for TxInId {
    fn containing_txid(self) -> TxId {
        self.txid()
    }
}

/// Concrete transaction type for the loose Transactions
pub type LooseTx = dyn AbstractTransaction<I = LooseIds> + Send + Sync;
