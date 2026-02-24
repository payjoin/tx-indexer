use crate::{
    traits::abstract_types::AbstractTransaction,
    traits::graph_index::IndexedGraph,
    unified::{handle::TxHandle, id::AnyTxId},
};

pub mod storage;

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
}

#[derive(PartialEq, Eq, Hash, Copy, Clone, Debug, Ord, PartialOrd)]
pub struct TxId(pub u32);

impl TxId {
    pub fn new(txid: u32) -> Self {
        Self(txid)
    }

    pub fn index(self) -> u32 {
        self.0
    }

    pub fn txout_id(self, vout: u32) -> TxOutId {
        TxOutId::new(self, vout)
    }

    pub fn txin_id(self, vin: u32) -> TxInId {
        TxInId::new(self, vin)
    }

    pub fn with<'a>(self, index: &'a dyn IndexedGraph) -> TxHandle<'a> {
        let txid = AnyTxId::from(self);
        TxHandle::new(txid, index)
    }
}

/// Concrete transaction type for the loose Transactions
pub type LooseTx = dyn AbstractTransaction + Send + Sync;
