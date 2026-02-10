use std::sync::Arc;

use crate::ScriptPubkeyHash;
use crate::abstract_types::{AbstractTransaction, IdFamily};

pub trait PrevOutIndex {
    type I: IdFamily;
    // TODO: consider handle wrappers converting ids to the actual types.
    // justification: the heuristics may not care about the content of the data. Only access that thru the handler.
    fn prev_txout(&self, ot: &Self::TxInId) -> Self::TxOutId;
}

pub trait TxInIndex {
    type I: IdFamily;
    fn spending_txin(&self, tx: &Self::I::TxOutId) -> Option<Self::I::TxInId>;
}

pub trait ScriptPubkeyIndex {
    type I: IdFamily;
    /// Returns the first transaction output ID that uses the given script pubkey.
    fn script_pubkey_to_txout_id(
        &self,
        script_pubkey: &ScriptPubkeyHash,
    ) -> Option<Self::I::TxOutId>;
}

pub trait TxIndex {
    type I: IdFamily;
    fn tx(
        &self,
        txid: &Self::I::TxId,
    ) -> Option<Arc<dyn AbstractTransaction<I = Self::I> + Send + Sync>>;
}

pub trait GlobalClusteringIndex {
    type I: IdFamily;
    fn find(&self, txout_id: Self::I::TxOutId) -> Self::TxOutId;
    fn union(&self, txout_id1: Self::TxOutId, txout_id2: Self::TxOutId);
}

// TODO: seprate out into rw and ro traits
pub trait IndexedGraph<I: IdFamily>:
    PrevOutIndex<I = I>
    + TxInIndex<I = I>
    + ScriptPubkeyIndex<I = I>
    + TxIndex<I = I>
    + GlobalClusteringIndex<I = I>
{
}
