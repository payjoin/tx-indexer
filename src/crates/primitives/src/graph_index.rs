use std::sync::Arc;

use crate::ScriptPubkeyHash;
use crate::abstract_types::AbstractTransaction;
use crate::loose::TxId;
use crate::loose::TxInId;
use crate::loose::TxOutId;

pub trait PrevOutIndex {
    // TODO: consider handle wrappers converting ids to the actual types.
    // justification: the heuristics may not care about the content of the data. Only access that thru the handler.
    fn prev_txout(&self, ot: &TxInId) -> TxOutId;
}

pub trait TxInIndex {
    fn spending_txin(&self, tx: &TxOutId) -> Option<TxInId>;
}

pub trait ScriptPubkeyIndex {
    /// Returns the first transaction output ID that uses the given script pubkey.
    fn script_pubkey_to_txout_id(&self, script_pubkey: &ScriptPubkeyHash) -> Option<TxOutId>;
}

pub trait TxIndex {
    fn tx(&self, txid: &TxId) -> Option<Arc<dyn AbstractTransaction + Send + Sync>>;
}

pub trait GlobalClusteringIndex {
    fn find(&self, txout_id: TxOutId) -> TxOutId;
    fn union(&self, txout_id1: TxOutId, txout_id2: TxOutId);
}

// TODO: seprate out into rw and ro traits
pub trait IndexedGraph:
    PrevOutIndex + TxInIndex + ScriptPubkeyIndex + TxIndex + GlobalClusteringIndex
{
}
