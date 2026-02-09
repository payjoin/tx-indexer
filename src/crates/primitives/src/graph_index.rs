use std::sync::Arc;

use crate::ScriptPubkeyHash;
use crate::abstract_types::AbstractTransaction;

pub trait PrevOutIndex {
    type TxInId;
    type TxOutId;
    // TODO: consider handle wrappers converting ids to the actual types.
    // justification: the heuristics may not care about the content of the data. Only access that thru the handler.
    fn prev_txout(&self, ot: &Self::TxInId) -> Self::TxOutId;
}

pub trait TxInIndex {
    type TxOutId;
    type TxInId;
    fn spending_txin(&self, tx: &Self::TxOutId) -> Option<Self::TxInId>;
}

pub trait ScriptPubkeyIndex {
    type TxOutId;
    /// Returns the first transaction output ID that uses the given script pubkey.
    fn script_pubkey_to_txout_id(&self, script_pubkey: &ScriptPubkeyHash) -> Option<Self::TxOutId>;
}

pub trait TxIndex {
    type TxId;
    type TxOutId;
    type TxInId;
    fn tx(
        &self,
        txid: &Self::TxId,
    ) -> Option<
        Arc<
            dyn AbstractTransaction<
                    TxId = Self::TxId,
                    TxOutId = Self::TxOutId,
                    TxInId = Self::TxInId,
                > + Send
                + Sync,
        >,
    >;
}

pub trait GlobalClusteringIndex {
    type TxOutId;
    fn find(&self, txout_id: Self::TxOutId) -> Self::TxOutId;
    fn union(&self, txout_id1: Self::TxOutId, txout_id2: Self::TxOutId);
}

// TODO: seprate out into rw and ro traits
pub trait IndexedGraph<TxId, TxInId, TxOutId>:
    PrevOutIndex<TxInId = TxInId, TxOutId = TxOutId>
    + TxInIndex<TxOutId = TxOutId, TxInId = TxInId>
    + ScriptPubkeyIndex<TxOutId = TxOutId>
    + TxIndex<TxId = TxId, TxOutId = TxOutId, TxInId = TxInId>
    + GlobalClusteringIndex<TxOutId = TxOutId>
{
}
