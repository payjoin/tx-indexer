use crate::traits::abstract_types::AbstractTransaction;
use crate::{AnyInId, AnyOutId, AnyTxId, ScriptPubkeyHash};

pub trait PrevOutIndex {
    // TODO: consider handle wrappers converting ids to the actual types.
    // justification: the heuristics may not care about the content of the data. Only access that thru the handler.
    fn prev_txout(&self, ot: &AnyInId) -> AnyOutId;
}

pub trait TxInIndex {
    fn spending_txin(&self, tx: &AnyOutId) -> Option<AnyInId>;
}

pub trait ScriptPubkeyIndex {
    /// Returns the first transaction output ID that uses the given script pubkey.
    fn script_pubkey_to_txout_id(&self, script_pubkey: &ScriptPubkeyHash) -> Option<AnyOutId>;
}

pub trait TxIndex {
    fn tx(&self, txid: &AnyTxId) -> Option<std::sync::Arc<dyn AbstractTransaction + Send + Sync>>;
}

// TODO: seprate out into rw and ro traits
pub trait IndexedGraph:
    Send + Sync + PrevOutIndex + TxInIndex + ScriptPubkeyIndex + TxIndex
{
}
