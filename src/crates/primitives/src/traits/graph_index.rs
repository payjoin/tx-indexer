use bitcoin::Amount;

use crate::traits::abstract_types::AbstractTransaction;
use crate::{AnyInId, AnyOutId, AnyTxId, ScriptPubkeyHash};

pub trait PrevOutIndex {
    // TODO: consider handle wrappers converting ids to the actual types.
    // justification: the heuristics may not care about the content of the data. Only access that thru the handler.
    fn prev_txout(&self, ot: &AnyInId) -> Option<AnyOutId>;
}

pub trait TxInIndex {
    fn spending_txin(&self, tx: &AnyOutId) -> Option<AnyInId>;
}

pub trait TxInOwnerIndex {
    fn txid_for_in(&self, in_id: &AnyInId) -> AnyTxId;
}

pub trait ScriptPubkeyIndex {
    /// Returns the first transaction output ID that uses the given script pubkey.
    fn script_pubkey_to_txout_id(&self, script_pubkey: &ScriptPubkeyHash) -> Option<AnyOutId>;
}

pub trait TxIndex {
    fn tx(&self, txid: &AnyTxId) -> Option<std::sync::Arc<dyn AbstractTransaction + Send + Sync>>;
}

pub trait TxIoIndex {
    fn tx_in_ids(&self, txid: &AnyTxId) -> Vec<AnyInId>;
    fn tx_out_ids(&self, txid: &AnyTxId) -> Vec<AnyOutId>;
    // TODO: these 4 methods dont need to exist right now. We end up deserializing the entire transaction just to get these values so we can just call tx() above.
    // There may be a senario where we can only deserialize the input or output we need. in which case these would be useful.
    fn locktime(&self, txid: &AnyTxId) -> u32;
    fn input_sequence(&self, in_id: &AnyInId) -> u32;
    fn witness_items(&self, in_id: &AnyInId) -> Vec<Vec<u8>>;
    fn script_sig_bytes(&self, in_id: &AnyInId) -> Vec<u8>;
}

pub trait OutpointIndex {
    fn outpoint_for_out(&self, out_id: &AnyOutId) -> (AnyTxId, u32);
}

pub trait TxOutDataIndex {
    fn tx_out_data(&self, out_id: &AnyOutId) -> (Amount, ScriptPubkeyHash);
    fn tx_out_spk_bytes(&self, out_id: &AnyOutId) -> Vec<u8>;
}

pub trait IndexedGraph:
    Send
    + Sync
    + PrevOutIndex
    + TxInIndex
    + TxInOwnerIndex
    + ScriptPubkeyIndex
    + TxIndex
    + TxIoIndex
    + OutpointIndex
    + TxOutDataIndex
{
}
