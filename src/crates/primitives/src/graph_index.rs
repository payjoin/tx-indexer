use std::sync::Arc;

use bitcoin::Amount;

use crate::ScriptPubkeyHash;
use crate::abstract_fingerprints::HasNLockTime;
use crate::abstract_id::AbstractId;
use crate::abstract_types::{AbstractTransaction, AbstractTxIn, AbstractTxOut, OutputCount};

// --- Handle traits for generic resolution via IndexedGraph<I> ---

/// Transaction handle produced by resolving a tx id with an index.
/// Implemented by backend-specific handle types (e.g. loose::TxHandle).
/// Uses object-safe return types (Box<dyn Iterator>) so that Box<dyn TxHandleLike<I>> is valid.
pub trait TxHandleLike<I: AbstractId>:
    AbstractTransaction<Id = I> + OutputCount + HasNLockTime
{
    /// Iterator of output handles that expose id() and script_pubkey_hash().
    fn output_handles(&self) -> Box<dyn Iterator<Item = Box<dyn TxOutHandleLike<I> + '_>> + '_>;

    /// Spent output IDs (object-safe version for trait objects).
    fn spent_coins(&self) -> Box<dyn Iterator<Item = I::TxOutId> + '_>;

    /// Output values in arbitrary order (object-safe version, e.g. for coinjoin detection).
    fn output_values(&self) -> Box<dyn Iterator<Item = Amount> + '_>;
}

/// Output handle produced by resolving a txout id with an index.
/// Not required to be Send/Sync so that handles holding &dyn IndexedGraph can implement it.
pub trait TxOutHandleLike<I: AbstractId> {
    fn id(&self) -> I::TxOutId;
    fn script_pubkey_hash(&self) -> ScriptPubkeyHash;
    fn value(&self) -> Amount;
    /// Output index (vout) within the transaction.
    fn vout(&self) -> u32;
    /// Containing transaction handle.
    fn tx(&self) -> Box<dyn TxHandleLike<I> + '_>;
    /// Spending tx input handle, if spent.
    fn spent_by(&self) -> Option<Box<dyn TxInHandleLike<I> + '_>>;
}

/// Input handle (spending txin); used for change detection.
pub trait TxInHandleLike<I: AbstractId> {
    fn tx(&self) -> Box<dyn TxHandleLike<I> + '_>;
}

/// Resolve a transaction ID with an index. Implemented by backend TxId types (e.g. loose::TxId).
pub trait WithIndex<I: AbstractId> {
    type Handle<'a>: TxHandleLike<I> + 'a
    where
        Self: 'a,
        I: 'a;

    /// Resolve this ID with the index, returning a handle that borrows the index.
    fn with_index<'a>(&self, index: &'a (dyn IndexedGraph<I> + 'a)) -> Self::Handle<'a>;

    /// Run a closure with the handle. Convenience for when the result type does not borrow.
    fn with_index_apply<'a, R, F>(&self, index: &'a (dyn IndexedGraph<I> + 'a), f: F) -> R
    where
        Self: 'a,
        F: FnOnce(Self::Handle<'a>) -> R,
        R: 'static,
    {
        f(self.with_index(index))
    }
}

/// TxId type that can produce TxOutIds by index (used by change clustering).
pub trait TxIdIndexOps<I: AbstractId> {
    fn txout_id(self, vout: u32) -> I::TxOutId;
}

/// Resolve a transaction output ID with an index. Implemented by backend TxOutId types.
pub trait TxOutIdWithIndex<I: AbstractId> {
    type Handle<'a>: TxOutHandleLike<I> + 'a
    where
        Self: 'a,
        I: 'a;

    /// Resolve this ID with the index, returning a handle that borrows the index.
    fn with_index<'a>(&self, index: &'a (dyn IndexedGraph<I> + 'a)) -> Self::Handle<'a>;

    /// Run a closure with the handle. Convenience for when the result type does not borrow.
    fn with_index_apply<'a, R, F>(&self, index: &'a (dyn IndexedGraph<I> + 'a), f: F) -> R
    where
        Self: 'a,
        F: FnOnce(Self::Handle<'a>) -> R,
        R: 'static,
    {
        f(self.with_index(index))
    }
}

/// Erased index handle: run a closure with `&dyn IndexedGraph<I>`.
/// Implemented by pipeline index handle types (e.g. LooseIndexHandle for I = LooseIds).
pub trait IndexHandleFor<I: AbstractId> {
    fn with_graph<R>(&self, f: impl FnOnce(&dyn IndexedGraph<I>) -> R) -> R;
}

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
    type Id: AbstractId;
    fn tx(
        &self,
        txid: &<<Self as TxIndex>::Id as AbstractId>::TxId,
    ) -> Option<Arc<dyn AbstractTransaction<Id = <Self as TxIndex>::Id> + Send + Sync>>;
}

pub trait GlobalClusteringIndex {
    type TxOutId;
    fn find(&self, txout_id: Self::TxOutId) -> Self::TxOutId;
    fn union(&self, txout_id1: Self::TxOutId, txout_id2: Self::TxOutId);
}

// TODO: seprate out into rw and ro traits
pub trait IndexedGraph<I: AbstractId>:
    PrevOutIndex<TxInId = I::TxInId, TxOutId = I::TxOutId>
    + TxInIndex<TxOutId = I::TxOutId, TxInId = I::TxInId>
    + ScriptPubkeyIndex<TxOutId = I::TxOutId>
    + TxIndex<Id = I>
    + GlobalClusteringIndex<TxOutId = I::TxOutId>
{
}

// Box<dyn TxHandleLike<I>> so that heuristics can use TxConstituent with boxed handles.
impl<I: AbstractId> AbstractTransaction for Box<dyn TxHandleLike<I> + '_> {
    type Id = I;
    fn id(&self) -> I::TxId {
        (**self).id()
    }
    fn inputs(&self) -> Box<dyn Iterator<Item = Box<dyn AbstractTxIn<Id = I>>> + '_> {
        (**self).inputs()
    }
    fn outputs(&self) -> Box<dyn Iterator<Item = Box<dyn AbstractTxOut>> + '_> {
        (**self).outputs()
    }
    fn output_len(&self) -> usize {
        (**self).output_len()
    }
    fn output_at(&self, index: usize) -> Option<Box<dyn AbstractTxOut>> {
        (**self).output_at(index)
    }
    fn locktime(&self) -> u32 {
        (**self).locktime()
    }
}

impl<I: AbstractId> OutputCount for Box<dyn TxHandleLike<I> + '_> {
    fn output_count(&self) -> usize {
        (**self).output_count()
    }
}

impl<I: AbstractId> HasNLockTime for Box<dyn TxHandleLike<I> + '_> {
    fn n_locktime(&self) -> u32 {
        (**self).n_locktime()
    }
}

impl<I: AbstractId> AbstractTransaction for &(dyn TxHandleLike<I> + '_) {
    type Id = I;
    fn id(&self) -> I::TxId {
        (**self).id()
    }
    fn inputs(&self) -> Box<dyn Iterator<Item = Box<dyn AbstractTxIn<Id = I>>> + '_> {
        (**self).inputs()
    }
    fn outputs(&self) -> Box<dyn Iterator<Item = Box<dyn AbstractTxOut>> + '_> {
        (**self).outputs()
    }
    fn output_len(&self) -> usize {
        (**self).output_len()
    }
    fn output_at(&self, index: usize) -> Option<Box<dyn AbstractTxOut>> {
        (**self).output_at(index)
    }
    fn locktime(&self) -> u32 {
        (**self).locktime()
    }
}

impl<I: AbstractId> OutputCount for &(dyn TxHandleLike<I> + '_) {
    fn output_count(&self) -> usize {
        (**self).output_count()
    }
}

impl<I: AbstractId> HasNLockTime for &(dyn TxHandleLike<I> + '_) {
    fn n_locktime(&self) -> u32 {
        (**self).n_locktime()
    }
}
