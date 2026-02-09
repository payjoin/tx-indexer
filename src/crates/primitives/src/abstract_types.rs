use std::sync::Arc;

use bitcoin::Amount;

use crate::ScriptPubkeyHash;

// Should be implemented by any type that is contained within a transaction.
pub trait TxConstituent {
    type Handle: AbstractTransaction;
    fn containing_tx(&self) -> Self::Handle;

    fn vout(&self) -> usize;
}

pub trait OutputCount: AbstractTransaction {
    fn output_count(&self) -> usize;
}

pub trait EnumerateSpentTxOuts: AbstractTransaction {
    fn spent_coins(&self) -> impl Iterator<Item = Self::TxOutId>;
}

// TODO: find a better name for this
pub trait EnumerateOutputValueInArbitraryOrder: AbstractTransaction {
    fn output_values(&self) -> impl Iterator<Item = Amount>;
}

// Blanket implementation for Arc<dyn AbstractTransaction> to bridge with the heuristics
impl<Atx: AbstractTransaction + ?Sized> EnumerateSpentTxOuts for Arc<Atx> {
    fn spent_coins(&self) -> impl Iterator<Item = Self::TxOutId> {
        self.inputs().map(|input| input.prev_txout_id())
    }
}

impl<Atx: AbstractTransaction + ?Sized> EnumerateOutputValueInArbitraryOrder for Arc<Atx> {
    fn output_values(&self) -> impl Iterator<Item = Amount> {
        self.outputs().map(|output| output.value())
    }
}

impl<T: AbstractTransaction + ?Sized> AbstractTransaction for Arc<T> {
    type TxId = T::TxId;
    type TxOutId = T::TxOutId;
    type TxInId = T::TxInId;
    fn id(&self) -> Self::TxId {
        (**self).id()
    }

    fn inputs(
        &self,
    ) -> Box<
        dyn Iterator<Item = Box<dyn AbstractTxIn<TxId = Self::TxId, TxOutId = Self::TxOutId>>> + '_,
    > {
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

impl<Atx: AbstractTransaction + ?Sized> OutputCount for Arc<Atx> {
    fn output_count(&self) -> usize {
        self.output_len()
    }
}

// Abstract transaction types for type erasure

/// Trait for transaction inputs
pub trait AbstractTxIn {
    type TxId;
    type TxOutId;
    /// Returns the transaction ID of the previous output
    fn prev_txid(&self) -> Self::TxId;
    /// Returns the output index of the previous output
    fn prev_vout(&self) -> u32;
    /// Returns the previous output ID
    fn prev_txout_id(&self) -> Self::TxOutId;
}

/// Trait for transaction outputs
pub trait AbstractTxOut {
    /// Returns the value of the output
    fn value(&self) -> Amount;
    /// Returns the script pubkey hash (20-byte hash) if available
    /// Returns None if the script doesn't contain a standard hash or is not supported
    fn script_pubkey_hash(&self) -> ScriptPubkeyHash;
}

/// Trait for transaction looking things. Generic over the ids as they can be either loose or dense.
pub trait AbstractTransaction {
    type TxId;
    type TxOutId: Eq + std::hash::Hash + Copy;
    type TxInId;
    /// Returns the transaction ID
    fn id(&self) -> Self::TxId;
    /// Returns an iterator over transaction inputs
    fn inputs(
        &self,
    ) -> Box<
        dyn Iterator<Item = Box<dyn AbstractTxIn<TxId = Self::TxId, TxOutId = Self::TxOutId>>> + '_,
    >;
    /// Returns an iterator over transaction outputs
    fn outputs(&self) -> Box<dyn Iterator<Item = Box<dyn AbstractTxOut>> + '_>;
    /// Returns the number of outputs
    fn output_len(&self) -> usize;
    /// Returns the output at the given index, if it exists
    fn output_at(&self, index: usize) -> Option<Box<dyn AbstractTxOut>>;

    fn locktime(&self) -> u32;
}
