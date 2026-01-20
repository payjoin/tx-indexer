use bitcoin::Amount;

use crate::loose::{TxId, TxOutId};

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
    fn spent_coins(&self) -> impl Iterator<Item = TxOutId>;
}

// TODO: find a better name for this
pub trait EnumerateOutputValueInArbitraryOrder: AbstractTransaction {
    fn output_values(&self) -> impl Iterator<Item = Amount>;
}

// Abstract transaction types for type erasure

/// Trait for transaction inputs
pub trait AbstractTxIn {
    /// Returns the transaction ID of the previous output
    fn prev_txid(&self) -> TxId;
    /// Returns the output index of the previous output
    fn prev_vout(&self) -> u32;
    /// Returns the previous output ID
    // TODO: should have a into impl from txin to txout id
    fn prev_txout_id(&self) -> TxOutId {
        TxOutId::new(self.prev_txid(), self.prev_vout())
    }
}

/// Trait for transaction outputs
pub trait AbstractTxOut {
    /// Returns the value of the output
    fn value(&self) -> Amount;
}

/// Trait for complete transactions
pub trait AbstractTransaction {
    /// Returns the transaction ID
    fn txid(&self) -> TxId;
    /// Returns the transaction ID (alias for txid for backward compatibility)
    fn id(&self) -> TxId {
        self.txid()
    }
    /// Returns an iterator over transaction inputs
    fn inputs(&self) -> Box<dyn Iterator<Item = Box<dyn AbstractTxIn>> + '_>;
    /// Returns an iterator over transaction outputs
    fn outputs(&self) -> Box<dyn Iterator<Item = Box<dyn AbstractTxOut>> + '_>;
    /// Returns the number of outputs
    fn output_len(&self) -> usize;
    /// Returns the output at the given index, if it exists
    fn output_at(&self, index: usize) -> Option<Box<dyn AbstractTxOut>>;
}
