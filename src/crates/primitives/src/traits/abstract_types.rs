use bitcoin::Amount;

use crate::{AnyOutId, AnyTxId, ScriptPubkeyHash};

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
    fn spent_coins(&self) -> impl Iterator<Item = AnyOutId>;
}

// TODO: find a better name for this
pub trait EnumerateOutputValueInArbitraryOrder: AbstractTransaction {
    fn output_values(&self) -> impl Iterator<Item = Amount>;
}

pub trait EnumerateInputValueInArbitraryOrder: AbstractTransaction {
    fn input_values(&self) -> impl Iterator<Item = Amount>;
}

/// Trait for transaction inputs
pub trait AbstractTxIn {
    /// Returns the transaction ID of the previous output
    fn prev_txid(&self) -> Option<AnyTxId>;
    /// Returns the output index of the previous output
    fn prev_vout(&self) -> Option<u32>;
    /// Returns the previous output ID
    fn prev_txout_id(&self) -> Option<AnyOutId>;
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
    /// Returns an iterator over transaction inputs
    fn inputs(&self) -> Box<dyn Iterator<Item = Box<dyn AbstractTxIn + '_>> + '_>;
    /// Returns an iterator over transaction outputs
    fn outputs(&self) -> Box<dyn Iterator<Item = Box<dyn AbstractTxOut + '_>> + '_>;
    /// Returns the number of outputs
    fn output_len(&self) -> usize;
    /// Returns the output at the given index, if it exists
    fn output_at(&self, index: usize) -> Option<Box<dyn AbstractTxOut + '_>>;

    fn locktime(&self) -> u32;
}
