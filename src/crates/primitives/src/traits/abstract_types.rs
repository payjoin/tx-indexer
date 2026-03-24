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
    /// Returns the full scriptPubKey bytes
    fn script_pubkey_bytes(&self) -> Vec<u8>;
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

/// Sequence number of a transaction input (needed for RBF detection)
pub trait HasSequence {
    fn sequence(&self) -> u32;
}

/// Witness and scriptSig data for a transaction input
pub trait HasWitnessData {
    fn witness_items(&self) -> Vec<Vec<u8>>;
    // TODO should be in HasScriptSig trait
    fn script_sig_bytes(&self) -> Vec<u8>;
}

/// Full scriptPubKey bytes for a transaction output
pub trait HasScriptPubkey {
    fn script_pubkey_bytes(&self) -> Vec<u8>;
}

/// Transaction version
pub trait HasVersion {
    fn version(&self) -> i32;
}

/// Value (amount) of a transaction output
pub trait HasValue {
    fn value(&self) -> Amount;
}

/// Previous outpoint of a transaction input (for BIP69 sorting)
pub trait HasPrevOutput {
    /// Txid bytes in internal (wire) order
    fn prev_outpoint_txid_bytes(&self) -> [u8; 32];
    fn prev_outpoint_vout(&self) -> u32;
}

// --- bitcoin type impls ---

impl HasSequence for bitcoin::TxIn {
    fn sequence(&self) -> u32 {
        self.sequence.0
    }
}

impl HasWitnessData for bitcoin::TxIn {
    fn witness_items(&self) -> Vec<Vec<u8>> {
        self.witness.iter().map(|item| item.to_vec()).collect()
    }

    fn script_sig_bytes(&self) -> Vec<u8> {
        self.script_sig.to_bytes()
    }
}

impl HasScriptPubkey for bitcoin::TxOut {
    fn script_pubkey_bytes(&self) -> Vec<u8> {
        self.script_pubkey.to_bytes()
    }
}

impl HasVersion for bitcoin::Transaction {
    fn version(&self) -> i32 {
        self.version.0
    }
}

impl HasValue for bitcoin::TxOut {
    fn value(&self) -> Amount {
        self.value
    }
}

impl HasPrevOutput for bitcoin::TxIn {
    fn prev_outpoint_txid_bytes(&self) -> [u8; 32] {
        let txid_ref: &[u8] = self.previous_output.txid.as_ref();
        let mut bytes = [0u8; 32];
        bytes.copy_from_slice(txid_ref);
        bytes
    }

    fn prev_outpoint_vout(&self) -> u32 {
        self.previous_output.vout
    }
}
