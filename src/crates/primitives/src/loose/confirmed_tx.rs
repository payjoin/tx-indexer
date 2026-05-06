use std::sync::Arc;

use bitcoin::Amount;

use crate::{
    AnyOutId, AnyTxId, ScriptPubkeyHash,
    traits::abstract_types::{AbstractTransaction, AbstractTxIn, AbstractTxOut, HasScriptPubkey},
};

/// A confirmed transaction stored as raw serialized bytes.
///
/// Implements [`AbstractTransaction`] by deserializing on demand — no upfront
/// allocation at index-build time. Suitable for small/test chains only; for
/// mainnet use the dense storage path instead.
pub struct ConfirmedTx {
    bytes: Arc<[u8]>,
}

impl ConfirmedTx {
    pub fn new(bytes: Arc<[u8]>) -> Self {
        Self { bytes }
    }

    fn parse(&self) -> bitcoin::Transaction {
        bitcoin::consensus::deserialize(&self.bytes)
            .expect("ConfirmedTx: bytes must be a valid serialized transaction")
    }
}

struct ConfirmedTxIn {
    prev_txid_bytes: [u8; 32],
    prev_vout: u32,
}

struct ConfirmedTxOut {
    value: Amount,
    script_pubkey: Vec<u8>,
}

impl HasScriptPubkey for ConfirmedTxOut {
    fn script_pubkey_bytes(&self) -> Vec<u8> {
        self.script_pubkey.clone()
    }
}

impl AbstractTxOut for ConfirmedTxOut {
    fn value(&self) -> Amount {
        self.value
    }

    fn script_pubkey_hash(&self) -> ScriptPubkeyHash {
        use bitcoin::hashes::{Hash, hash160};
        hash160::Hash::hash(&self.script_pubkey).to_byte_array()
    }
}

impl AbstractTxIn for ConfirmedTxIn {
    fn prev_txid(&self) -> Option<AnyTxId> {
        // Raw txid bytes cannot be resolved to AnyTxId without storage context.
        None
    }

    fn prev_vout(&self) -> Option<u32> {
        // Coinbase inputs use a null prevout (all-zero txid, vout = u32::MAX).
        if self.prev_vout == u32::MAX && self.prev_txid_bytes.iter().all(|b| *b == 0) {
            None
        } else {
            Some(self.prev_vout)
        }
    }

    fn prev_txout_id(&self) -> Option<AnyOutId> {
        None
    }
}

impl AbstractTransaction for ConfirmedTx {
    fn inputs(&self) -> Box<dyn Iterator<Item = Box<dyn AbstractTxIn + '_>> + '_> {
        let tx = self.parse();
        let items: Vec<Box<dyn AbstractTxIn + 'static>> = tx
            .input
            .iter()
            .map(|txin| {
                let mut prev_txid_bytes = [0u8; 32];
                prev_txid_bytes.copy_from_slice(txin.previous_output.txid.as_ref());
                Box::new(ConfirmedTxIn {
                    prev_txid_bytes,
                    prev_vout: txin.previous_output.vout,
                }) as Box<dyn AbstractTxIn + 'static>
            })
            .collect();
        Box::new(items.into_iter())
    }

    fn outputs(&self) -> Box<dyn Iterator<Item = Box<dyn AbstractTxOut + '_>> + '_> {
        let tx = self.parse();
        let items: Vec<Box<dyn AbstractTxOut + 'static>> = tx
            .output
            .iter()
            .map(|txout| {
                Box::new(ConfirmedTxOut {
                    value: txout.value,
                    script_pubkey: txout.script_pubkey.to_bytes(),
                }) as Box<dyn AbstractTxOut + 'static>
            })
            .collect();
        Box::new(items.into_iter())
    }

    fn input_len(&self) -> usize {
        self.parse().input.len()
    }

    fn output_len(&self) -> usize {
        self.parse().output.len()
    }

    fn output_at(&self, index: usize) -> Option<Box<dyn AbstractTxOut + '_>> {
        let tx = self.parse();
        tx.output.get(index).map(|txout| {
            Box::new(ConfirmedTxOut {
                value: txout.value,
                script_pubkey: txout.script_pubkey.to_bytes(),
            }) as Box<dyn AbstractTxOut + '_>
        })
    }

    fn locktime(&self) -> u32 {
        self.parse().lock_time.to_consensus_u32()
    }

    fn is_coinbase(&self) -> bool {
        self.parse().is_coinbase()
    }
}
