use std::sync::Arc;

use bitcoin::Amount;
use bitcoin_slices::{Visit, Visitor, bsl};
use core::ops::ControlFlow;

use crate::{
    AnyOutId, AnyTxId, ScriptPubkeyHash,
    traits::abstract_types::{AbstractTransaction, AbstractTxIn, AbstractTxOut, HasScriptPubkey},
};

/// A confirmed transaction stored as raw serialized bytes.
///
/// Implements [`AbstractTransaction`] via `bitcoin_slices` zero-copy parsing —
/// no full `bitcoin::Transaction` allocation, ever. Each method does one
/// visitor pass over the bytes, extracting only the data it needs.
///
/// Suitable for small/test chains only. For mainnet use the dense storage path.
pub struct ConfirmedTx {
    bytes: Arc<[u8]>,
}

impl ConfirmedTx {
    pub fn new(bytes: Arc<[u8]>) -> Self {
        Self { bytes }
    }

    fn parse_all(&self) -> ParsedTx {
        let mut visitor = ParsedTx::default();
        bsl::Transaction::visit(&self.bytes, &mut visitor)
            .expect("ConfirmedTx: bytes must be a valid serialized transaction");
        visitor
    }
}

#[derive(Default)]
struct ParsedTx {
    inputs: Vec<ConfirmedTxIn>,
    outputs: Vec<ConfirmedTxOut>,
    locktime: u32,
    version: i32,
}

impl Visitor for ParsedTx {
    fn visit_tx_in(&mut self, _vin: usize, tx_in: &bsl::TxIn<'_>) -> ControlFlow<()> {
        let prevout = tx_in.prevout();
        let mut prev_txid_bytes = [0u8; 32];
        prev_txid_bytes.copy_from_slice(prevout.txid());
        self.inputs.push(ConfirmedTxIn {
            prev_txid_bytes,
            prev_vout: prevout.vout(),
            sequence: tx_in.sequence(),
        });
        ControlFlow::Continue(())
    }

    fn visit_tx_out(&mut self, _vout: usize, tx_out: &bsl::TxOut<'_>) -> ControlFlow<()> {
        self.outputs.push(ConfirmedTxOut {
            value: Amount::from_sat(tx_out.value()),
            script_pubkey: tx_out.script_pubkey().to_vec(),
        });
        ControlFlow::Continue(())
    }

    fn visit_transaction(&mut self, tx: &bsl::Transaction<'_>) -> ControlFlow<()> {
        self.locktime = tx.locktime();
        self.version = tx.version();
        ControlFlow::Continue(())
    }
}

struct ConfirmedTxIn {
    prev_txid_bytes: [u8; 32],
    prev_vout: u32,
    sequence: u32,
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
        // Coinbase inputs have a null prevout (all-zero txid, vout = u32::MAX).
        if self.prev_vout == u32::MAX && self.prev_txid_bytes.iter().all(|b| *b == 0) {
            None
        } else {
            Some(self.prev_vout)
        }
    }

    fn prev_txout_id(&self) -> Option<AnyOutId> {
        None
    }

    fn sequence(&self) -> u32 {
        self.sequence
    }
}

impl AbstractTransaction for ConfirmedTx {
    fn inputs(&self) -> Box<dyn Iterator<Item = Box<dyn AbstractTxIn + '_>> + '_> {
        let parsed = self.parse_all();
        let items: Vec<Box<dyn AbstractTxIn + 'static>> = parsed
            .inputs
            .into_iter()
            .map(|i| Box::new(i) as Box<dyn AbstractTxIn + 'static>)
            .collect();
        Box::new(items.into_iter())
    }

    fn outputs(&self) -> Box<dyn Iterator<Item = Box<dyn AbstractTxOut + '_>> + '_> {
        let parsed = self.parse_all();
        let items: Vec<Box<dyn AbstractTxOut + 'static>> = parsed
            .outputs
            .into_iter()
            .map(|o| Box::new(o) as Box<dyn AbstractTxOut + 'static>)
            .collect();
        Box::new(items.into_iter())
    }

    fn input_len(&self) -> usize {
        self.parse_all().inputs.len()
    }

    fn output_len(&self) -> usize {
        self.parse_all().outputs.len()
    }

    fn output_at(&self, index: usize) -> Option<Box<dyn AbstractTxOut + '_>> {
        self.parse_all()
            .outputs
            .into_iter()
            .nth(index)
            .map(|o| Box::new(o) as Box<dyn AbstractTxOut + '_>)
    }

    fn locktime(&self) -> u32 {
        self.parse_all().locktime
    }

    fn is_coinbase(&self) -> bool {
        self.parse_all()
            .inputs
            .first()
            .map(|i| i.prev_vout().is_none())
            .unwrap_or(false)
    }
}
