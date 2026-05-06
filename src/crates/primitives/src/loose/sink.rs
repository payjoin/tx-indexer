use std::collections::HashMap;
use std::sync::Arc;

use bitcoin::hashes::{Hash, hash160};

use crate::{
    ScriptPubkeyHash,
    loose::{InMemoryIndex, TxId, TxInId, TxOutId, confirmed_tx::ConfirmedTx},
    traits::IndexSink,
};

/// [`IndexSink`] that builds a [`InMemoryIndex`] from parsed block data.
///
/// Uses lightweight per-tx staging buffers (small ephemeral vecs) to avoid
/// deserializing transactions at index-build time; the raw bytes are stored in
/// [`ConfirmedTx`] for lazy on-demand parsing at query time.
///
/// Only practical for small/test chains (regtest, signet). For mainnet use the
/// dense storage path.
pub struct LooseIndexSink {
    index: InMemoryIndex,
    txid_to_loose: HashMap<[u8; 32], TxId>,
    // Per-tx staging (cleared in on_transaction)
    current_inputs: Vec<([u8; 32], u32)>,
    current_spk_hashes: Vec<ScriptPubkeyHash>,
}

impl LooseIndexSink {
    pub fn new() -> Self {
        Self {
            index: InMemoryIndex::new(),
            txid_to_loose: HashMap::new(),
            current_inputs: Vec::new(),
            current_spk_hashes: Vec::new(),
        }
    }

    /// Consume the sink and return the completed index.
    pub fn finish(self) -> InMemoryIndex {
        self.index
    }
}

impl Default for LooseIndexSink {
    fn default() -> Self {
        Self::new()
    }
}

impl IndexSink for LooseIndexSink {
    type Error = std::convert::Infallible;

    fn on_input(
        &mut self,
        _vin: usize,
        prev_txid: &[u8; 32],
        prev_vout: u32,
    ) -> Result<(), Self::Error> {
        self.current_inputs.push((*prev_txid, prev_vout));
        Ok(())
    }

    fn on_output(&mut self, _vout: usize, script_pubkey: &[u8]) -> Result<(), Self::Error> {
        let spk_hash = hash160::Hash::hash(script_pubkey).to_byte_array();
        self.current_spk_hashes.push(spk_hash);
        Ok(())
    }

    fn on_transaction(
        &mut self,
        txid: &[u8; 32],
        _blk_file_no: u32,
        _blk_file_off: u32,
        _tx_len: u32,
        tx_bytes: &[u8],
    ) -> Result<(), Self::Error> {
        let loose_txid = TxId::new(self.index.txs.len() as u32 + 1);

        // Build prev_txouts / spending_txins from staged inputs.
        for (vin, (prev_txid_raw, prev_vout)) in self.current_inputs.drain(..).enumerate() {
            // Skip coinbase null prevouts.
            if prev_vout == u32::MAX && prev_txid_raw.iter().all(|b| *b == 0) {
                continue;
            }
            if let Some(&prev_loose_txid) = self.txid_to_loose.get(&prev_txid_raw) {
                let prev_out_id = TxOutId::new(prev_loose_txid, prev_vout);
                let vin_id = TxInId::new(loose_txid, vin as u32);
                self.index.prev_txouts.insert(vin_id, prev_out_id);
                self.index.spending_txins.insert(prev_out_id, vin_id);
            }
        }

        // Build spk_to_txout_ids from staged outputs.
        for (vout, spk_hash) in self.current_spk_hashes.drain(..).enumerate() {
            let out_id = TxOutId::new(loose_txid, vout as u32);
            self.index
                .spk_to_txout_ids
                .entry(spk_hash)
                .or_insert(out_id);
        }

        // Store raw bytes for lazy deserialization at query time.
        let arc_bytes: Arc<[u8]> = tx_bytes.into();
        self.index
            .txs
            .insert(loose_txid, Arc::new(ConfirmedTx::new(arc_bytes)));

        self.txid_to_loose.insert(*txid, loose_txid);
        Ok(())
    }

    fn on_block_end(&mut self, _block_tx_count: u64) -> Result<(), Self::Error> {
        Ok(())
    }
}
