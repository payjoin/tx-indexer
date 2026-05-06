use std::collections::HashMap;

use bitcoin::hashes::hash160::Hash as Hash160;
use bitcoin_slices::bitcoin_hashes::Hash;

use crate::{
    ScriptPubkeyHash,
    dense::{TxId, TxOutId},
    indecies::{ConfirmedTxPtrIndex, DenseIndexSet, INID_NONE, OUTID_NONE, TxPtr},
    parser::BlockFileError,
    sled::spk_db::SledScriptPubkeyDb,
    traits::{IndexSink, ScriptPubkeyDb},
};

/// [`IndexSink`] implementation that writes into the dense binary index files.
///
/// Moves all dense-specific state out of the parser visitor so the parser
/// itself stays storage-agnostic.
pub struct DenseIndexSink<'a> {
    pub(crate) indices: &'a mut DenseIndexSet,
    spk_db: &'a mut SledScriptPubkeyDb,
    // TODO: unbounded — grows to hold every txid seen. Needs a cap / eviction strategy for mainnet.
    txids: HashMap<[u8; 32], TxId>,
    tx_in_total: u64,
    tx_out_total: u64,
    tx_total: u64,
    current_in_count: u64,
    current_out_count: u64,
}

impl<'a> DenseIndexSink<'a> {
    pub fn new(
        indices: &'a mut DenseIndexSet,
        spk_db: &'a mut SledScriptPubkeyDb,
    ) -> Result<Self, BlockFileError> {
        let (tx_in_total, tx_out_total) = tx_io_totals(&indices.txptr);
        let tx_total = indices
            .block_tx
            .last()
            .map_err(BlockFileError::Io)?
            .unwrap_or(0) as u64;
        Ok(Self {
            indices,
            spk_db,
            txids: HashMap::new(),
            tx_in_total,
            tx_out_total,
            tx_total,
            current_in_count: 0,
            current_out_count: 0,
        })
    }
}

impl IndexSink for DenseIndexSink<'_> {
    type Error = BlockFileError;

    fn on_input(
        &mut self,
        vin: usize,
        prev_txid: &[u8; 32],
        prev_vout: u32,
    ) -> Result<(), BlockFileError> {
        let in_id = self.tx_in_total + vin as u64;
        let out_id = if is_null_prevout(prev_txid, prev_vout) {
            OUTID_NONE
        } else if let Some(prev_dense) = self.txids.get(prev_txid).copied() {
            let (start, end) = tx_out_range_for(prev_dense, &self.indices.txptr);
            let candidate = start + prev_vout as u64;
            if candidate < end {
                candidate
            } else {
                OUTID_NONE
            }
        } else {
            OUTID_NONE
        };
        self.indices
            .in_prevout
            .append(out_id)
            .map_err(BlockFileError::Io)?;
        if out_id != OUTID_NONE {
            self.indices
                .out_spent
                .set(out_id, in_id)
                .map_err(BlockFileError::Io)?;
        }
        self.current_in_count += 1;
        Ok(())
    }

    fn on_output(&mut self, vout: usize, script_pubkey: &[u8]) -> Result<(), BlockFileError> {
        let out_id = self.tx_out_total + vout as u64;
        self.indices
            .out_spent
            .append(INID_NONE)
            .map_err(BlockFileError::Io)?;
        if out_id != self.indices.out_spent.len() - 1 {
            return Err(BlockFileError::CorruptId());
        }
        let spk_hash = script_pubkey_hash(script_pubkey);
        self.spk_db
            .insert_if_absent(spk_hash, TxOutId::new(out_id))
            .map_err(BlockFileError::SpkDb)?;
        self.current_out_count += 1;
        Ok(())
    }

    fn on_transaction(
        &mut self,
        txid: &[u8; 32],
        blk_file_no: u32,
        blk_file_off: u32,
        tx_len: u32,
        _tx_bytes: &[u8],
    ) -> Result<(), BlockFileError> {
        self.tx_in_total += self.current_in_count;
        self.tx_out_total += self.current_out_count;
        let ptr = TxPtr::new(
            blk_file_no,
            blk_file_off,
            tx_len,
            self.tx_in_total,
            self.tx_out_total,
        );
        let dense_txid = self.indices.txptr.append(ptr).map_err(BlockFileError::Io)?;
        self.txids.insert(*txid, dense_txid);
        self.current_in_count = 0;
        self.current_out_count = 0;
        Ok(())
    }

    fn on_block_end(&mut self, block_tx_count: u64) -> Result<(), BlockFileError> {
        self.tx_total += block_tx_count;
        if self.tx_total > u32::MAX as u64 {
            return Err(BlockFileError::CorruptId());
        }
        self.indices
            .block_tx
            .append(self.tx_total as u32)
            .map_err(BlockFileError::Io)?;
        Ok(())
    }
}

fn tx_io_totals(txptr_index: &ConfirmedTxPtrIndex) -> (u64, u64) {
    if txptr_index.is_empty() {
        return (0, 0);
    }
    let last = TxId::new((txptr_index.len() - 1) as u32);
    let ptr = txptr_index
        .get(last)
        .unwrap_or_else(|e| panic!("Corrupted data store: error reading txptr: {:?}", e))
        .unwrap_or_else(|| {
            panic!(
                "Corrupted data store: transaction not found for txid: {:?}",
                last
            )
        });
    (ptr.tx_in_end(), ptr.tx_out_end())
}

fn tx_out_range_for(txid: TxId, txptr_index: &ConfirmedTxPtrIndex) -> (u64, u64) {
    let end = txptr_index
        .get(txid)
        .unwrap_or_else(|e| panic!("Corrupted data store: error reading txptr index: {:?}", e))
        .unwrap_or_else(|| panic!("Corrupted data store: txid out of range: {:?}", txid))
        .tx_out_end();
    if txid.index() == 0 {
        (0, end)
    } else {
        let prev = txptr_index
            .get(TxId::new(txid.index() - 1))
            .unwrap_or_else(|e| panic!("Corrupted data store: error reading txptr index: {:?}", e))
            .unwrap_or_else(|| {
                panic!(
                    "Corrupted data store: txid out of range: {:?}",
                    txid.index() - 1
                )
            })
            .tx_out_end();
        (prev, end)
    }
}

fn is_null_prevout(prev_txid: &[u8; 32], prev_vout: u32) -> bool {
    prev_vout == u32::MAX && prev_txid.iter().all(|b| *b == 0)
}

fn script_pubkey_hash(script_pubkey: &[u8]) -> ScriptPubkeyHash {
    Hash160::hash(script_pubkey).to_byte_array()
}
