use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::PathBuf,
};

use super::{BlockFileError, BlockFileId, Parser, TxId, TxInId, TxOutId};
use crate::ScriptPubkeyHash;
use crate::confirmed::{
    BlockTxIndex, ConfirmedTxPtrIndex, INID_NONE, InPrevoutIndex, OUTID_NONE, OutSpentByIndex,
    TxPtr,
};
use crate::traits::storage::ScriptPubkeyDb;

pub struct IndexPaths {
    pub txptr: PathBuf,
    pub block_tx: PathBuf,
    pub in_prevout: PathBuf,
    pub out_spent: PathBuf,
}

pub struct DenseStorage {
    blocks_dir: PathBuf,
    txptr_index: ConfirmedTxPtrIndex,
    block_tx_index: BlockTxIndex,
    in_prevout_index: InPrevoutIndex,
    out_spent_index: OutSpentByIndex,
    spk_db: Box<dyn ScriptPubkeyDb<Error = std::io::Error> + Send + Sync>,
}

pub fn build_indices(
    blocks_dir: impl Into<PathBuf>,
    range: std::ops::Range<u64>,
    paths: IndexPaths,
    mut spk_db: Box<dyn ScriptPubkeyDb<Error = std::io::Error> + Send + Sync>,
) -> Result<(DenseStorage, HashMap<bitcoin::Txid, TxId>), BlockFileError> {
    let mut parser = Parser::new(blocks_dir);
    let mut txptr_index = ConfirmedTxPtrIndex::create(&paths.txptr).map_err(BlockFileError::Io)?;
    let mut block_tx_index = BlockTxIndex::create(&paths.block_tx).map_err(BlockFileError::Io)?;
    let mut in_prevout_index =
        InPrevoutIndex::create(&paths.in_prevout).map_err(BlockFileError::Io)?;
    let mut out_spent_index =
        OutSpentByIndex::create(&paths.out_spent).map_err(BlockFileError::Io)?;
    let txids = parser.parse_blocks(
        range,
        &mut txptr_index,
        &mut block_tx_index,
        &mut in_prevout_index,
        &mut out_spent_index,
        &mut spk_db,
    )?;
    let storage = DenseStorage {
        blocks_dir: parser.blocks_dir().to_path_buf(),
        txptr_index,
        block_tx_index,
        in_prevout_index,
        out_spent_index,
        spk_db,
    };
    Ok((storage, txids))
}

impl DenseStorage {
    fn block_file_path(&self, block_file: BlockFileId) -> PathBuf {
        let file_name = format!("blk{:05}.dat", block_file.0);
        self.blocks_dir.join(file_name)
    }

    fn tx_ptr(&self, txid: TxId) -> TxPtr {
        match self.txptr_index.get(txid) {
            Ok(Some(ptr)) => ptr,
            Ok(None) => panic!(
                "Corrupted data store: transaction not found for txid: {:?}",
                txid
            ),
            Err(e) => panic!("Corrupted data store: error reading txptr: {:?}", e),
        }
    }

    /// Return the range of TxIds for the given block height.
    pub fn tx_range_for_block(&self, height: u64) -> (u32, u32) {
        let end = self
            .block_tx_index
            .get(height)
            .unwrap_or_else(|e| {
                panic!(
                    "Corrupted data store: error reading block tx index: {:?}",
                    e
                )
            })
            .unwrap_or_else(|| {
                panic!(
                    "Corrupted data store: block height out of range: {}",
                    height
                )
            });
        if height == 0 {
            (0, end)
        } else {
            let start = self
                .block_tx_index
                .get(height - 1)
                .unwrap_or_else(|e| {
                    panic!(
                        "Corrupted data store: error reading block tx index: {:?}",
                        e
                    )
                })
                .unwrap_or_else(|| {
                    panic!(
                        "Corrupted data store: block height out of range: {}",
                        height - 1
                    )
                });
            (start, end)
        }
    }

    pub fn block_of_tx(&self, txid: TxId) -> u64 {
        let target = txid.index();
        let mut lo = 0u64;
        let mut hi = self.block_tx_index.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let mid_end = self
                .block_tx_index
                .get(mid)
                .unwrap_or_else(|e| {
                    panic!(
                        "Corrupted data store: error reading block tx index: {:?}",
                        e
                    )
                })
                .unwrap_or_else(|| {
                    panic!("Corrupted data store: block height out of range: {}", mid)
                });
            if mid_end > target {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        if lo >= self.block_tx_index.len() {
            panic!(
                "Corrupted data store: txid out of range for block index: {}",
                target
            );
        }
        lo
    }

    pub fn tx_in_range(&self, txid: TxId) -> (u64, u64) {
        let end = self.tx_ptr(txid).tx_in_end();
        if txid.index() == 0 {
            (0, end)
        } else {
            let prev = self.tx_ptr(TxId::new(txid.index() - 1)).tx_in_end();
            (prev, end)
        }
    }

    pub fn tx_out_range(&self, txid: TxId) -> (u64, u64) {
        let end = self.tx_ptr(txid).tx_out_end();
        if txid.index() == 0 {
            (0, end)
        } else {
            let prev = self.tx_ptr(TxId::new(txid.index() - 1)).tx_out_end();
            (prev, end)
        }
    }

    fn upper_bound_tx_out(&self, out_id: u64) -> TxId {
        let mut lo = 0u64;
        let mut hi = self.txptr_index.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let mid_id = TxId::new(mid as u32);
            let mid_end = self.tx_ptr(mid_id).tx_out_end();
            if mid_end > out_id {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        if lo >= self.txptr_index.len() {
            panic!("Corrupted data store: output id out of range: {}", out_id);
        }
        TxId::new(lo as u32)
    }

    fn upper_bound_tx_in(&self, in_id: u64) -> TxId {
        let mut lo = 0u64;
        let mut hi = self.txptr_index.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let mid_id = TxId::new(mid as u32);
            let mid_end = self.tx_ptr(mid_id).tx_in_end();
            if mid_end > in_id {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        if lo >= self.txptr_index.len() {
            panic!("Corrupted data store: input id out of range: {}", in_id);
        }
        TxId::new(lo as u32)
    }

    fn txid_and_vout_for_out(&self, out_id: TxOutId) -> (TxId, u32) {
        let txid = self.upper_bound_tx_out(out_id.index());
        let (start, _end) = self.tx_out_range(txid);
        let vout = out_id.index() - start;
        (txid, vout as u32)
    }

    pub fn txid_for_out(&self, out_id: TxOutId) -> TxId {
        self.txid_and_vout_for_out(out_id).0
    }

    fn txid_and_vin_for_in(&self, in_id: TxInId) -> (TxId, u32) {
        let txid = self.upper_bound_tx_in(in_id.index());
        let (start, _end) = self.tx_in_range(txid);
        let vin = in_id.index() - start;
        (txid, vin as u32)
    }

    pub fn txid_for_in(&self, in_id: TxInId) -> TxId {
        self.txid_and_vin_for_in(in_id).0
    }

    pub fn prevout_for_in(&self, in_id: TxInId) -> Option<TxOutId> {
        let out_id = self
            .in_prevout_index
            .get(in_id.index())
            .unwrap_or_else(|e| {
                panic!(
                    "Corrupted data store: error reading in_prevout index: {:?}",
                    e
                )
            })
            .unwrap_or_else(|| {
                panic!(
                    "Corrupted data store: input id out of range: {}",
                    in_id.index()
                )
            });
        if out_id == OUTID_NONE {
            None
        } else {
            Some(TxOutId::new(out_id))
        }
    }

    pub fn spender_for_out(&self, out_id: TxOutId) -> Option<TxInId> {
        let in_id = self
            .out_spent_index
            .get(out_id.index())
            .unwrap_or_else(|e| {
                panic!(
                    "Corrupted data store: error reading out_spent index: {:?}",
                    e
                )
            })
            .unwrap_or_else(|| {
                panic!(
                    "Corrupted data store: output id out of range: {}",
                    out_id.index()
                )
            });
        if in_id == INID_NONE {
            None
        } else {
            Some(TxInId::new(in_id))
        }
    }

    /// Read a transaction from disk into a buffer.
    fn read_tx(
        &self,
        block_file: BlockFileId,
        tx_offset: u32,
        tx_len: u32,
    ) -> Result<Vec<u8>, BlockFileError> {
        let path = self.block_file_path(block_file);
        let mut f = File::open(&path).map_err(BlockFileError::Io)?;
        f.seek(SeekFrom::Start(tx_offset as u64))
            .map_err(BlockFileError::Io)?;
        let mut buf = vec![0u8; tx_len as usize];
        f.read_exact(&mut buf).map_err(BlockFileError::Io)?;
        Ok(buf)
    }

    /// Return the transaction at the given dense TxId as a rust-bitcoin Transaction.
    pub fn get_tx(&self, txid: TxId) -> bitcoin::Transaction {
        let ptr = self.tx_ptr(txid);
        let block_file = BlockFileId(ptr.blk_file_no());
        let tx_offset = ptr.blk_file_off();
        let tx_bytes = self
            .read_tx(block_file, tx_offset, ptr.tx_len())
            .unwrap_or_else(|e| panic!("Corrupted data store: error reading tx: {:?}", e));
        bitcoin::consensus::deserialize::<bitcoin::Transaction>(&tx_bytes).unwrap_or_else(|e| {
            panic!("Corrupted data store: error parsing tx: {:?}", e);
        })
    }

    /// Return the transaction output at the given dense TxOutId as a rust-bitcoin TxOut.
    pub fn get_txout(&self, id: TxOutId) -> bitcoin::TxOut {
        let (txid, vout) = self.txid_and_vout_for_out(id);
        let tx = self.get_tx(txid);
        tx.output
            .get(vout as usize)
            .unwrap_or_else(|| {
                panic!(
                    "Corrupted data store: transaction output not found for id: {:?}",
                    id
                );
            })
            .clone()
    }

    /// Return the transaction input at the given dense TxInId as a rust-bitcoin TxIn.
    pub fn get_txin(&self, id: TxInId) -> bitcoin::TxIn {
        let (txid, vin) = self.txid_and_vin_for_in(id);
        let tx = self.get_tx(txid);
        tx.input
            .get(vin as usize)
            .unwrap_or_else(|| {
                panic!(
                    "Corrupted data store: transaction input not found for id: {:?}",
                    id
                );
            })
            .clone()
    }

    /// Return all dense TxInIds for the transaction at the given dense TxId.
    pub fn get_txin_ids(&self, txid: TxId) -> Vec<TxInId> {
        let (start, end) = self.tx_in_range(txid);
        (start..end).map(TxInId::new).collect()
    }

    /// Return all dense TxOutIds for the transaction at the given dense TxId.
    pub fn get_txout_ids(&self, txid: TxId) -> Vec<TxOutId> {
        let (start, end) = self.tx_out_range(txid);
        (start..end).map(TxOutId::new).collect()
    }

    /// Return the first dense TxOutId that uses the given script pubkey hash.
    pub fn script_pubkey_to_txout_id(
        &self,
        script_pubkey: &ScriptPubkeyHash,
    ) -> Result<Option<TxOutId>, BlockFileError> {
        self.spk_db
            .get(script_pubkey)
            .map_err(BlockFileError::SpkDb)
    }
}
