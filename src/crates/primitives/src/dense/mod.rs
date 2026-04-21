use std::path::PathBuf;

use crate::{
    ScriptPubkeyHash,
    blk_file::BlkFileStore,
    indecies::{
        BlockTxIndex, ConfirmedTxPtrIndex, INID_NONE, InPrevoutIndex, OUTID_NONE, OutSpentByIndex,
        TxPtr,
    },
    parser::{BlkFileHint, BlockFileError, Parser},
    sled::{db::SledDBFactory, spk_db::SledScriptPubkeyDb},
    traits::ScriptPubkeyDb,
    unified::SyncError,
};

#[repr(transparent)]
#[derive(PartialEq, Eq, Hash, Copy, Clone, Debug, Ord, PartialOrd)]
pub struct BlockFileId(pub u32);

#[repr(transparent)]
#[derive(PartialEq, Eq, Hash, Copy, Clone, Debug, Ord, PartialOrd)]
/// The i-th transaction in chain order.
pub struct TxId(pub u32);

impl TxId {
    pub fn new(txid: u32) -> Self {
        Self(txid)
    }

    pub fn index(self) -> u32 {
        self.0
    }
}

#[repr(transparent)]
#[derive(PartialEq, Eq, Hash, Copy, Clone, Debug, Ord, PartialOrd)]
/// The i-th output in chain order.
pub struct TxOutId(pub u64);

impl TxOutId {
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    pub fn index(self) -> u64 {
        self.0
    }
}

#[repr(transparent)]
#[derive(PartialEq, Eq, Hash, Copy, Clone, Debug, Ord, PartialOrd)]
/// The i-th input in chain order.
pub struct TxInId(pub u64);

impl TxInId {
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    pub fn index(self) -> u64 {
        self.0
    }
}

pub struct IndexPaths {
    pub txptr: PathBuf,
    pub block_tx: PathBuf,
    pub in_prevout: PathBuf,
    pub out_spent: PathBuf,
}

// TODO: new method for above

pub struct DenseStorageBuilder {
    data_dir: PathBuf,
    index_dir: PathBuf,
    range: std::ops::Range<u64>,
    file_hints: Vec<BlkFileHint>,
}

impl DenseStorageBuilder {
    pub fn new(
        data_dir: PathBuf,
        index_dir: PathBuf,
        range: std::ops::Range<u64>,
        file_hints: Vec<BlkFileHint>,
    ) -> Self {
        Self {
            data_dir,
            index_dir,
            range,
            file_hints,
        }
    }

    /// Build a [`DenseStorage`] for every block from genesis up to the chain tip.
    ///
    /// `data_dir` is Bitcoin Core's data directory (e.g. `~/.bitcoin/` or
    /// `~/.bitcoin/regtest/`); `blocks/` and `blocks/index/` are derived from it automatically.
    ///
    /// `index_dir` is the output directory where all dense index files and the sled database
    /// will be written. The caller is responsible for creating this directory before calling.
    pub fn sync_from_genesis(
        data_dir: PathBuf,
        index_dir: PathBuf,
    ) -> Result<Self, BlockFileError> {
        use bitcoin_block_index::BlockIndex;
        let block_index_path = data_dir.join("blocks/index");

        let mut index = BlockIndex::open(&block_index_path).map_err(BlockFileError::BlockIndex)?;

        let tip_hash = index.best_block().map_err(BlockFileError::BlockIndex)?;
        let tip_loc = index
            .block_location(&tip_hash)
            .map_err(BlockFileError::BlockIndex)?;
        let end_height = tip_loc.height as u64;

        let file_hints = Self::collect_file_hints(&mut index, 0, end_height)?;

        let builder = DenseStorageBuilder {
            data_dir,
            index_dir,
            range: 0..end_height + 1,
            file_hints,
        };
        Ok(builder)
    }

    /// Build a [`DenseStorage`] for the `depth + 1` blocks ending at the chain tip.
    ///
    /// `bitcoind_datadir` is Bitcoin Core's data directory (e.g. `~/.bitcoin/` or
    /// `~/.bitcoin/regtest/`); `blocks/` and `blocks/index/` are derived from it automatically.
    ///
    /// `index_dir` is the output directory where all dense index files and the sled database
    /// will be written. The caller is responsible for creating this directory before calling.
    pub fn sync_from_tip(
        data_dir: PathBuf,
        index_dir: PathBuf,
        depth: u32,
    ) -> Result<Self, BlockFileError> {
        // TODO: check if the indecies were built already past or before the depth
        use bitcoin_block_index::BlockIndex;
        let block_index_path = data_dir.join("blocks/index");

        let mut index = BlockIndex::open(&block_index_path).map_err(BlockFileError::BlockIndex)?;

        let tip_hash = index.best_block().map_err(BlockFileError::BlockIndex)?;
        let chain = index
            .walk_back(&tip_hash, depth)
            .map_err(BlockFileError::BlockIndex)?;

        let start_height = chain
            .first()
            .expect("walk_back returns depth+1 items")
            .height as u64;
        let end_height = chain
            .last()
            .expect("walk_back returns depth+1 items")
            .height as u64;

        let file_hints = Self::collect_file_hints(&mut index, start_height, end_height)?;

        let builder = DenseStorageBuilder {
            data_dir,
            index_dir,
            range: start_height..end_height + 1,
            file_hints,
        };
        Ok(builder)
    }

    /// Collect [`BlkFileHint`] entries for every blk file that
    /// overlaps the inclusive height range `[start_height, end_height]`, so the parser
    /// can skip blk files outside the requested range.
    fn collect_file_hints(
        index: &mut bitcoin_block_index::BlockIndex,
        start_height: u64,
        end_height: u64,
    ) -> Result<Vec<BlkFileHint>, BlockFileError> {
        let last_file = index
            .last_block_file()
            .map_err(BlockFileError::BlockIndex)?;
        let mut file_hints: Vec<BlkFileHint> = Vec::new();

        for file_no in 0..=last_file {
            let info = index
                .block_file_info(file_no)
                .map_err(BlockFileError::BlockIndex)?;
            if (info.height_last as u64) < start_height {
                continue;
            }
            // TODO: This assumes block heights are monotonically increasing.
            // Due to reorgs this may not always be the case
            if (info.height_first as u64) > end_height {
                break;
            }
            file_hints.push(BlkFileHint {
                file_no,
                height_first: info.height_first,
                height_last: info.height_last,
                data_len: Some(info.size as usize),
            });
        }

        Ok(file_hints)
    }

    pub fn build(self) -> Result<DenseStorage, SyncError> {
        build_indices(self)
    }
}

pub(crate) fn build_indices(builder: DenseStorageBuilder) -> Result<DenseStorage, SyncError> {
    let datadir = builder.data_dir;
    let blocks_dir = datadir.join("blocks");
    let index_dir = builder.index_dir;
    let paths = IndexPaths {
        txptr: index_dir.join("txptr.bin"),
        block_tx: index_dir.join("block_tx.bin"),
        in_prevout: index_dir.join("in_prevout.bin"),
        out_spent: index_dir.join("out_spent.bin"),
    };
    let mut spk_db = SledDBFactory::open(index_dir.join("spk_db"))
        .map_err(SyncError::Sled)?
        .spk_db()
        .map_err(SyncError::Sled)?;

    let block_height_offset = builder.range.start;
    let mut parser = Parser::new(blocks_dir).with_file_hints(builder.file_hints);
    let mut txptr_index = ConfirmedTxPtrIndex::create(&paths.txptr)
        .map_err(|e| SyncError::Parse(BlockFileError::Io(e)))?;
    let mut block_tx_index = BlockTxIndex::create(&paths.block_tx)
        .map_err(|e| SyncError::Parse(BlockFileError::Io(e)))?;
    let mut in_prevout_index = InPrevoutIndex::create(&paths.in_prevout)
        .map_err(|e| SyncError::Parse(BlockFileError::Io(e)))?;
    let mut out_spent_index = OutSpentByIndex::create(&paths.out_spent)
        .map_err(|e| SyncError::Parse(BlockFileError::Io(e)))?;
    parser
        .parse_blocks(
            builder.range,
            &mut txptr_index,
            &mut block_tx_index,
            &mut in_prevout_index,
            &mut out_spent_index,
            &mut spk_db,
        )
        .map_err(SyncError::Parse)?;
    let storage = DenseStorage {
        store: parser.into_blk_store(),
        block_height_offset,
        txptr_index,
        block_tx_index,
        in_prevout_index,
        out_spent_index,
        spk_db,
    };
    Ok(storage)
}
pub struct DenseStorage {
    store: BlkFileStore,
    block_height_offset: u64,
    txptr_index: ConfirmedTxPtrIndex,
    block_tx_index: BlockTxIndex,
    in_prevout_index: InPrevoutIndex,
    out_spent_index: OutSpentByIndex,
    spk_db: SledScriptPubkeyDb,
}

impl DenseStorage {
    pub fn tx_count(&self) -> u64 {
        self.txptr_index.len()
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

    /// Returns the smallest index `i` in `0..len` such that `value_at(i) > target`, or `None` if none.
    fn upper_bound(len: u64, target: u64, value_at: impl Fn(u64) -> u64) -> Option<u64> {
        let mut lo = 0u64;
        let mut hi = len;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if value_at(mid) > target {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        if lo >= len { None } else { Some(lo) }
    }

    pub fn block_of_tx(&self, txid: TxId) -> u64 {
        let target = txid.index() as u64;
        let len = self.block_tx_index.len();
        let value_at = |i: u64| {
            self.block_tx_index
                .get(i)
                .unwrap_or_else(|e| {
                    panic!(
                        "Corrupted data store: error reading block tx index: {:?}",
                        e
                    )
                })
                .unwrap_or_else(|| panic!("Corrupted data store: block height out of range: {}", i))
                as u64
        };
        let relative = Self::upper_bound(len, target, value_at).unwrap_or_else(|| {
            panic!(
                "Corrupted data store: txid out of range for block index: {}",
                target
            )
        });
        relative + self.block_height_offset
    }

    /// Return the range of TxInIds for the given transaction.
    pub fn tx_in_range(&self, txid: TxId) -> (u64, u64) {
        let end = self.tx_ptr(txid).tx_in_end();
        if txid.index() == 0 {
            (0, end)
        } else {
            let prev = self.tx_ptr(TxId::new(txid.index() - 1)).tx_in_end();
            (prev, end)
        }
    }

    /// Return the range of TxOutIds for the given transaction.
    pub fn tx_out_range(&self, txid: TxId) -> (u64, u64) {
        let end = self.tx_ptr(txid).tx_out_end();
        if txid.index() == 0 {
            (0, end)
        } else {
            let prev = self.tx_ptr(TxId::new(txid.index() - 1)).tx_out_end();
            (prev, end)
        }
    }

    /// Return the transaction id for the given TxOutId.
    pub fn txid_for_out(&self, out_id: TxOutId) -> TxId {
        let len = self.txptr_index.len();
        let value_at = |i: u64| self.tx_ptr(TxId::new(i as u32)).tx_out_end();
        let lo = Self::upper_bound(len, out_id.index(), value_at).unwrap_or_else(|| {
            panic!("Corrupted data store: output id out of range: {:?}", out_id)
        });
        TxId::new(lo as u32)
    }

    /// Return the transaction id for the given TxInId.
    pub fn txid_for_in(&self, in_id: TxInId) -> TxId {
        let len = self.txptr_index.len();
        let value_at = |i: u64| self.tx_ptr(TxId::new(i as u32)).tx_in_end();
        let lo = Self::upper_bound(len, in_id.index(), value_at)
            .unwrap_or_else(|| panic!("Corrupted data store: input id out of range: {:?}", in_id));
        TxId::new(lo as u32)
    }

    fn txid_and_vout_for_out(&self, out_id: TxOutId) -> (TxId, u32) {
        let txid = self.txid_for_out(out_id);
        let (start, _end) = self.tx_out_range(txid);
        let vout = out_id.index() - start;
        (txid, vout as u32)
    }

    fn txid_and_vin_for_in(&self, in_id: TxInId) -> (TxId, u32) {
        let txid = self.txid_for_in(in_id);
        let (start, _end) = self.tx_in_range(txid);
        let vin = in_id.index() - start;
        (txid, vin as u32)
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
        self.store
            .read_at(block_file.0, tx_offset, tx_len)
            .map_err(BlockFileError::Io)
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
