use std::{collections::HashMap, path::Path};

use bitcoin_slices::bitcoin_hashes::Hash;
use bitcoin_slices::{Visit, Visitor, bsl};

use bitcoin::hashes::hash160::Hash as Hash160;
use core::ops::ControlFlow;

use crate::{
    ScriptPubkeyHash,
    blk_file::BlkFileStore,
    dense::{BlockFileId, TxId, TxOutId},
    indecies::{ConfirmedTxPtrIndex, DenseIndexSet, INID_NONE, OUTID_NONE, TxPtr},
    sled::spk_db::{SledScriptPubkeyDb, SledScriptPubkeyDbError},
    traits::ScriptPubkeyDb,
};

/// Block file layout: 4-byte magic + 4-byte block size (LE) + block payload.
/// Block 0 starts at offset 8.
const BLOCK_START_LEN: usize = 8;

// TODO: provide option to memory map the block files

/// Parser-side blk file metadata used to locate and bound parsing within a file.
///
/// `file_no` selects the `blkNNNNN.dat` file, `height_first`/`height_last`
/// describe the expected height range in that file, and `data_len` is the
/// logical used byte length when known.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlkFileHint {
    pub file_no: u32,
    pub height_first: u32,
    pub height_last: u32,
    pub data_len: Option<usize>,
}

impl Default for BlkFileHint {
    fn default() -> Self {
        Self {
            file_no: 0,
            height_first: 0,
            height_last: u32::MAX,
            data_len: None,
        }
    }
}

/// Storage for dense IDs backed by Bitcoin Core block files.
///
/// Parses blocks via bitcoin_slices (Visitor pattern).
pub struct Parser {
    store: BlkFileStore,
    /// Blk file layout hints for the files this parser should scan.
    ///
    /// If empty, all blocks are assumed to be in `blk00000.dat` starting at height 0
    /// (suitable for regtest or small test chains).
    file_hints: Vec<BlkFileHint>,
}

impl Parser {
    pub fn new(blocks_dir: impl Into<std::path::PathBuf>) -> Self {
        Self {
            store: BlkFileStore::open(blocks_dir),
            file_hints: Vec::new(),
        }
    }

    /// Set blk file layout hints so the parser can locate blocks in multi-file chains.
    pub fn with_file_hints(mut self, hints: Vec<BlkFileHint>) -> Self {
        self.file_hints = hints;
        self
    }

    pub fn blocks_dir(&self) -> &Path {
        self.store.blocks_dir()
    }

    /// Consume the parser and return the underlying [`BlkFileStore`] for reuse.
    pub fn into_blk_store(self) -> BlkFileStore {
        self.store
    }

    /// Parse blocks in `range` (by global block height) and write index entries.
    ///
    /// Uses file hints to locate the right blk files.  When no hints are set,
    /// falls back to `blk00000.dat` starting at height 0.
    pub fn parse_blocks(
        &mut self,
        range: std::ops::Range<u64>,
        indices: &mut DenseIndexSet,
        spk_db: &mut SledScriptPubkeyDb,
    ) -> Result<(), BlockFileError> {
        // Default: single file starting at height 0.
        let default_hint = [BlkFileHint::default()];
        let hints: &[BlkFileHint] = if self.file_hints.is_empty() {
            &default_hint
        } else {
            &self.file_hints
        };
        log::debug!("Starting to parse blocks in range: {:?}", range);
        let parse_start = std::time::Instant::now();

        let mut tx_total = indices
            .block_tx
            .last()
            .map_err(BlockFileError::Io)?
            .unwrap_or(0) as u64;
        let mut txids = HashMap::new();

        'files: for hint in hints {
            let file_no = hint.file_no;
            let height_first = hint.height_first;
            let file_first = height_first as u64;
            // Skip files entirely before range.
            if file_first > range.end.saturating_sub(1) {
                break;
            }

            let file_id = BlockFileId(file_no);
            let bytes = self.store.read_file(file_no).map_err(BlockFileError::Io)?;
            let used_len = match hint.data_len {
                Some(data_len) => {
                    let used_len = data_len;
                    if used_len > bytes.len() {
                        return Err(BlockFileError::UnexpectedEof {
                            offset: used_len,
                            len: bytes.len(),
                        });
                    }
                    used_len
                }
                None => bytes.len(),
            };
            // Only parse the logical bytes Bitcoin Core says are used. This avoids
            // interpreting the preallocated tail of the active blk file as block data.
            let bytes = &bytes[..used_len];

            let mut global_height = file_first;
            let mut offset = 0usize;

            while offset + BLOCK_START_LEN <= bytes.len() {
                if global_height >= range.end {
                    break 'files;
                }

                let block_size =
                    u32::from_le_bytes(bytes[offset + 4..offset + 8].try_into().map_err(|_| {
                        BlockFileError::UnexpectedEof {
                            offset: offset + 8,
                            len: bytes.len(),
                        }
                    })?) as usize;
                let block_start = offset + BLOCK_START_LEN;
                let block_end = block_start + block_size;
                if block_end > bytes.len() {
                    return Err(BlockFileError::UnexpectedEof {
                        offset: block_end,
                        len: bytes.len(),
                    });
                }

                if global_height >= range.start {
                    let block_slice = &bytes[block_start..block_end];
                    let block_start_in_file = block_start as u64;

                    // Collect all per-block writes into a batch. The collector is
                    // scoped here so its &ConfirmedTxPtrIndex borrow drops before
                    // the bulk flush below mutates indices.
                    let batch = {
                        let txptr_base = indices.txptr.len() as u32;
                        let tx_in_base = indices.in_prevout.len();
                        let tx_out_base = indices.out_spent.len();
                        let mut collector = TxIdCollector {
                            block_file: file_id,
                            block_start_in_file,
                            block_slice,
                            txids: &mut txids,
                            committed_txptr: &indices.txptr,
                            txptr_base,
                            batch: BlockBatch::default(),
                            tx_in_running: tx_in_base,
                            tx_out_running: tx_out_base,
                            tx_out_base,
                            current_in: 0,
                            current_out: 0,
                            spk_db,
                            error: None,
                        };
                        bsl::Block::visit(block_slice, &mut collector)
                            .map_err(BlockFileError::Parse)?;
                        if let Some(error) = collector.error.take() {
                            return Err(error);
                        }
                        collector.batch
                    };

                    // Bulk-append all records from the block, then flush so the
                    // subsequent set() calls target on-disk data.
                    for ptr in &batch.txptrs {
                        indices.txptr.append(*ptr).map_err(BlockFileError::Io)?;
                    }
                    for out_id in &batch.in_prevouts {
                        indices
                            .in_prevout
                            .append(*out_id)
                            .map_err(BlockFileError::Io)?;
                    }
                    for in_id in &batch.out_spents {
                        indices
                            .out_spent
                            .append(*in_id)
                            .map_err(BlockFileError::Io)?;
                    }
                    indices.flush().map_err(BlockFileError::Io)?;
                    for (out_id, in_id) in &batch.out_spent_updates {
                        indices
                            .out_spent
                            .set(*out_id, *in_id)
                            .map_err(BlockFileError::Io)?;
                    }

                    tx_total += batch.txptrs.len() as u64;
                    if tx_total > u32::MAX as u64 {
                        return Err(BlockFileError::CorruptId());
                    }
                    indices
                        .block_tx
                        .append(tx_total as u32)
                        .map_err(BlockFileError::Io)?;
                }

                offset = block_end;
                global_height += 1;
            }
        }

        let parse_duration = parse_start.elapsed();
        log::debug!(
            "Parsing blocks took {} seconds",
            parse_duration.as_secs_f64()
        );

        Ok(())
    }
}

/// Writes staged for a single block before being bulk-applied to the indices.
#[derive(Default)]
struct BlockBatch {
    txptrs: Vec<TxPtr>,
    in_prevouts: Vec<u64>,
    /// One entry per output, all initialised to `INID_NONE`.
    out_spents: Vec<u64>,
    /// `(out_id, in_id)` pairs for outputs spent within or before this block.
    out_spent_updates: Vec<(u64, u64)>,
}

/// Visitor that collects TxIds (file + byte offset) for each transaction in a block.
struct TxIdCollector<'a> {
    block_file: BlockFileId,
    block_start_in_file: u64,
    block_slice: &'a [u8],
    // TODO: This is an unbounded map and will consume many GBs for mainnet.
    // The problem is we need to resolve txids to dense ids.
    txids: &'a mut HashMap<[u8; 32], TxId>,
    /// Read-only view of txptrs committed before this block.
    committed_txptr: &'a ConfirmedTxPtrIndex,
    /// `committed_txptr.len()` at the start of this block, used to distinguish
    /// same-block txptrs (staged in `batch`) from committed ones.
    txptr_base: u32,
    batch: BlockBatch,
    /// Running total of inputs through the end of the last completed tx.
    tx_in_running: u64,
    /// Running total of outputs through the end of the last completed tx.
    tx_out_running: u64,
    /// `out_spent.len()` at block start, used for the sequential-id sanity check.
    tx_out_base: u64,
    current_in: u64,
    current_out: u64,
    spk_db: &'a mut SledScriptPubkeyDb,
    error: Option<BlockFileError>,
}

impl TxIdCollector<'_> {
    fn tx_out_range_for(&self, txid: TxId) -> (u64, u64) {
        let idx = txid.index();
        if idx >= self.txptr_base {
            let i = (idx - self.txptr_base) as usize;
            let end = self.batch.txptrs[i].tx_out_end();
            let start = if i > 0 {
                self.batch.txptrs[i - 1].tx_out_end()
            } else if self.txptr_base > 0 {
                self.committed_txptr
                    .get(TxId::new(self.txptr_base - 1))
                    .unwrap_or_else(|e| panic!("Corrupted data store: error reading txptr index: {:?}", e))
                    .unwrap_or_else(|| panic!("Corrupted data store: txid out of range: {}", self.txptr_base - 1))
                    .tx_out_end()
            } else {
                0
            };
            (start, end)
        } else {
            tx_out_range_for(txid, self.committed_txptr)
        }
    }
}

impl Visitor for TxIdCollector<'_> {
    fn visit_tx_in(&mut self, _vin: usize, tx_in: &bsl::TxIn<'_>) -> ControlFlow<()> {
        let in_id = self.tx_in_running + self.current_in;
        let prevout = tx_in.prevout();
        let out_id = if is_null_prevout(prevout) {
            OUTID_NONE
        } else {
            let bytes = <&[u8; 32]>::try_from(prevout.txid()).expect("prevout txid is 32 bytes");
            if let Some(prev_dense) = self.txids.get(bytes).copied() {
                let (start, end) = self.tx_out_range_for(prev_dense);
                let vout = prevout.vout() as u64;
                let out_id = start + vout;
                if out_id >= end { OUTID_NONE } else { out_id }
            } else {
                OUTID_NONE
            }
        };
        self.batch.in_prevouts.push(out_id);
        if out_id != OUTID_NONE {
            self.batch.out_spent_updates.push((out_id, in_id));
        }
        self.current_in += 1;
        ControlFlow::Continue(())
    }

    fn visit_tx_out(&mut self, _vout: usize, tx_out: &bsl::TxOut<'_>) -> ControlFlow<()> {
        let out_id = self.tx_out_running + self.current_out;
        let expected_out_id = self.tx_out_base + self.batch.out_spents.len() as u64;
        if out_id != expected_out_id {
            self.error = Some(BlockFileError::CorruptId());
            return ControlFlow::Break(());
        }
        let spk_hash = script_pubkey_hash(tx_out.script_pubkey());
        if let Err(err) = self.spk_db.insert_if_absent(spk_hash, TxOutId::new(out_id)) {
            self.error = Some(BlockFileError::SpkDb(err));
            return ControlFlow::Break(());
        }
        self.batch.out_spents.push(INID_NONE);
        self.current_out += 1;
        ControlFlow::Continue(())
    }

    fn visit_transaction(&mut self, tx: &bsl::Transaction<'_>) -> ControlFlow<()> {
        if self.error.is_some() {
            return ControlFlow::Break(());
        }
        let tx_slice = tx.as_ref();
        let tx_len = tx_slice.len();
        if tx_len > u32::MAX as usize {
            self.error = Some(BlockFileError::CorruptId());
            return ControlFlow::Break(());
        }
        let offset_in_block = tx_slice.as_ptr() as usize - self.block_slice.as_ptr() as usize;
        let file_offset = self.block_start_in_file + offset_in_block as u64;
        self.tx_in_running += self.current_in;
        self.tx_out_running += self.current_out;
        let ptr = TxPtr::new(
            self.block_file.0,
            file_offset as u32,
            tx_len as u32,
            self.tx_in_running,
            self.tx_out_running,
        );
        let txid = TxId::new(self.txptr_base + self.batch.txptrs.len() as u32);
        self.batch.txptrs.push(ptr);
        self.txids.insert(tx.txid().to_byte_array(), txid);
        self.current_in = 0;
        self.current_out = 0;
        ControlFlow::Continue(())
    }
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

fn is_null_prevout(prevout: &bsl::OutPoint<'_>) -> bool {
    prevout.vout() == u32::MAX && prevout.txid().iter().all(|b| *b == 0)
}

fn script_pubkey_hash(script_pubkey: &[u8]) -> ScriptPubkeyHash {
    let hash = Hash160::hash(script_pubkey);
    hash.to_byte_array()
}

#[derive(Debug)]
pub enum BlockFileError {
    Io(std::io::Error),
    UnexpectedEof { offset: usize, len: usize },
    Parse(bitcoin_slices::Error),
    SpkDb(SledScriptPubkeyDbError),
    CorruptId(),
    BlockIndex(bitcoin_block_index::Error),
}

impl std::fmt::Display for BlockFileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BlockFileError::Io(e) => write!(f, "io: {}", e),
            BlockFileError::UnexpectedEof { offset, len } => {
                write!(f, "unexpected eof at offset {} (len {})", offset, len)
            }
            BlockFileError::Parse(e) => write!(f, "parse: {:?}", e),
            BlockFileError::SpkDb(e) => write!(f, "spk db: {:?}", e),
            BlockFileError::CorruptId() => write!(f, "corrupt id"),
            BlockFileError::BlockIndex(e) => write!(f, "block index: {e}"),
        }
    }
}

impl std::error::Error for BlockFileError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            BlockFileError::Io(e) => Some(e),
            BlockFileError::Parse(_)
            | BlockFileError::UnexpectedEof { .. }
            | BlockFileError::SpkDb(_)
            | BlockFileError::CorruptId()
            | BlockFileError::BlockIndex(_) => None,
        }
    }
}
