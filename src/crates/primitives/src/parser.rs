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

        let (mut tx_in_total, mut tx_out_total) = tx_io_totals(&indices.txptr);
        let mut tx_total = indices
            .block_tx
            .last()
            .map_err(BlockFileError::Io)?
            .unwrap_or(0) as u64;
        let mut txids = HashMap::new();
        // In-memory shadow of `tx_out_end` for txs appended during *this* parse
        // session. Index 0 holds the pre-session base (= total outputs in all
        // previously-confirmed txs), and slot `i + 1` holds the cumulative
        // `tx_out_end` after the i-th tx of this session. Lets `visit_tx_in`
        // resolve a prevout's `(out_start, out_end)` without re-reading the
        // freshly appended `txptr` index.
        let first_session_txid = u32::try_from(indices.txptr.len()).map_err(|_| {
            BlockFileError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "txptr index exceeds u32::MAX entries",
            ))
        })?;
        let mut tx_out_ends: Vec<u64> = vec![tx_out_total];

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
                    let mut collector = TxIdCollector {
                        block_file: file_id,
                        block_start_in_file,
                        block_slice,
                        indices,
                        error: None,
                        tx_in_total: &mut tx_in_total,
                        tx_out_total: &mut tx_out_total,
                        tx_count: 0,
                        current_in: 0,
                        current_out: 0,
                        spk_db,
                        txids: &mut txids,
                        tx_out_ends: &mut tx_out_ends,
                        first_session_txid,
                    };
                    bsl::Block::visit(block_slice, &mut collector)
                        .map_err(BlockFileError::Parse)?;
                    if let Some(error) = collector.error.take() {
                        return Err(error);
                    }
                    tx_total += collector.tx_count;
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

/// Visitor that collects TxIds (file + byte offset) for each transaction in a block.
struct TxIdCollector<'a> {
    block_file: BlockFileId,
    block_start_in_file: u64,
    block_slice: &'a [u8],
    // TODO: This is an unbounded map and will consume many GBs for mainnet.
    // The problem is we need to resolve txids to dense ids.
    txids: &'a mut HashMap<[u8; 32], TxId>,
    indices: &'a mut DenseIndexSet,
    error: Option<BlockFileError>,
    tx_in_total: &'a mut u64,
    tx_out_total: &'a mut u64,
    tx_count: u64,
    current_in: u64,
    current_out: u64,
    spk_db: &'a mut SledScriptPubkeyDb,
    /// Session-local shadow of `tx_out_end`. See `parse_blocks` for layout.
    tx_out_ends: &'a mut Vec<u64>,
    /// First TxId appended in this parse session; used to translate dense
    /// TxIds into indices into `tx_out_ends`.
    first_session_txid: u32,
}

impl Visitor for TxIdCollector<'_> {
    fn visit_tx_in(&mut self, _vin: usize, tx_in: &bsl::TxIn<'_>) -> ControlFlow<()> {
        let in_id = *self.tx_in_total + self.current_in;
        let prevout = tx_in.prevout();
        let out_id = if is_null_prevout(prevout) {
            OUTID_NONE
        } else {
            let bytes = <&[u8; 32]>::try_from(prevout.txid()).expect("prevout txid is 32 bytes");
            if let Some(prev_dense) = self.txids.get(bytes).copied() {
                // `prev_dense` was inserted by `visit_transaction` earlier in
                // this same session, so it lives in `tx_out_ends`. Slot 0 is
                // the pre-session base; slot `local + 1` holds prev's
                // `tx_out_end`, slot `local` holds the previous tx's end
                // (== prev's `out_start`).
                let local = (prev_dense.index() - self.first_session_txid) as usize;
                let start = self.tx_out_ends[local];
                let end = self.tx_out_ends[local + 1];
                let vout = prevout.vout() as u64;
                let out_id = start + vout;
                if out_id >= end { OUTID_NONE } else { out_id }
            } else {
                OUTID_NONE
            }
        };
        if let Err(err) = self.indices.in_prevout.append(out_id) {
            self.error = Some(BlockFileError::Io(err));
            return ControlFlow::Break(());
        }
        if out_id != OUTID_NONE
            && let Err(err) = self.indices.out_spent.set(out_id, in_id)
        {
            self.error = Some(BlockFileError::Io(err));
            return ControlFlow::Break(());
        }
        self.current_in += 1;
        ControlFlow::Continue(())
    }

    fn visit_tx_out(&mut self, _vout: usize, tx_out: &bsl::TxOut<'_>) -> ControlFlow<()> {
        let out_id = *self.tx_out_total + self.current_out;
        if let Err(err) = self.indices.out_spent.append(INID_NONE) {
            self.error = Some(BlockFileError::Io(err));
            return ControlFlow::Break(());
        }
        if out_id != self.indices.out_spent.len() - 1 {
            self.error = Some(BlockFileError::CorruptId());
            return ControlFlow::Break(());
        }
        let spk_hash = script_pubkey_hash(tx_out.script_pubkey());
        if let Err(err) = self.spk_db.insert_if_absent(spk_hash, TxOutId::new(out_id)) {
            self.error = Some(BlockFileError::SpkDb(err));
            return ControlFlow::Break(());
        }
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
        *self.tx_in_total += self.current_in;
        *self.tx_out_total += self.current_out;
        let ptr = TxPtr::new(
            self.block_file.0,
            file_offset as u32,
            tx_len as u32,
            *self.tx_in_total,
            *self.tx_out_total,
        );
        match self.indices.txptr.append(ptr) {
            Ok(txid) => {
                self.txids.insert(tx.txid().to_byte_array(), txid);
                self.tx_out_ends.push(*self.tx_out_total);
            }
            Err(err) => {
                self.error = Some(BlockFileError::Io(err));
                return ControlFlow::Break(());
            }
        }
        self.tx_count += 1;
        self.current_in = 0;
        self.current_out = 0;
        ControlFlow::Continue(())
    }
}

fn tx_io_totals(txptr_index: &ConfirmedTxPtrIndex) -> (u64, u64) {
    let len = txptr_index.len();
    if txptr_index.is_empty() {
        return (0, 0);
    }
    let last = TxId::new((len - 1) as u32);
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
