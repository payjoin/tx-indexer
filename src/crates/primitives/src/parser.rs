use std::collections::HashMap;

use bitcoin_slices::bitcoin_hashes::Hash;
use bitcoin_slices::{Visit, Visitor, bsl};

use bitcoin::hashes::hash160::Hash as Hash160;
use bitcoin_block_index::BlockIndex;
use core::ops::ControlFlow;

use crate::{
    ScriptPubkeyHash,
    dense::{BitcoindDataDirectory, BlockFileId, TxId, TxOutId},
    indecies::{
        BlockTxIndex, ConfirmedTxPtrIndex, INID_NONE, InPrevoutIndex, OUTID_NONE, OutSpentByIndex,
        TxPtr,
    },
    sled::spk_db::{SledScriptPubkeyDb, SledScriptPubkeyDbError},
    traits::ScriptPubkeyDb,
};

/// Block file layout: 4-byte magic + 4-byte block size (LE) + block payload.
const BLOCK_HEADER_LEN: usize = 8;

/// Known network magic bytes.
const MAGICS: [[u8; 4]; 4] = [
    [0xf9, 0xbe, 0xb4, 0xd9], // mainnet
    [0x0b, 0x11, 0x09, 0x07], // testnet3
    [0xfa, 0xbf, 0xb5, 0xda], // regtest
    [0x0a, 0x03, 0xcf, 0x40], // signet
];

// TODO: provide option to memory map the block files

/// Storage for dense IDs backed by Bitcoin Core block files.
///
/// Parses blocks via bitcoin_slices (Visitor pattern).
#[derive(Debug)]
pub struct Parser {
    data_dir: BitcoindDataDirectory,
}

impl Parser {
    pub fn new(data_dir: BitcoindDataDirectory) -> Self {
        Self { data_dir }
    }

    /// Parse the first `range.len()` blocks from the first block file (blk00000.dat).
    /// Returns the dense TxIds of all transactions in those blocks and writes
    /// tx pointers to the confirmed tx pointer index.
    pub fn parse_blocks(
        &mut self,
        range: std::ops::Range<u64>,
        txptr_index: &mut ConfirmedTxPtrIndex,
        block_tx_index: &mut BlockTxIndex,
        in_prevout_index: &mut InPrevoutIndex,
        out_spent_index: &mut OutSpentByIndex,
        spk_db: &mut SledScriptPubkeyDb,
    ) -> Result<(), BlockFileError> {
        self.parse_files(
            &[0],
            Some(range),
            txptr_index,
            block_tx_index,
            in_prevout_index,
            out_spent_index,
            spk_db,
        )
    }

    /// Parse blocks from the most recent blk files covering the last `depth` heights.
    ///
    /// Uses the block index to walk backwards from the latest file until we've
    /// covered enough block height range, then processes those files in forward order.
    pub fn parse_from_tip(
        &mut self,
        block_index: &mut BlockIndex,
        depth: u32,
        txptr_index: &mut ConfirmedTxPtrIndex,
        block_tx_index: &mut BlockTxIndex,
        in_prevout_index: &mut InPrevoutIndex,
        out_spent_index: &mut OutSpentByIndex,
        spk_db: &mut SledScriptPubkeyDb,
    ) -> Result<(), BlockFileError> {
        let last_file = block_index
            .last_block_file()
            .map_err(BlockFileError::BlockIndex)?;
        let last_info = block_index
            .block_file_info(last_file)
            .map_err(BlockFileError::BlockIndex)?;

        let tip_height = last_info.height_last;
        let target_height = tip_height.saturating_sub(depth);

        // Walk backwards through files to find the first file we need.
        let mut first_file = last_file;
        for file_no in (0..last_file).rev() {
            match block_index.block_file_info(file_no) {
                Ok(info) if info.height_last >= target_height => {
                    first_file = file_no;
                }
                _ => break,
            }
        }

        let files: Vec<u32> = (first_file..=last_file).collect();
        println!("Parsing files: {:?}", files);
        self.parse_files(
            &files,
            None,
            txptr_index,
            block_tx_index,
            in_prevout_index,
            out_spent_index,
            spk_db,
        )
    }

    /// Parse all blocks from the given blk file numbers (in order).
    ///
    /// If `block_range` is Some, only process that count-range of blocks
    /// from the first file (legacy behaviour). If None, process all blocks
    /// in every listed file.
    pub fn parse_files(
        &mut self,
        file_numbers: &[u32],
        block_range: Option<std::ops::Range<u64>>,
        txptr_index: &mut ConfirmedTxPtrIndex,
        block_tx_index: &mut BlockTxIndex,
        in_prevout_index: &mut InPrevoutIndex,
        out_spent_index: &mut OutSpentByIndex,
        spk_db: &mut SledScriptPubkeyDb,
    ) -> Result<(), BlockFileError> {
        let (mut tx_in_total, mut tx_out_total) = tx_io_totals(txptr_index);
        let mut tx_total = block_tx_index
            .last()
            .map_err(BlockFileError::Io)?
            .unwrap_or(0) as u64;
        let mut txids = HashMap::new();
        let mut global_blocks_parsed = 0u64;

        for &file_no in file_numbers {
            let file_id = BlockFileId(file_no);
            let path = self.data_dir.block_file_path(file_id);
            let bytes = std::fs::read(&path).map_err(BlockFileError::Io)?;

            let mut offset = 0usize;

            while offset + BLOCK_HEADER_LEN <= bytes.len() {
                // Skip zero padding between blocks.
                if bytes[offset] == 0 {
                    offset += 1;
                    continue;
                }

                // Validate magic bytes.
                let magic: [u8; 4] = bytes[offset..offset + 4].try_into().map_err(|_| {
                    BlockFileError::UnexpectedEof {
                        offset: offset + 4,
                        len: bytes.len(),
                    }
                })?;
                if !MAGICS.contains(&magic) {
                    // Unknown data — stop parsing this file.
                    break;
                }

                let block_size =
                    u32::from_le_bytes(bytes[offset + 4..offset + 8].try_into().map_err(|_| {
                        BlockFileError::UnexpectedEof {
                            offset: offset + 8,
                            len: bytes.len(),
                        }
                    })?) as usize;
                if block_size == 0 {
                    break;
                }
                let block_start = offset + BLOCK_HEADER_LEN;
                let block_end = block_start + block_size;
                if block_end > bytes.len() {
                    // Truncated block at end of file — stop gracefully.
                    break;
                }

                // Apply block_range filter if present.
                let in_range = match &block_range {
                    Some(range) => {
                        global_blocks_parsed >= range.start && global_blocks_parsed < range.end
                    }
                    None => true,
                };

                if in_range {
                    let block_slice = &bytes[block_start..block_end];
                    let block_start_in_file = block_start as u64;
                    let mut collector = TxIdCollector {
                        block_file: file_id,
                        block_start_in_file,
                        block_slice,
                        txptr_index,
                        error: None,
                        tx_in_total: &mut tx_in_total,
                        tx_out_total: &mut tx_out_total,
                        tx_count: 0,
                        current_in: 0,
                        current_out: 0,
                        in_prevout_index,
                        out_spent_index,
                        spk_db,
                        txids: &mut txids,
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
                    block_tx_index
                        .append(tx_total as u32)
                        .map_err(BlockFileError::Io)?;
                }

                offset = block_end;
                global_blocks_parsed += 1;

                // Early exit if we've passed the range end.
                if let Some(range) = &block_range {
                    if global_blocks_parsed >= range.end {
                        return Ok(());
                    }
                }
            }
        }

        Ok(())
    }
}

/// Visitor that collects TxIds (file + byte offset) for each transaction in a block.
struct TxIdCollector<'a> {
    block_file: BlockFileId,
    block_start_in_file: u64,
    block_slice: &'a [u8],
    txids: &'a mut HashMap<[u8; 32], TxId>,
    txptr_index: &'a mut ConfirmedTxPtrIndex,
    error: Option<BlockFileError>,
    tx_in_total: &'a mut u64,
    tx_out_total: &'a mut u64,
    tx_count: u64,
    current_in: u64,
    current_out: u64,
    in_prevout_index: &'a mut InPrevoutIndex,
    out_spent_index: &'a mut OutSpentByIndex,
    spk_db: &'a mut SledScriptPubkeyDb,
}

impl Visitor for TxIdCollector<'_> {
    fn visit_tx_in(&mut self, _vin: usize, tx_in: &bsl::TxIn<'_>) -> ControlFlow<()> {
        let in_id = *self.tx_in_total + self.current_in;
        let prevout = tx_in.prevout();
        let out_id = if is_null_prevout(prevout) {
            OUTID_NONE
        } else {
            let mut bytes = [0u8; 32];
            bytes.copy_from_slice(prevout.txid());
            if let Some(prev_dense) = self.txids.get(&bytes).copied() {
                let (start, end) = tx_out_range_for(prev_dense, self.txptr_index);
                let vout = prevout.vout() as u64;
                let out_id = start + vout;
                if out_id >= end { OUTID_NONE } else { out_id }
            } else {
                OUTID_NONE
            }
        };
        if let Err(err) = self.in_prevout_index.append(out_id) {
            self.error = Some(BlockFileError::Io(err));
            return ControlFlow::Break(());
        }
        if out_id != OUTID_NONE {
            if let Err(err) = self.out_spent_index.set(out_id, in_id) {
                self.error = Some(BlockFileError::Io(err));
                return ControlFlow::Break(());
            }
        }
        self.current_in += 1;
        ControlFlow::Continue(())
    }

    fn visit_tx_out(&mut self, _vout: usize, tx_out: &bsl::TxOut<'_>) -> ControlFlow<()> {
        let out_id = *self.tx_out_total + self.current_out;
        if let Err(err) = self.out_spent_index.append(INID_NONE) {
            self.error = Some(BlockFileError::Io(err));
            return ControlFlow::Break(());
        }
        if out_id != self.out_spent_index.len() - 1 {
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
        match self.txptr_index.append(ptr) {
            Ok(txid) => {
                self.txids.insert(tx.txid().to_byte_array(), txid);
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
    if len == 0 {
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
    BlockIndex(bitcoin_block_index::Error),
    CorruptId(),
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
            BlockFileError::BlockIndex(e) => write!(f, "block index: {}", e),
            BlockFileError::CorruptId() => write!(f, "corrupt id"),
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
            | BlockFileError::BlockIndex(_)
            | BlockFileError::CorruptId() => None,
        }
    }
}
