use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use bitcoin_slices::bitcoin_hashes::Hash;
use bitcoin_slices::{Visit, Visitor, bsl};

use bitcoin::hashes::Hash as _;
use core::ops::ControlFlow;

use super::{BlockFileId, TxId};
use crate::confirmed::{
    BlockTxIndex, ConfirmedTxPtrIndex, INID_NONE, InPrevoutIndex, OUTID_NONE, OutSpentByIndex,
    TxPtr,
};

/// Block file layout: 4-byte magic + 4-byte block size (LE) + block payload.
/// Block 0 starts at offset 8.
const BLOCK_START_LEN: usize = 8;

// TODO: provide option to memory map the block files

/// Storage for dense IDs backed by Bitcoin Core block files.
///
/// Parses blocks via bitcoin_slices (Visitor pattern), maintains an in-memory
/// block index.
#[derive(Debug)]
pub struct Parser {
    blocks_dir: PathBuf,
    // TODO: this can be replaced with the kernel
    /// For each parsed block: (BlockFileId, block_start, block_len).
    /// Used to find which block contains a given byte offset.
    block_index: Vec<(BlockFileId, u64, u64)>,
}

impl Parser {
    pub fn new(blocks_dir: impl Into<PathBuf>) -> Self {
        Self {
            blocks_dir: blocks_dir.into(),
            block_index: Vec::new(),
        }
    }

    pub fn blocks_dir(&self) -> &Path {
        &self.blocks_dir
    }

    pub fn block_index(&self) -> &[(BlockFileId, u64, u64)] {
        &self.block_index
    }

    fn block_file_path(&self, block_file: BlockFileId) -> PathBuf {
        let file_name = format!("blk{:05}.dat", block_file.0);
        self.blocks_dir.join(file_name)
    }

    /// Parse the first `range.len()` blocks from the first block file (blk00000.dat).
    /// Returns the dense TxIds of all transactions in those blocks, appends
    /// block boundaries to the internal index, and writes tx pointers to the
    /// confirmed tx pointer index.
    pub fn parse_blocks(
        &mut self,
        range: std::ops::Range<u64>,
        txptr_index: &mut ConfirmedTxPtrIndex,
        block_tx_index: &mut BlockTxIndex,
        in_prevout_index: &mut InPrevoutIndex,
        out_spent_index: &mut OutSpentByIndex,
    ) -> Result<HashMap<bitcoin::Txid, TxId>, BlockFileError> {
        let file_id = BlockFileId(0);
        let path = self.block_file_path(file_id);
        let bytes = std::fs::read(&path).map_err(BlockFileError::Io)?;

        let mut txids = HashMap::new();
        let mut offset = 0usize;
        let mut blocks_parsed = 0u64;
        let (mut tx_in_total, mut tx_out_total) = tx_io_totals(txptr_index);
        let mut tx_total = block_tx_index
            .last()
            .map_err(BlockFileError::Io)?
            .unwrap_or(0) as u64;

        while blocks_parsed < range.end && offset + BLOCK_START_LEN <= bytes.len() {
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

            if blocks_parsed >= range.start {
                let block_slice = &bytes[block_start..block_end];
                let block_start_in_file = block_start as u64;
                let mut collector = TxIdCollector {
                    block_file: file_id,
                    block_start_in_file,
                    block_slice,
                    txids: &mut txids,
                    txptr_index,
                    error: None,
                    tx_in_total: &mut tx_in_total,
                    tx_out_total: &mut tx_out_total,
                    tx_count: 0,
                    current_in: 0,
                    current_out: 0,
                    in_prevout_index,
                    out_spent_index,
                };
                bsl::Block::visit(block_slice, &mut collector)
                    .map_err(|e| BlockFileError::Parse(e))?;
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

                self.block_index
                    .push((file_id, block_start_in_file, block_size as u64));
            }

            offset = block_end;
            blocks_parsed += 1;
        }

        Ok(txids)
    }
}

/// Visitor that collects TxIds (file + byte offset) for each transaction in a block.
struct TxIdCollector<'a> {
    block_file: BlockFileId,
    block_start_in_file: u64,
    block_slice: &'a [u8],
    txids: &'a mut HashMap<bitcoin::Txid, TxId>,
    txptr_index: &'a mut ConfirmedTxPtrIndex,
    error: Option<BlockFileError>,
    tx_in_total: &'a mut u64,
    tx_out_total: &'a mut u64,
    tx_count: u64,
    current_in: u64,
    current_out: u64,
    in_prevout_index: &'a mut InPrevoutIndex,
    out_spent_index: &'a mut OutSpentByIndex,
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
            let prev_txid = bitcoin::Txid::from_byte_array(bytes);
            if let Some(prev_dense) = self.txids.get(&prev_txid).copied() {
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

    fn visit_tx_out(&mut self, _vout: usize, _tx_out: &bsl::TxOut<'_>) -> ControlFlow<()> {
        let out_id = *self.tx_out_total + self.current_out;
        if let Err(err) = self.out_spent_index.append(INID_NONE) {
            self.error = Some(BlockFileError::Io(err));
            return ControlFlow::Break(());
        }
        if out_id != self.out_spent_index.len() - 1 {
            self.error = Some(BlockFileError::CorruptId());
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
        let offset_in_block = tx_slice.as_ptr() as usize - self.block_slice.as_ptr() as usize;
        let file_offset = self.block_start_in_file + offset_in_block as u64;
        *self.tx_in_total += self.current_in;
        *self.tx_out_total += self.current_out;
        let ptr = TxPtr::new(
            self.block_file.0,
            file_offset as u32,
            *self.tx_in_total,
            *self.tx_out_total,
        );
        match self.txptr_index.append(ptr) {
            Ok(txid) => {
                self.txids.insert(
                    bitcoin::Txid::from_slice(&tx.txid().to_byte_array()).unwrap(),
                    txid,
                );
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

#[derive(Debug)]
pub enum BlockFileError {
    Io(std::io::Error),
    UnexpectedEof { offset: usize, len: usize },
    Parse(bitcoin_slices::Error),
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
            | BlockFileError::CorruptId() => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use bitcoin::Amount;

    use crate::{
        dense::TxOutId,
        integration::{HarnessOut, run_harness},
    };

    #[test]
    fn integration_mine_empty_block() -> Result<()> {
        run_harness(
            |harness| {
                let address = harness.client().new_address()?;
                harness.generate_blocks(1, &address)?;
                let count = harness.get_block_count()?;
                let best = harness.best_block_hash()?;
                let block = harness.get_block(best)?;
                let coinbase_txid = block.txdata[0].compute_txid();
                Ok(HarnessOut {
                    expected_txids: vec![coinbase_txid],
                    block_count_after: count,
                })
            },
            |_harness, storage, out, dense_txids| {
                assert_eq!(out.expected_txids.len(), 1, "one coinbase tx");
                let want = out.expected_txids[0];
                // We parsed block_count_after blocks; last block is the one we just mined
                assert!(
                    !dense_txids.is_empty(),
                    "expected at least one dense TxId (got {})",
                    dense_txids.len()
                );
                let dense_id = dense_txids
                    .get(&want)
                    .ok_or_else(|| anyhow::anyhow!("no dense TxId for coinbase {}", want))?;
                let tx = storage.get_tx(*dense_id);
                assert_eq!(tx.compute_txid(), want);
                // Coinbase has one input (null prevout) and at least one output
                assert_eq!(tx.input.len(), 1);
                assert!(tx.input[0].previous_output.is_null());
                assert!(!tx.output.is_empty());
                Ok(())
            },
        )
    }

    #[test]
    fn integration_mine_block_with_transactions() -> Result<()> {
        run_harness(
            |harness| {
                let addr1 = harness.client().new_address()?;
                let addr2 = harness.client().new_address()?;
                let amount = Amount::from_sat(50_000);
                let txid1 = harness.send_to_address(&addr1, amount)?;
                let txid2 = harness.send_to_address(&addr2, amount)?;
                harness.generate_blocks(1, &addr1)?;
                let count = harness.get_block_count()?;
                let best = harness.best_block_hash()?;
                let block = harness.get_block(best)?;
                let mut expected = vec![block.txdata[0].compute_txid()];
                for tx in &block.txdata[1..] {
                    expected.push(tx.compute_txid());
                }
                assert!(
                    expected.contains(&txid1) && expected.contains(&txid2),
                    "block should contain both sent txs"
                );
                Ok(HarnessOut {
                    expected_txids: expected,
                    block_count_after: count,
                })
            },
            |harness, storage, out, dense_txids| {
                for want in &out.expected_txids {
                    let dense_id = dense_txids
                        .get(want)
                        .ok_or_else(|| anyhow::anyhow!("no dense TxId for {}", want))?;
                    let tx = storage.get_tx(*dense_id);
                    assert_eq!(tx.compute_txid(), *want);
                    let rpc_tx = harness.get_raw_transaction(*want)?;
                    assert_eq!(tx.compute_txid(), rpc_tx.compute_txid());
                    assert_eq!(tx.input.len(), rpc_tx.input.len());
                    assert_eq!(tx.output.len(), rpc_tx.output.len());

                    let block = storage.block_of_tx(*dense_id);
                    assert_eq!(block, out.block_count_after);
                    let (start, end) = storage.tx_range_for_block(block);
                    assert!(dense_id.index() >= start);
                    assert!(dense_id.index() < end);
                }
                Ok(())
            },
        )
    }

    #[test]
    fn integration_dense_ids_roundtrip() -> Result<()> {
        run_harness(
            |harness| {
                let addr = harness.client().new_address()?;
                let _txid = harness.send_to_address(&addr, Amount::from_sat(30_000))?;
                harness.generate_blocks(1, &addr)?;
                let count = harness.get_block_count()?;
                let best = harness.best_block_hash()?;
                let block = harness.get_block(best)?;
                let mut expected = vec![block.txdata[0].compute_txid()];
                for tx in &block.txdata[1..] {
                    expected.push(tx.compute_txid());
                }
                Ok(HarnessOut {
                    expected_txids: expected,
                    block_count_after: count,
                })
            },
            |harness, storage, out, dense_txids| {
                // Pick a non-coinbase tx to test input/output ID round-trips
                let want = out
                    .expected_txids
                    .iter()
                    .find(|id| {
                        let tx = harness.get_raw_transaction(**id).unwrap();
                        !tx.input[0].previous_output.is_null()
                    })
                    .copied()
                    .ok_or_else(|| anyhow::anyhow!("no non-coinbase tx in block"))?;

                let dense_id = dense_txids
                    .get(&want)
                    .ok_or_else(|| anyhow::anyhow!("no dense TxId for {}", want))?;

                let tx = storage.get_tx(*dense_id);
                let txin_ids = storage.get_txin_ids(*dense_id);
                let txout_ids = storage.get_txout_ids(*dense_id);
                let (in_start, in_end) = storage.tx_in_range(*dense_id);
                let (out_start, out_end) = storage.tx_out_range(*dense_id);

                assert_eq!(txin_ids.len(), tx.input.len());
                assert_eq!(txout_ids.len(), tx.output.len());
                assert_eq!(in_end - in_start, txin_ids.len() as u64);
                assert_eq!(out_end - out_start, txout_ids.len() as u64);

                for (i, id) in txin_ids.iter().enumerate() {
                    assert_eq!(id.index(), in_start + i as u64);
                }
                for (i, id) in txout_ids.iter().enumerate() {
                    assert_eq!(id.index(), out_start + i as u64);
                }

                for (i, id) in txin_ids.iter().enumerate() {
                    let txin = storage.get_txin(*id);
                    assert_eq!(txin.previous_output, tx.input[i].previous_output);
                }
                for (i, id) in txout_ids.iter().enumerate() {
                    let txout = storage.get_txout(*id);
                    assert_eq!(txout.value, tx.output[i].value);
                    assert_eq!(txout.script_pubkey, tx.output[i].script_pubkey);
                }

                Ok(())
            },
        )
    }

    #[test]
    fn integration_prevout_spender_indexes() -> Result<()> {
        run_harness(
            |harness| {
                let addr = harness.client().new_address()?;
                let coinbase_addr = harness.client().new_address()?;
                harness.generate_blocks(1, &coinbase_addr)?;
                let _txid = harness.send_to_address(&addr, Amount::from_sat(30_000))?;
                harness.generate_blocks(1, &addr)?;
                let count = harness.get_block_count()?;
                let best = harness.best_block_hash()?;
                let block = harness.get_block(best)?;
                let mut expected = vec![block.txdata[0].compute_txid()];
                for tx in &block.txdata[1..] {
                    expected.push(tx.compute_txid());
                }
                Ok(HarnessOut {
                    expected_txids: expected,
                    block_count_after: count,
                })
            },
            |harness, storage, out, dense_txids| {
                let want = out
                    .expected_txids
                    .iter()
                    .find(|id| {
                        let tx = harness.get_raw_transaction(**id).unwrap();
                        !tx.input[0].previous_output.is_null()
                    })
                    .copied()
                    .ok_or_else(|| anyhow::anyhow!("no non-coinbase tx in block"))?;

                let dense_id = dense_txids
                    .get(&want)
                    .ok_or_else(|| anyhow::anyhow!("no dense TxId for {}", want))?;
                let tx = storage.get_tx(*dense_id);
                let txin_ids = storage.get_txin_ids(*dense_id);

                assert_eq!(txin_ids.len(), tx.input.len());

                for (i, in_id) in txin_ids.iter().enumerate() {
                    let txin = storage.get_txin(*in_id);
                    assert_eq!(txin.previous_output, tx.input[i].previous_output);
                    if txin.previous_output.is_null() {
                        assert_eq!(storage.prevout_for_in(*in_id), None);
                    } else {
                        let prev_txid = txin.previous_output.txid;
                        let prev_dense = dense_txids
                            .get(&prev_txid)
                            .ok_or_else(|| anyhow::anyhow!("no dense TxId for {}", prev_txid))?;
                        let (start, _end) = storage.tx_out_range(*prev_dense);
                        let out_id = TxOutId::new(start + txin.previous_output.vout as u64);
                        assert_eq!(storage.prevout_for_in(*in_id), Some(out_id));
                        assert_eq!(storage.spender_for_out(out_id), Some(*in_id));
                    }
                }

                Ok(())
            },
        )
    }
}
