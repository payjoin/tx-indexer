use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
};

use bitcoin_slices::bitcoin_hashes::Hash;
// TODO: remove this once bitcoin slices updates to use bitcoin 0.32.0
use bitcoin::hashes::Hash as BitcoinHash;
use bitcoin_slices::{Visit, Visitor, bsl};
use core::ops::ControlFlow;

use super::{BlockFileId, TxId, TxInId, TxOutId};

/// Block file layout: 4-byte magic + 4-byte block size (LE) + block payload.
/// Block 0 starts at offset 8.
const BLOCK_START_LEN: usize = 8;

/// Storage for dense IDs backed by Bitcoin Core block files.
///
/// Parses blocks via bitcoin_slices (Visitor pattern), maintains an in-memory
/// block index, and supports lookup of transactions, inputs, and outputs by
/// dense ID (file + byte offset).
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

    fn block_file_path(&self, block_file: BlockFileId) -> PathBuf {
        let file_name = format!("blk{:05}.dat", block_file.0);
        self.blocks_dir.join(file_name)
    }

    /// Parse the first `range.len()` blocks from the first block file (blk00000.dat).
    /// Returns the dense TxIds of all transactions in those blocks and appends
    /// block boundaries to the internal index for later lookups.
    pub fn parse_blocks(
        &mut self,
        range: std::ops::Range<u64>,
    ) -> Result<HashMap<bitcoin::Txid, TxId>, BlockFileError> {
        let file_id = BlockFileId(0);
        let path = self.block_file_path(file_id);
        let bytes = std::fs::read(&path).map_err(BlockFileError::Io)?;

        let mut txids = HashMap::new();
        let mut offset = 0usize;
        let mut blocks_parsed = 0u64;

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
                };
                bsl::Block::visit(block_slice, &mut collector)
                    .map_err(|e| BlockFileError::Parse(e))?;

                self.block_index
                    .push((file_id, block_start_in_file, block_size as u64));
            }

            offset = block_end;
            blocks_parsed += 1;
        }

        Ok(txids)
    }

    /// Find the block that contains the given file offset. Returns (block_file, block_start, block_len).
    fn find_block(
        &self,
        file_id: BlockFileId,
        file_offset: u32,
    ) -> Option<(BlockFileId, u64, u64)> {
        let file_offset = file_offset as u64;
        self.block_index.iter().find_map(|&(id, start, len)| {
            if id == file_id && file_offset >= start && file_offset < start + len {
                Some((id, start, len))
            } else {
                None
            }
        })
    }

    /// Read a block from disk into a buffer.
    fn read_block(
        &self,
        block_file: BlockFileId,
        block_start: u64,
        block_len: u64,
    ) -> Result<Vec<u8>, BlockFileError> {
        let path = self.block_file_path(block_file);
        let mut f = File::open(&path).map_err(BlockFileError::Io)?;
        f.seek(SeekFrom::Start(block_start))
            .map_err(BlockFileError::Io)?;
        let mut buf = vec![0u8; block_len as usize];
        f.read_exact(&mut buf).map_err(BlockFileError::Io)?;
        Ok(buf)
    }

    /// Return the transaction at the given dense TxId as a rust-bitcoin Transaction.
    pub fn get_tx(&self, txid: TxId) -> bitcoin::Transaction {
        let (block_file, block_start, block_len) = self
            .find_block(txid.block_file(), txid.byte_offset())
            .unwrap_or_else(|| {
                panic!(
                    "Corrupted data store: transaction not found for txid: {:?}",
                    txid
                );
            });
        let block_bytes = self
            .read_block(block_file, block_start, block_len)
            .unwrap_or_else(|e| panic!("Corrupted data store: error reading block: {:?}", e));
        let block_slice = &block_bytes[..];
        // Offset of this tx within the block (block_bytes starts at 0).
        let target_offset_in_block = txid.byte_offset() as u64 - block_start;
        let mut finder = FindTxVisitor {
            block_slice,
            target_offset_in_block,
            result: None,
        };
        bsl::Block::visit(block_slice, &mut finder).unwrap_or_else(|e| {
            panic!("Corrupted data store: error parsing block: {:?}", e);
        });
        finder.result.unwrap_or_else(|| {
            panic!(
                "Corrupted data store: transaction not found for txid: {:?}",
                txid
            );
        })
    }

    /// Return the transaction output at the given dense TxOutId as a rust-bitcoin TxOut.
    pub fn get_txout(&self, id: TxOutId) -> bitcoin::TxOut {
        let (block_file, block_start, block_len) = self
            .find_block(id.txid().block_file(), id.byte_offset())
            .unwrap_or_else(|| {
                panic!(
                    "Corrupted data store: transaction output not found for id: {:?}",
                    id
                );
            });
        let block_bytes = self
            .read_block(block_file, block_start, block_len)
            .unwrap_or_else(|e| panic!("Corrupted data store: error reading block: {:?}", e));
        let block_slice = &block_bytes[..];
        let target_offset_in_block = id.byte_offset() as u64 - block_start;
        let mut finder = FindTxOutVisitor {
            block_slice,
            target_offset_in_block,
            result: None,
        };
        bsl::Block::visit(block_slice, &mut finder).unwrap_or_else(|e| {
            panic!("Corrupted data store: error parsing block: {:?}", e);
        });
        finder.result.unwrap_or_else(|| {
            panic!(
                "Corrupted data store: transaction output not found for id: {:?}",
                id
            );
        })
    }

    /// Return the transaction input at the given dense TxInId as a rust-bitcoin TxIn.
    pub fn get_txin(&self, id: TxInId) -> bitcoin::TxIn {
        let (block_file, block_start, block_len) = self
            .find_block(id.txid().block_file(), id.byte_offset())
            .unwrap_or_else(|| {
                panic!(
                    "Corrupted data store: transaction input not found for id: {:?}",
                    id
                );
            });
        let block_bytes = self
            .read_block(block_file, block_start, block_len)
            .unwrap_or_else(|e| panic!("Corrupted data store: error reading block: {:?}", e));
        let block_slice = &block_bytes[..];
        let target_offset_in_block = id.byte_offset() as u64 - block_start;
        let mut finder = FindTxInVisitor {
            block_slice,
            target_offset_in_block,
            result: None,
        };
        bsl::Block::visit(block_slice, &mut finder).unwrap_or_else(|e| {
            panic!("Corrupted data store: error parsing block: {:?}", e);
        });
        finder.result.unwrap_or_else(|| {
            panic!(
                "Corrupted data store: transaction input not found for id: {:?}",
                id
            );
        })
    }

    /// Return all dense TxInIds for the transaction at the given dense TxId.
    pub fn get_txin_ids(&self, txid: TxId) -> Vec<TxInId> {
        let (block_file, block_start, block_len) = self
            .find_block(txid.block_file(), txid.byte_offset())
            .unwrap_or_else(|| {
                panic!(
                    "Corrupted data store: transaction not found for txid: {:?}",
                    txid
                );
            });
        let block_bytes = self
            .read_block(block_file, block_start, block_len)
            .unwrap_or_else(|e| panic!("Corrupted data store: error reading block: {:?}", e));
        let block_slice = &block_bytes[..];
        let target_offset_in_block = txid.byte_offset() as u64 - block_start;
        let mut collector = CollectTxInIdsVisitor {
            block_slice,
            block_start,
            target_offset_in_block,
            txid,
            txin_ids: Vec::new(),
        };
        bsl::Block::visit(block_slice, &mut collector).unwrap_or_else(|e| {
            panic!("Corrupted data store: error parsing block: {:?}", e);
        });
        collector.txin_ids
    }

    /// Return all dense TxOutIds for the transaction at the given dense TxId.
    pub fn get_txout_ids(&self, txid: TxId) -> Vec<TxOutId> {
        let (block_file, block_start, block_len) = self
            .find_block(txid.block_file(), txid.byte_offset())
            .unwrap_or_else(|| {
                panic!(
                    "Corrupted data store: transaction not found for txid: {:?}",
                    txid
                );
            });
        let block_bytes = self
            .read_block(block_file, block_start, block_len)
            .unwrap_or_else(|e| panic!("Corrupted data store: error reading block: {:?}", e));
        let block_slice = &block_bytes[..];
        let target_offset_in_block = txid.byte_offset() as u64 - block_start;
        let mut collector = CollectTxOutIdsVisitor {
            block_slice,
            block_start,
            target_offset_in_block,
            txid,
            txout_ids: Vec::new(),
        };
        bsl::Block::visit(block_slice, &mut collector).unwrap_or_else(|e| {
            panic!("Corrupted data store: error parsing block: {:?}", e);
        });
        collector.txout_ids
    }
}

/// Visitor that collects TxIds (file + byte offset) for each transaction in a block.
struct TxIdCollector<'a> {
    block_file: BlockFileId,
    block_start_in_file: u64,
    block_slice: &'a [u8],
    txids: &'a mut HashMap<bitcoin::Txid, TxId>,
}

impl Visitor for TxIdCollector<'_> {
    fn visit_transaction(&mut self, tx: &bsl::Transaction<'_>) -> ControlFlow<()> {
        let tx_slice = tx.as_ref();
        let offset_in_block = tx_slice.as_ptr() as usize - self.block_slice.as_ptr() as usize;
        let file_offset = self.block_start_in_file + offset_in_block as u64;
        let txid = TxId::new(self.block_file, file_offset as u32);
        self.txids.insert(
            bitcoin::Txid::from_slice(&tx.txid().to_byte_array()).unwrap(),
            txid,
        );
        ControlFlow::Continue(())
    }
}

/// Visitor that finds the single transaction at target_offset_in_block and parses it to bitcoin::Transaction.
struct FindTxVisitor<'a> {
    block_slice: &'a [u8],
    target_offset_in_block: u64,
    result: Option<bitcoin::Transaction>,
}

impl Visitor for FindTxVisitor<'_> {
    fn visit_transaction(&mut self, tx: &bsl::Transaction<'_>) -> ControlFlow<()> {
        let tx_slice = tx.as_ref();
        let offset_in_block = tx_slice.as_ptr() as usize - self.block_slice.as_ptr() as usize;
        if offset_in_block as u64 == self.target_offset_in_block {
            if let Ok(tx) = bitcoin::consensus::deserialize::<bitcoin::Transaction>(tx_slice) {
                self.result = Some(tx);
            }
        }
        ControlFlow::Continue(())
    }
}

/// Visitor that finds the TxOut at target_offset_in_block and parses it to bitcoin::TxOut.
struct FindTxOutVisitor<'a> {
    block_slice: &'a [u8],
    target_offset_in_block: u64,
    result: Option<bitcoin::TxOut>,
}

impl Visitor for FindTxOutVisitor<'_> {
    fn visit_tx_out(&mut self, _vout: usize, tx_out: &bsl::TxOut<'_>) -> ControlFlow<()> {
        let slice = tx_out.as_ref();
        let offset_in_block = slice.as_ptr() as usize - self.block_slice.as_ptr() as usize;
        if offset_in_block as u64 == self.target_offset_in_block {
            if let Ok(txout) = bitcoin::consensus::deserialize::<bitcoin::TxOut>(slice) {
                self.result = Some(txout);
            }
        }
        ControlFlow::Continue(())
    }
}

/// Visitor that finds the TxIn at target_offset_in_block and parses it to bitcoin::TxIn.
struct FindTxInVisitor<'a> {
    block_slice: &'a [u8],
    target_offset_in_block: u64,
    result: Option<bitcoin::TxIn>,
}

impl Visitor for FindTxInVisitor<'_> {
    fn visit_tx_in(&mut self, _vin: usize, tx_in: &bsl::TxIn<'_>) -> ControlFlow<()> {
        let slice = tx_in.as_ref();
        let offset_in_block = slice.as_ptr() as usize - self.block_slice.as_ptr() as usize;
        if offset_in_block as u64 == self.target_offset_in_block {
            if let Ok(txin) = bitcoin::consensus::deserialize::<bitcoin::TxIn>(slice) {
                self.result = Some(txin);
            }
        }
        ControlFlow::Continue(())
    }
}

/// Visitor that collects TxInIds for the transaction at target_offset_in_block.
/// bitcoin_slices calls visit_tx_in before visit_transaction for each tx, so we
/// do a nested visit when we match the target in visit_transaction.
struct CollectTxInIdsVisitor<'a> {
    block_slice: &'a [u8],
    block_start: u64,
    target_offset_in_block: u64,
    txid: TxId,
    txin_ids: Vec<TxInId>,
}

/// Inner visitor used to collect txin ids from a single tx (called when we've matched the target).
struct CollectTxInIdsInnerVisitor<'a> {
    block_slice: &'a [u8],
    block_start: u64,
    txid: TxId,
    txin_ids: &'a mut Vec<TxInId>,
}

impl Visitor for CollectTxInIdsInnerVisitor<'_> {
    fn visit_tx_in(&mut self, _vin: usize, tx_in: &bsl::TxIn<'_>) -> ControlFlow<()> {
        let slice = tx_in.as_ref();
        let offset_in_block = slice.as_ptr() as usize - self.block_slice.as_ptr() as usize;
        let file_offset = self.block_start + offset_in_block as u64;
        self.txin_ids.push(self.txid.txin_id(file_offset as u32));
        ControlFlow::Continue(())
    }
}

impl Visitor for CollectTxInIdsVisitor<'_> {
    fn visit_transaction(&mut self, tx: &bsl::Transaction<'_>) -> ControlFlow<()> {
        let tx_slice = tx.as_ref();
        let offset_in_block = tx_slice.as_ptr() as usize - self.block_slice.as_ptr() as usize;
        if offset_in_block as u64 == self.target_offset_in_block {
            let mut inner = CollectTxInIdsInnerVisitor {
                block_slice: self.block_slice,
                block_start: self.block_start,
                txid: self.txid,
                txin_ids: &mut self.txin_ids,
            };
            let _ = bsl::Transaction::visit(tx_slice, &mut inner);
        }
        ControlFlow::Continue(())
    }
}

/// Visitor that collects TxOutIds for the transaction at target_offset_in_block.
struct CollectTxOutIdsVisitor<'a> {
    block_slice: &'a [u8],
    block_start: u64,
    target_offset_in_block: u64,
    txid: TxId,
    txout_ids: Vec<TxOutId>,
}

/// Inner visitor used to collect txout ids from a single tx.
struct CollectTxOutIdsInnerVisitor<'a> {
    block_slice: &'a [u8],
    block_start: u64,
    txid: TxId,
    txout_ids: &'a mut Vec<TxOutId>,
}

impl Visitor for CollectTxOutIdsInnerVisitor<'_> {
    fn visit_tx_out(&mut self, _vout: usize, tx_out: &bsl::TxOut<'_>) -> ControlFlow<()> {
        let slice = tx_out.as_ref();
        let offset_in_block = slice.as_ptr() as usize - self.block_slice.as_ptr() as usize;
        let file_offset = self.block_start + offset_in_block as u64;
        self.txout_ids.push(self.txid.txout_id(file_offset as u32));
        ControlFlow::Continue(())
    }
}

impl Visitor for CollectTxOutIdsVisitor<'_> {
    fn visit_transaction(&mut self, tx: &bsl::Transaction<'_>) -> ControlFlow<()> {
        let tx_slice = tx.as_ref();
        let offset_in_block = tx_slice.as_ptr() as usize - self.block_slice.as_ptr() as usize;
        if offset_in_block as u64 == self.target_offset_in_block {
            let mut inner = CollectTxOutIdsInnerVisitor {
                block_slice: self.block_slice,
                block_start: self.block_start,
                txid: self.txid,
                txout_ids: &mut self.txout_ids,
            };
            let _ = bsl::Transaction::visit(tx_slice, &mut inner);
        }
        ControlFlow::Continue(())
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

    use crate::integration::{HarnessOut, run_harness};

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
            |_harness, parser, out, dense_txids| {
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
                let tx = parser.get_tx(*dense_id);
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
            |harness, parser, out, dense_txids| {
                for want in &out.expected_txids {
                    let dense_id = dense_txids
                        .get(want)
                        .ok_or_else(|| anyhow::anyhow!("no dense TxId for {}", want))?;
                    let tx = parser.get_tx(*dense_id);
                    assert_eq!(tx.compute_txid(), *want);
                    let rpc_tx = harness.get_raw_transaction(*want)?;
                    assert_eq!(tx.compute_txid(), rpc_tx.compute_txid());
                    assert_eq!(tx.input.len(), rpc_tx.input.len());
                    assert_eq!(tx.output.len(), rpc_tx.output.len());
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
            |harness, parser, out, dense_txids| {
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

                let tx = parser.get_tx(*dense_id);
                let txin_ids = parser.get_txin_ids(*dense_id);
                let txout_ids = parser.get_txout_ids(*dense_id);

                assert_eq!(txin_ids.len(), tx.input.len());
                assert_eq!(txout_ids.len(), tx.output.len());

                for (i, id) in txin_ids.iter().enumerate() {
                    let txin = parser.get_txin(*id);
                    assert_eq!(txin.previous_output, tx.input[i].previous_output);
                }
                for (i, id) in txout_ids.iter().enumerate() {
                    let txout = parser.get_txout(*id);
                    assert_eq!(txout.value, tx.output[i].value);
                    assert_eq!(txout.script_pubkey, tx.output[i].script_pubkey);
                }

                Ok(())
            },
        )
    }
}
