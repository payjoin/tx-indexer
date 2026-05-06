use std::path::Path;

use bitcoin_slices::bitcoin_hashes::Hash;
use bitcoin_slices::{Visit, Visitor, bsl};

use core::ops::ControlFlow;

use crate::{
    blk_file::BlkFileStore, dense::BlockFileId, sled::spk_db::SledScriptPubkeyDbError,
    traits::IndexSink,
};

/// Block file layout: 4-byte magic + 4-byte block size (LE) + block payload.
const BLOCK_START_LEN: usize = 8;

/// Parser-side blk file metadata used to locate and bound parsing within a file.
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

/// Parses Bitcoin Core block files and feeds events to an [`IndexSink`].
pub struct Parser {
    store: BlkFileStore,
    file_hints: Vec<BlkFileHint>,
}

impl Parser {
    pub fn new(blocks_dir: impl Into<std::path::PathBuf>) -> Self {
        Self {
            store: BlkFileStore::open(blocks_dir),
            file_hints: Vec::new(),
        }
    }

    pub fn with_file_hints(mut self, hints: Vec<BlkFileHint>) -> Self {
        self.file_hints = hints;
        self
    }

    pub fn blocks_dir(&self) -> &Path {
        self.store.blocks_dir()
    }

    pub fn into_blk_store(self) -> BlkFileStore {
        self.store
    }

    /// Parse blocks in `range` (by global block height) and emit events to `sink`.
    pub fn parse_blocks<S>(
        &mut self,
        range: std::ops::Range<u64>,
        sink: &mut S,
    ) -> Result<(), BlockFileError>
    where
        S: IndexSink,
        BlockFileError: From<S::Error>,
    {
        let default_hint = [BlkFileHint::default()];
        let hints: &[BlkFileHint] = if self.file_hints.is_empty() {
            &default_hint
        } else {
            &self.file_hints
        };
        log::debug!("Starting to parse blocks in range: {:?}", range);
        let parse_start = std::time::Instant::now();

        'files: for hint in hints {
            let file_no = hint.file_no;
            let file_first = hint.height_first as u64;
            if file_first > range.end.saturating_sub(1) {
                break;
            }

            let file_id = BlockFileId(file_no);
            let bytes = self.store.read_file(file_no).map_err(BlockFileError::Io)?;
            let used_len = match hint.data_len {
                Some(data_len) => {
                    if data_len > bytes.len() {
                        return Err(BlockFileError::UnexpectedEof {
                            offset: data_len,
                            len: bytes.len(),
                        });
                    }
                    data_len
                }
                None => bytes.len(),
            };
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
                    let tx_count = {
                        let mut collector = TxIdCollector {
                            block_file: file_id,
                            block_start_in_file: block_start as u64,
                            block_slice,
                            sink,
                            error: None,
                            tx_count: 0,
                        };
                        bsl::Block::visit(block_slice, &mut collector)
                            .map_err(BlockFileError::Parse)?;
                        if let Some(error) = collector.error.take() {
                            return Err(error);
                        }
                        collector.tx_count
                    };
                    sink.on_block_end(tx_count).map_err(BlockFileError::from)?;
                }

                offset = block_end;
                global_height += 1;
            }
        }

        log::debug!(
            "Parsing blocks took {} seconds",
            parse_start.elapsed().as_secs_f64()
        );
        Ok(())
    }
}

struct TxIdCollector<'a, S> {
    block_file: BlockFileId,
    block_start_in_file: u64,
    block_slice: &'a [u8],
    sink: &'a mut S,
    error: Option<BlockFileError>,
    tx_count: u64,
}

impl<S> Visitor for TxIdCollector<'_, S>
where
    S: IndexSink,
    BlockFileError: From<S::Error>,
{
    fn visit_tx_in(&mut self, vin: usize, tx_in: &bsl::TxIn<'_>) -> ControlFlow<()> {
        let prevout = tx_in.prevout();
        let prev_txid = <&[u8; 32]>::try_from(prevout.txid()).expect("prevout txid is 32 bytes");
        if let Err(e) = self
            .sink
            .on_input(vin, prev_txid, prevout.vout())
            .map_err(BlockFileError::from)
        {
            self.error = Some(e);
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }

    fn visit_tx_out(&mut self, vout: usize, tx_out: &bsl::TxOut<'_>) -> ControlFlow<()> {
        if let Err(e) = self
            .sink
            .on_output(vout, tx_out.script_pubkey())
            .map_err(BlockFileError::from)
        {
            self.error = Some(e);
            return ControlFlow::Break(());
        }
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
        let txid = tx.txid().to_byte_array();
        if let Err(e) = self
            .sink
            .on_transaction(
                &txid,
                self.block_file.0,
                file_offset as u32,
                tx_len as u32,
                tx_slice,
            )
            .map_err(BlockFileError::from)
        {
            self.error = Some(e);
            return ControlFlow::Break(());
        }
        self.tx_count += 1;
        ControlFlow::Continue(())
    }
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

impl From<std::convert::Infallible> for BlockFileError {
    fn from(e: std::convert::Infallible) -> Self {
        match e {}
    }
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
