use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::Arc,
};

use crate::{
    ScriptPubkeyHash,
    abstract_types::{AbstractTransaction, AbstractTxIn, AbstractTxOut, TxOutIdOps},
    dense::{BlockFileError, DenseIds, Parser, TxId, TxInId, TxOutId},
    graph_index::{IndexedGraph, PrevOutIndex, ScriptPubkeyIndex, TxInIndex, TxIndex},
};

pub struct InMemoryIndexBuilder;

impl InMemoryIndexBuilder {
    /// Parse blocks and return parser (in an `Arc`) and txid map for building the index.
    pub fn build_over_range(
        blocks_dir: impl Into<PathBuf>,
        block_range: std::ops::Range<u64>,
    ) -> Result<(Arc<Parser>, HashMap<bitcoin::Txid, TxId>), BlockFileError> {
        let mut parser = Parser::new(blocks_dir);
        let txids = parser.parse_blocks(block_range)?;
        Ok((Arc::new(parser), txids))
    }
}

pub struct InMemoryIndex {
    pub(crate) parser: Arc<Parser>,
    txids: HashSet<TxId>,
    spending_txins: HashMap<TxOutId, TxInId>,
    prev_txouts: HashMap<TxInId, TxOutId>,
    spk_to_txout_ids: HashMap<ScriptPubkeyHash, TxOutId>,
}

impl InMemoryIndex {
    pub fn new(parser: Arc<Parser>) -> Self {
        Self {
            parser,
            txids: HashSet::new(),
            spending_txins: HashMap::new(),
            prev_txouts: HashMap::new(),
            spk_to_txout_ids: HashMap::new(),
        }
    }

    pub fn add_txs(&mut self, txs: HashMap<bitcoin::Txid, TxId>) {
        for (_, dense_id) in txs.iter() {
            let txin_ids = self.parser.get_txin_ids(*dense_id);
            for txin_id in txin_ids {
                let txin = self.parser.get_txin(txin_id);
                let ot = txin.previous_output;
                if let Some(prev_dense_id) = txs.get(&ot.txid) {
                    let prev_txout_id = self.parser.get_txout_ids(*prev_dense_id)[ot.vout as usize];
                    self.prev_txouts.insert(txin_id, prev_txout_id);
                    self.spending_txins.insert(prev_txout_id, txin_id);
                }
            }
            let txout_ids = self.parser.get_txout_ids(*dense_id);
            for txout_id in txout_ids {
                let txout = self.parser.get_txout(txout_id);
                let spk_hash = extract_script_pubkey_hash(&txout.script_pubkey);
                self.spk_to_txout_ids.insert(spk_hash, txout_id);
            }
            self.txids.insert(*dense_id);
        }
    }
}

// TODO: remove this and associated types.
/// Owned transaction view for dense storage: loads tx data from disk via the parser on demand.
pub struct DenseTx {
    tx_id: TxId,
    index: Arc<InMemoryIndex>,
}

impl DenseTx {
    pub(crate) fn new(tx_id: TxId, index: Arc<InMemoryIndex>) -> Self {
        Self { tx_id, index }
    }
}

impl AbstractTransaction for DenseTx {
    type I = DenseIds;
    fn id(&self) -> TxId {
        self.tx_id
    }
    fn inputs(&self) -> Box<dyn Iterator<Item = Box<dyn AbstractTxIn<I = Self::I>>> + '_> {
        let txin_ids = self.index.parser.get_txin_ids(self.tx_id);
        let index = Arc::clone(&self.index);
        Box::new(txin_ids.into_iter().map(move |txin_id| {
            Box::new(DenseTxIn {
                txin_id,
                index: Arc::clone(&index),
            }) as Box<dyn AbstractTxIn<I = DenseIds>>
        }))
    }
    fn outputs(&self) -> Box<dyn Iterator<Item = Box<dyn AbstractTxOut<I = Self::I>>> + '_> {
        let txout_ids = self.index.parser.get_txout_ids(self.tx_id);
        let index = Arc::clone(&self.index);
        Box::new(txout_ids.into_iter().map(move |txout_id| {
            Box::new(DenseTxOut {
                txout_id,
                index: Arc::clone(&index),
            }) as Box<dyn AbstractTxOut<I = DenseIds>>
        }))
    }
    fn output_len(&self) -> usize {
        self.index.parser.get_txout_ids(self.tx_id).len()
    }
    fn output_at(&self, index: usize) -> Option<Box<dyn AbstractTxOut<I = Self::I>>> {
        let txout_ids = self.index.parser.get_txout_ids(self.tx_id);
        let txout_id = *txout_ids.get(index)?;
        Some(Box::new(DenseTxOut {
            txout_id,
            index: Arc::clone(&self.index),
        }))
    }
    fn locktime(&self) -> u32 {
        self.index
            .parser
            .get_tx(self.tx_id)
            .lock_time
            .to_consensus_u32()
    }
}

struct DenseTxIn {
    txin_id: TxInId,
    index: Arc<InMemoryIndex>,
}

impl AbstractTxIn for DenseTxIn {
    type I = DenseIds;
    fn prev_txid(&self) -> TxId {
        self.index.prev_txout(&self.txin_id).containing_txid()
    }
    fn prev_vout(&self) -> u32 {
        let prev_txout_id = self.index.prev_txout(&self.txin_id);
        let txout_ids = self
            .index
            .parser
            .get_txout_ids(prev_txout_id.containing_txid());
        txout_ids
            .iter()
            .position(|id| *id == prev_txout_id)
            .map(|i| i as u32)
            .unwrap_or(0)
    }
    fn prev_txout_id(&self) -> TxOutId {
        self.index.prev_txout(&self.txin_id)
    }
}

struct DenseTxOut {
    txout_id: TxOutId,
    index: Arc<InMemoryIndex>,
}

impl AbstractTxOut for DenseTxOut {
    type I = DenseIds;
    fn id(&self) -> TxOutId {
        self.txout_id
    }
    fn value(&self) -> bitcoin::Amount {
        self.index.parser.get_txout(self.txout_id).value
    }
    fn script_pubkey_hash(&self) -> ScriptPubkeyHash {
        extract_script_pubkey_hash(&self.index.parser.get_txout(self.txout_id).script_pubkey)
    }
}

impl TxIndex for Arc<InMemoryIndex> {
    type I = DenseIds;
    fn tx(&self, txid: &TxId) -> Option<Arc<dyn AbstractTransaction<I = Self::I> + Send + Sync>> {
        self.txids.get(txid).map(|_| {
            Arc::new(DenseTx::new(*txid, self.clone()))
                as Arc<dyn AbstractTransaction<I = DenseIds> + Send + Sync>
        })
    }
}

impl PrevOutIndex for Arc<InMemoryIndex> {
    type I = DenseIds;
    fn prev_txout(&self, ot: &TxInId) -> TxOutId {
        self.as_ref().prev_txout(ot)
    }
}
impl TxInIndex for Arc<InMemoryIndex> {
    type I = DenseIds;
    fn spending_txin(&self, txout_id: &TxOutId) -> Option<TxInId> {
        self.as_ref().spending_txin(txout_id)
    }
}
impl ScriptPubkeyIndex for Arc<InMemoryIndex> {
    type I = DenseIds;
    fn script_pubkey_to_txout_id(&self, script_pubkey: &ScriptPubkeyHash) -> Option<TxOutId> {
        self.as_ref().script_pubkey_to_txout_id(script_pubkey)
    }
}
impl IndexedGraph<DenseIds> for Arc<InMemoryIndex> {}

impl PrevOutIndex for InMemoryIndex {
    type I = DenseIds;
    fn prev_txout(&self, ot: &TxInId) -> TxOutId {
        // TODO: remove unwrap
        self.prev_txouts.get(ot).cloned().unwrap()
    }
}

impl TxInIndex for InMemoryIndex {
    type I = DenseIds;
    fn spending_txin(&self, txout_id: &TxOutId) -> Option<TxInId> {
        self.spending_txins.get(txout_id).cloned()
    }
}

impl ScriptPubkeyIndex for InMemoryIndex {
    type I = DenseIds;
    fn script_pubkey_to_txout_id(&self, script_pubkey: &ScriptPubkeyHash) -> Option<TxOutId> {
        self.spk_to_txout_ids.get(script_pubkey).cloned()
    }
}

fn extract_script_pubkey_hash(script: &bitcoin::ScriptBuf) -> ScriptPubkeyHash {
    let script_hash = script.script_hash();

    let mut hash = [0u8; 20];
    hash.copy_from_slice(&script_hash.to_raw_hash()[..]);
    hash
}
