use crate::{
    ScriptPubkeyHash,
    abstract_types::AbstractTransaction,
    graph_index::{IndexedGraph, PrevOutIndex, ScriptPubkeyIndex, TxInIndex, TxIndex},
    loose::{TxId, TxInId, TxOutId},
    unified::{
        handle::TxHandle,
        id::{AnyInId, AnyOutId, AnyTxId},
    },
};

use bitcoin::consensus::Encodable;

use std::{
    collections::HashMap,
    hash::{DefaultHasher, Hasher},
    sync::Arc,
};

impl IndexedGraph for InMemoryIndex {}

pub struct InMemoryIndex {
    pub prev_txouts: HashMap<TxInId, TxOutId>,
    pub spending_txins: HashMap<TxOutId, TxInId>,
    // TODO: test that insertion order does not make a difference
    pub txs: HashMap<TxId, Arc<dyn AbstractTransaction + Send + Sync>>,
    pub tx_order: Vec<TxId>,
    /// Index mapping script pubkey hash (20 bytes) and the first transaction output ID that uses it
    pub spk_to_txout_ids: HashMap<ScriptPubkeyHash, TxOutId>,
}

pub struct LooseIndexBuilder {
    txs: Vec<Arc<dyn AbstractTransaction + Send + Sync>>,
}

impl LooseIndexBuilder {
    pub fn new() -> Self {
        Self { txs: Vec::new() }
    }

    pub fn add_tx(&mut self, tx: Arc<dyn AbstractTransaction + Send + Sync>) -> &mut Self {
        self.txs.push(tx);
        self
    }

    pub fn build(self) -> InMemoryIndex {
        let mut index = InMemoryIndex::new();
        for tx in self.txs {
            index.add_tx(tx);
        }
        index
    }
}

impl Default for LooseIndexBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for InMemoryIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InMemoryIndex")
            .field("prev_txouts", &self.prev_txouts)
            .field("spending_txins", &self.spending_txins)
            .field("txs", &format!("{} transactions", self.txs.len()))
            .field("tx_order", &self.tx_order.len())
            .finish()
    }
}

impl Default for InMemoryIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryIndex {
    pub fn new() -> Self {
        Self {
            prev_txouts: HashMap::new(),
            spending_txins: HashMap::new(),
            txs: HashMap::new(),
            tx_order: Vec::new(),
            spk_to_txout_ids: HashMap::new(),
        }
    }

    // FIXME: check all the keys before inserting. Lets not modify anything before checking for all dup checks
    pub fn add_tx<'a>(
        &'a mut self,
        tx: Arc<dyn AbstractTransaction + Send + Sync>,
    ) -> TxHandle<'a> {
        let any_tx_id = tx.id();
        let tx_id = any_tx_id
            .loose_txid()
            .expect("loose storage only supports loose txids");
        // TODO: only loose txids for now

        // Process inputs to build the index before storing
        // Collect inputs into a vector to avoid lifetime issues
        for (vin, txin) in tx.inputs().enumerate() {
            let vin_id = tx_id.txin_id(vin as u32);
            let prev_vout = txin.prev_vout();
            let prev_txid = txin.prev_txid();
            let prev_outid = TxOutId::new(
                prev_txid.loose_txid().expect("prev_txid should be loose"),
                prev_vout,
            );
            self.spending_txins.insert(prev_outid, vin_id);
            self.prev_txouts.insert(vin_id, prev_outid);
        }

        // Process outputs to build SPK index
        // Only keep track of the first transaction output ID that uses the given script pubkey.
        // The rest can be clustered via same address clustering.
        for (vout_idx, output) in tx.outputs().enumerate() {
            let spk_hash = output.script_pubkey_hash();
            self.spk_to_txout_ids
                .entry(spk_hash)
                .or_insert_with(|| TxOutId::new(tx_id, vout_idx as u32));
        }

        let result = self.txs.insert(tx_id.into(), tx);
        if result.is_some() {
            panic!("Transaction with id {:?} already exists!", tx_id);
        }
        self.tx_order.push(tx_id);

        TxHandle::new(any_tx_id, self)
    }

    // TODO: once we need stable id, we may need to manage the random key ourselves. Once we need to persist things solve this TODO
    pub fn compute_txid(txid: bitcoin::Txid) -> TxId {
        let mut hasher = DefaultHasher::new();
        let mut buf = Vec::new();
        txid.consensus_encode(&mut buf).unwrap();
        hasher.write(buf.as_slice());
        let hash = hasher.finish();
        TxId(hash as u32)
    }
}

impl PrevOutIndex for InMemoryIndex {
    fn prev_txout(&self, id: &AnyInId) -> AnyOutId {
        let loose_id = id
            .loose_id()
            .expect("loose storage only supports loose txin ids");
        let out_id = *self
            .prev_txouts
            .get(&loose_id)
            .expect("Previous output should always be present if index is built correctly");
        AnyOutId::from(out_id)
    }
}
impl TxInIndex for InMemoryIndex {
    fn spending_txin(&self, tx_out: &AnyOutId) -> Option<AnyInId> {
        let loose_out = tx_out
            .loose_id()
            .expect("loose storage only supports loose txout ids");
        self.spending_txins
            .get(&loose_out)
            .cloned()
            .map(AnyInId::from)
    }
}

impl ScriptPubkeyIndex for InMemoryIndex {
    fn script_pubkey_to_txout_id(&self, script_pubkey: &ScriptPubkeyHash) -> Option<AnyOutId> {
        self.spk_to_txout_ids
            .get(script_pubkey)
            .cloned()
            .map(AnyOutId::from)
    }
}

impl TxIndex for InMemoryIndex {
    fn tx(&self, txid: &AnyTxId) -> Option<Arc<dyn AbstractTransaction + Send + Sync>> {
        let loose_txid = txid
            .loose_txid()
            .expect("loose storage only supports loose txids");
        self.txs.get(&loose_txid).cloned()
    }
}
