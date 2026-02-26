use crate::handle::TxHandle;
use crate::traits::graph_index::{
    IndexedGraph, OutpointIndex, PrevOutIndex, ScriptPubkeyIndex, TxInIndex, TxIndex, TxIoIndex,
    TxOutDataIndex,
};
use crate::{
    AnyInId, AnyOutId, AnyTxId, ScriptPubkeyHash, traits::abstract_types::AbstractTransaction,
};

use bitcoin::consensus::Encodable;

use std::{
    collections::HashMap,
    hash::{DefaultHasher, Hasher},
    sync::Arc,
};

// TBD whether this is a generic or u32 specifically
/// Sum of the short id of the txid and vout.
#[derive(PartialEq, Eq, Hash, Copy, Clone, Debug, Ord, PartialOrd)]
pub struct TxOutId {
    txid: TxId,
    vout: u32,
}

impl TxOutId {
    pub fn new(txid: TxId, vout: u32) -> Self {
        Self { txid, vout }
    }

    pub fn txid(&self) -> TxId {
        self.txid
    }

    pub fn vout(&self) -> u32 {
        self.vout
    }
}

/// Sum of the short id of the txid and vin
#[derive(PartialEq, Eq, Hash, Copy, Clone, Debug, Ord, PartialOrd)]
pub struct TxInId {
    /// The transaction ID of the transaction spending this input
    txid: TxId,
    /// The input index of the transaction spending this input
    vin: u32,
}

impl TxInId {
    pub fn new(txid: TxId, vin: u32) -> Self {
        Self { txid, vin }
    }

    pub fn txid(&self) -> TxId {
        self.txid
    }

    pub fn vin(&self) -> u32 {
        self.vin
    }
}

#[derive(PartialEq, Eq, Hash, Copy, Clone, Debug, Ord, PartialOrd)]
pub struct TxId(pub u32);

impl TxId {
    pub fn new(txid: u32) -> Self {
        Self(txid)
    }

    pub fn index(self) -> u32 {
        self.0
    }

    pub fn txout_id(self, vout: u32) -> TxOutId {
        TxOutId::new(self, vout)
    }

    pub fn txin_id(self, vin: u32) -> TxInId {
        TxInId::new(self, vin)
    }
}

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

        // Process inputs to build the index before storing
        // Collect inputs into a vector to avoid lifetime issues
        for (vin, txin) in tx.inputs().enumerate() {
            let vin_id = tx_id.txin_id(vin as u32);
            let prev_vout = txin.prev_vout();
            let prev_txid = txin.prev_txid();
            if let (Some(prev_vout), Some(prev_txid)) = (prev_vout, prev_txid) {
                let prev_outid = TxOutId::new(
                    prev_txid.loose_txid().expect("prev_txid should be loose"),
                    prev_vout,
                );
                self.spending_txins.insert(prev_outid, vin_id);
                self.prev_txouts.insert(vin_id, prev_outid);
            }
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

        any_tx_id.with(self)
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
    fn prev_txout(&self, id: &AnyInId) -> Option<AnyOutId> {
        let loose_id = id
            .loose_id()
            .expect("loose storage only supports loose txin ids");
        self.prev_txouts.get(&loose_id).copied().map(AnyOutId::from)
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

impl TxIoIndex for InMemoryIndex {
    fn tx_in_ids(&self, txid: &AnyTxId) -> Vec<AnyInId> {
        let loose_txid = txid
            .loose_txid()
            .expect("loose storage only supports loose txids");
        let tx = self
            .txs
            .get(&loose_txid)
            .expect("loose txid not found in storage");
        let input_len = tx.inputs().count();
        (0..input_len)
            .map(|vin| AnyInId::from(TxInId::new(loose_txid, vin as u32)))
            .collect()
    }

    fn tx_out_ids(&self, txid: &AnyTxId) -> Vec<AnyOutId> {
        let loose_txid = txid
            .loose_txid()
            .expect("loose storage only supports loose txids");
        let tx = self
            .txs
            .get(&loose_txid)
            .expect("loose txid not found in storage");
        let output_len = tx.output_len();
        (0..output_len)
            .map(|vout| AnyOutId::from(TxOutId::new(loose_txid, vout as u32)))
            .collect()
    }

    fn locktime(&self, txid: &AnyTxId) -> u32 {
        let tx = self.tx(txid).expect("loose txid not found in storage");
        tx.locktime()
    }
}

impl OutpointIndex for InMemoryIndex {
    fn outpoint_for_out(&self, out_id: &AnyOutId) -> (AnyTxId, u32) {
        let loose_out = out_id
            .loose_id()
            .expect("loose storage only supports loose outids");
        (AnyTxId::from(loose_out.txid()), loose_out.vout())
    }
}

impl TxOutDataIndex for InMemoryIndex {
    fn tx_out_data(&self, out_id: &AnyOutId) -> (bitcoin::Amount, ScriptPubkeyHash) {
        let loose_out = out_id
            .loose_id()
            .expect("loose storage only supports loose outids");
        let tx = self
            .txs
            .get(&loose_out.txid())
            .expect("loose txid not found in storage");
        let output = tx
            .output_at(loose_out.vout() as usize)
            .expect("txout should be present if index is built correctly");
        (output.value(), output.script_pubkey_hash())
    }
}
