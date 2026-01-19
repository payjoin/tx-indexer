use bitcoin::consensus::Encodable;

use crate::disjoint_set::DisJointSet;
use crate::loose::{TxHandle, TxId, TxInId, TxOutId};
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hasher};

pub trait PrevOutIndex {
    // TODO: this should take an input id and return an id
    // TODO: consider handle wrappers converting ids to the actual types.
    // justification: the heuristics may not care about the content of the data. Only access that thru the handler.
    fn prev_txout(&self, ot: &TxInId) -> TxOutId;
}

pub trait TxInIndex {
    fn spending_txin(&self, tx: &TxOutId) -> Option<TxInId>;
}

pub trait IndexedGraph: PrevOutIndex + TxInIndex {}

impl IndexedGraph for InMemoryIndex {}

#[derive(Debug)]
pub struct InMemoryIndex {
    pub prev_txouts: HashMap<TxInId, TxOutId>,
    pub spending_txins: HashMap<TxOutId, TxInId>,
    //  TODO: in the future replace with a trait
    // TODO: test that insertion order does not make a difference
    pub txs: HashMap<TxId, bitcoin::Transaction>,
}

impl InMemoryIndex {
    pub fn new() -> Self {
        Self {
            prev_txouts: HashMap::new(),
            spending_txins: HashMap::new(),
            txs: HashMap::new(),
        }
    }

    // FIXME: check all the keys before inserting. Lets not modify anything before checking for all dup checks
    pub fn add_tx<'a>(&'a mut self, tx: &bitcoin::Transaction) -> TxHandle<'a> {
        let id = self.compute_txid(tx.compute_txid());
        let result = self.txs.insert(id, tx.clone());
        if result.is_some() {
            panic!("Transaction with id {:?} already exists!", id);
        }
        // TODO:
        // Need to look at txs that spend from this tx.
        // Compare the long tx of the spending txins and compare with the long txid of this tx
        for (vin, txin) in tx.input.iter().enumerate() {
            let vin_id = id.txin_id(vin as u32);
            let prev_vout = txin.previous_output.vout;
            let prev_txid = self.compute_txid(txin.previous_output.txid);
            let prev_outid = prev_txid.txout_id(prev_vout);
            self.spending_txins.insert(prev_outid, vin_id);
            self.prev_txouts.insert(vin_id, prev_outid);
        }

        TxHandle::new(id, self)
    }

    // TODO: once we need stable id, we may need to manage the random key ourselves. Once we need to persist things solve this TODO
    pub fn compute_txid(&self, txid: bitcoin::Txid) -> TxId {
        let mut hasher = DefaultHasher::new();
        let mut buf = Vec::new();
        txid.consensus_encode(&mut buf).unwrap();
        hasher.write(buf.as_slice());
        let hash = hasher.finish();
        TxId(hash as u32)
    }
}

impl PrevOutIndex for InMemoryIndex {
    fn prev_txout(&self, id: &TxInId) -> TxOutId {
        self.prev_txouts
            .get(id)
            .expect("Previous output should always be present if index is build correctly")
            .clone()
    }
}
impl TxInIndex for InMemoryIndex {
    fn spending_txin(&self, tx_out: &TxOutId) -> Option<TxInId> {
        self.spending_txins.get(tx_out).cloned()
    }
}

// Clustering and classification related traits

#[derive(Debug)]
pub struct InMemoryClusteringIndex<UF: DisJointSet<TxOutId>> {
    index: InMemoryIndex,
    merged_prevouts: UF,
    // TODO: hashmap makes sense for loose repr. For packed graph this can be a large bit vector. One bit for the entire set of ordered txs.
    // TODO: this should be unhardcoded to be construct out of some generic storage type
    tagged_coinjoins: HashMap<TxId, bool>,
    tagged_change_outputs: HashMap<TxOutId, bool>,
}

impl<UF: DisJointSet<TxOutId> + Default> InMemoryClusteringIndex<UF> {
    pub fn new() -> Self {
        Self {
            index: InMemoryIndex::new(),
            merged_prevouts: UF::default(),
            tagged_coinjoins: HashMap::new(),
            tagged_change_outputs: HashMap::new(),
        }
    }

    pub fn find_root(&mut self, tx_out_id: &TxOutId) -> TxOutId {
        self.merged_prevouts.find(*tx_out_id)
    }

    pub fn union(&mut self, a: &TxOutId, b: &TxOutId) {
        self.merged_prevouts.union(*a, *b);
    }

    pub fn is_coinjoin(&self, tx_id: &TxId) -> Option<bool> {
        self.tagged_coinjoins.get(tx_id).cloned()
    }

    pub fn annotate_coinjoin(&mut self, tx_id: &TxId, is_coinjoin: bool) {
        self.tagged_coinjoins.insert(*tx_id, is_coinjoin);
    }

    pub fn is_change(&self, tx_out_id: &TxOutId) -> Option<bool> {
        self.tagged_change_outputs.get(tx_out_id).cloned()
    }

    pub fn annotate_change(&mut self, tx_out_id: &TxOutId, is_change: bool) {
        self.tagged_change_outputs.insert(*tx_out_id, is_change);
    }
}
