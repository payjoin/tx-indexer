use bitcoin::consensus::Encodable;
use std::sync::Arc;

use crate::ScriptPubkeyHash;
use crate::abstract_types::{AbstractTransaction, AbstractTxIn, AbstractTxOut};
use crate::datalog::Relation;
use crate::disjoint_set::SparseDisjointSet;
use crate::loose::{TxHandle, TxId, TxInId, TxOutId};
use std::hash::{DefaultHasher, Hasher};
use std::{
    any::TypeId,
    collections::{BTreeSet, HashMap},
};

pub trait PrevOutIndex {
    // TODO: consider handle wrappers converting ids to the actual types.
    // justification: the heuristics may not care about the content of the data. Only access that thru the handler.
    fn prev_txout(&self, ot: &TxInId) -> TxOutId;
}

pub trait TxInIndex {
    fn spending_txin(&self, tx: &TxOutId) -> Option<TxInId>;
}

pub trait ScriptPubkeyIndex {
    fn script_pubkey_to_txout_ids(&self, script_pubkey: &ScriptPubkeyHash) -> BTreeSet<TxOutId>;
}

pub trait IndexedGraph: PrevOutIndex + TxInIndex + ScriptPubkeyIndex {}

impl IndexedGraph for InMemoryIndex {}

pub struct InMemoryIndex {
    pub prev_txouts: HashMap<TxInId, TxOutId>,
    pub spending_txins: HashMap<TxOutId, TxInId>,
    // TODO: test that insertion order does not make a difference
    pub txs: HashMap<TxId, Arc<dyn AbstractTransaction + Send + Sync>>,
    pub global_clustering: SparseDisjointSet<TxOutId>,
    /// Index mapping script pubkey hash (20 bytes) to set of transaction IDs that use it
    pub spk_to_txout_ids: HashMap<ScriptPubkeyHash, BTreeSet<TxOutId>>,
}

impl std::fmt::Debug for InMemoryIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InMemoryIndex")
            .field("prev_txouts", &self.prev_txouts)
            .field("spending_txins", &self.spending_txins)
            .field("txs", &format!("{} transactions", self.txs.len()))
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
            global_clustering: SparseDisjointSet::new(),
            spk_to_txout_ids: HashMap::new(),
        }
    }

    // FIXME: check all the keys before inserting. Lets not modify anything before checking for all dup checks
    pub fn add_tx<'a>(
        &'a mut self,
        tx: Arc<dyn AbstractTransaction + Send + Sync>,
    ) -> TxHandle<'a> {
        let tx_id = tx.txid();

        // Process inputs to build the index before storing
        // Collect inputs into a vector to avoid lifetime issues
        let inputs: Vec<_> = tx.inputs().collect();
        for (vin, txin) in inputs.iter().enumerate() {
            let vin_id = tx_id.txin_id(vin as u32);
            let prev_vout = txin.prev_vout();
            let prev_txid = txin.prev_txid();
            let prev_outid = prev_txid.txout_id(prev_vout);
            self.spending_txins.insert(prev_outid, vin_id);
            self.prev_txouts.insert(vin_id, prev_outid);
        }

        // Process outputs to build SPK index
        let outputs: Vec<_> = tx.outputs().collect();
        for (vout_idx, output) in outputs.iter().enumerate() {
            let spk_hash = output.script_pubkey_hash();
            self.spk_to_txout_ids
                .entry(spk_hash)
                .or_default()
                .insert(TxOutId::new(tx_id, vout_idx as u32));
        }

        let result = self.txs.insert(tx_id, tx);
        if result.is_some() {
            panic!("Transaction with id {:?} already exists!", tx_id);
        }

        TxHandle::new(tx_id, self)
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
    fn prev_txout(&self, id: &TxInId) -> TxOutId {
        *self
            .prev_txouts
            .get(id)
            .expect("Previous output should always be present if index is build correctly")
    }
}
impl TxInIndex for InMemoryIndex {
    fn spending_txin(&self, tx_out: &TxOutId) -> Option<TxInId> {
        self.spending_txins.get(tx_out).cloned()
    }
}

impl ScriptPubkeyIndex for InMemoryIndex {
    fn script_pubkey_to_txout_ids(&self, script_pubkey: &ScriptPubkeyHash) -> BTreeSet<TxOutId> {
        self.spk_to_txout_ids
            .get(script_pubkey)
            .cloned()
            .unwrap_or_default()
    }
}

// Wrapper types for implementing abstract traits on bitcoin types
// These own their data to avoid lifetime issues when stored
struct BitcoinTxInWrapper {
    prev_txid: TxId,
    prev_vout: u32,
}

impl AbstractTxIn for BitcoinTxInWrapper {
    fn prev_txid(&self) -> TxId {
        self.prev_txid
    }

    fn prev_vout(&self) -> u32 {
        self.prev_vout
    }
}

struct BitcoinTxOutWrapper {
    value: bitcoin::Amount,
    script_pubkey: bitcoin::ScriptBuf,
}

impl AbstractTxOut for BitcoinTxOutWrapper {
    fn value(&self) -> bitcoin::Amount {
        self.value
    }

    fn script_pubkey_hash(&self) -> ScriptPubkeyHash {
        extract_script_pubkey_hash(&self.script_pubkey)
    }
}

fn extract_script_pubkey_hash(script: &bitcoin::ScriptBuf) -> ScriptPubkeyHash {
    let script_hash = script.script_hash();

    let mut hash = [0u8; 20];
    hash.copy_from_slice(&script_hash.to_raw_hash()[..]);
    hash
}

struct BitcoinTransactionWrapper {
    tx: bitcoin::Transaction,
}

impl AbstractTransaction for BitcoinTransactionWrapper {
    fn txid(&self) -> TxId {
        InMemoryIndex::compute_txid(self.tx.compute_txid())
    }

    fn inputs(&self) -> Box<dyn Iterator<Item = Box<dyn AbstractTxIn>> + '_> {
        // Collect into a vector to avoid lifetime issues with the iterator
        let inputs: Vec<Box<dyn AbstractTxIn>> = self
            .tx
            .input
            .iter()
            .map(|txin| {
                Box::new(BitcoinTxInWrapper {
                    prev_txid: InMemoryIndex::compute_txid(txin.previous_output.txid),
                    prev_vout: txin.previous_output.vout,
                }) as Box<dyn AbstractTxIn>
            })
            .collect();
        Box::new(inputs.into_iter())
    }

    fn outputs(&self) -> Box<dyn Iterator<Item = Box<dyn AbstractTxOut>> + '_> {
        // Collect into a vector to avoid lifetime issues with the iterator
        let outputs: Vec<Box<dyn AbstractTxOut>> = self
            .tx
            .output
            .iter()
            .map(|txout| {
                Box::new(BitcoinTxOutWrapper {
                    value: txout.value,
                    script_pubkey: txout.script_pubkey.clone(),
                }) as Box<dyn AbstractTxOut>
            })
            .collect();
        Box::new(outputs.into_iter())
    }

    fn output_len(&self) -> usize {
        self.tx.output.len()
    }

    fn output_at(&self, index: usize) -> Option<Box<dyn AbstractTxOut>> {
        self.tx.output.get(index).map(|txout| {
            Box::new(BitcoinTxOutWrapper {
                value: txout.value,
                script_pubkey: txout.script_pubkey.clone(),
            }) as Box<dyn AbstractTxOut>
        })
    }
}

impl From<bitcoin::Transaction> for Box<dyn AbstractTransaction + Send + Sync> {
    fn from(val: bitcoin::Transaction) -> Self {
        Box::new(BitcoinTransactionWrapper { tx: val })
    }
}

pub trait FactStore {
    fn insert<R: Relation>(&mut self, fact: R::Fact) -> bool;
    fn len<R: Relation>(&self) -> usize;
    fn read_range<R: Relation>(&self, start: usize, end: usize) -> Vec<R::Fact>;
    fn contains<R: Relation>(&self, fact: &R::Fact) -> bool;
}

pub struct MemStore {
    index: InMemoryIndex,
    /// Map of relational type ids and their state
    rels: HashMap<TypeId, Box<dyn RelBox>>,
}

impl MemStore {
    pub fn new(index: InMemoryIndex) -> Self {
        Self {
            index,
            rels: HashMap::new(),
        }
    }

    pub fn index(&self) -> &InMemoryIndex {
        &self.index
    }

    pub fn index_mut(&mut self) -> &mut InMemoryIndex {
        &mut self.index
    }

    // TODO: placeholder hack should remove later
    pub fn initialize<R: Relation>(&mut self) {
        let tid = TypeId::of::<R>();
        self.rels
            .entry(tid)
            .or_insert_with(|| Box::new(RelState::<R::Fact>::new()));
    }

    fn get_or_init<R: Relation>(&mut self) -> &mut RelState<R::Fact> {
        let tid = TypeId::of::<R>();
        self.rels
            .entry(tid)
            .or_insert_with(|| Box::new(RelState::<R::Fact>::new()));
        self.rels
            .get_mut(&tid)
            .unwrap()
            .as_any_mut()
            .downcast_mut::<RelState<R::Fact>>()
            .expect("relation state type mismatch")
    }

    fn get<R: Relation>(&self) -> &RelState<R::Fact> {
        let tid = TypeId::of::<R>();
        self.rels
            .get(&tid)
            .expect("relation not initialized")
            .as_any()
            .downcast_ref::<RelState<R::Fact>>()
            .expect("relation state type mismatch")
    }
}

trait RelBox {
    fn as_any(&self) -> &dyn std::any::Any;
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
}

struct RelState<F: Clone + Eq + 'static> {
    log: Vec<F>,
}

impl<F: Clone + Eq + 'static> RelBox for RelState<F> {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl<F: Clone + Eq + 'static> RelState<F> {
    fn new() -> Self {
        Self { log: Vec::new() }
    }

    fn insert(&mut self, fact: F) -> bool {
        self.log.push(fact);
        true
    }
}

impl FactStore for MemStore {
    fn insert<R: Relation>(&mut self, fact: R::Fact) -> bool {
        self.get_or_init::<R>().insert(fact)
    }

    fn len<R: Relation>(&self) -> usize {
        self.get::<R>().log.len()
    }

    fn read_range<R: Relation>(&self, start: usize, end: usize) -> Vec<R::Fact> {
        let st = self.get::<R>();
        let end = end.min(st.log.len());
        if start >= end {
            return Vec::new();
        }
        st.log[start..end].to_vec()
    }

    fn contains<R: Relation>(&self, fact: &R::Fact) -> bool {
        self.get::<R>().log.contains(fact)
    }
}
