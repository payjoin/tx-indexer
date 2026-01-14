use bitcoin::Amount;
use bitcoin::consensus::Encodable;
use std::collections::HashMap;
use std::hash::DefaultHasher;
use std::hash::Hasher;

use crate::abstract_types::AbstractTxHandle;
use crate::abstract_types::EnumerateOutputValueInArbitraryOrder;
use crate::abstract_types::EnumerateSpentTxOuts;
use crate::abstract_types::OutputCount;
use crate::abstract_types::TxConstituent;
use crate::disjoint_set::DisJointSet;

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
    prev_txouts: HashMap<TxInId, TxOutId>,
    spending_txins: HashMap<TxOutId, TxInId>,
    //  TODO: in the future replace with a trait
    // TODO: test that insertion order does not make a difference
    txs: HashMap<TxId, bitcoin::Transaction>,
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

        TxHandle { id, index: self }
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

// Type defintions for loose txs and their ids

// TBD whether this is a generic or u32 specifically
/// Sum of the short id of the txid and vout.
#[derive(PartialEq, Eq, Hash, Copy, Clone, Debug)]
pub struct TxOutId {
    pub txid: TxId,
    pub vout: u32,
}

impl TxOutId {
    fn with<'a>(&self, index: &'a InMemoryIndex) -> TxOutHandle<'a> {
        TxOutHandle { id: *self, index }
    }
}

pub struct TxOutHandle<'a> {
    id: TxOutId,
    index: &'a InMemoryIndex,
}

impl<'a> TxOutHandle<'a> {
    // TODO: this new should exist. You always get a handle from the id.
    pub fn new(id: TxOutId, index: &'a InMemoryIndex) -> Self {
        Self { id, index }
    }

    pub fn id(&self) -> TxOutId {
        self.id
    }

    pub fn tx(&self) -> TxHandle<'a> {
        self.id.txid.with(self.index)
    }

    pub fn amount(&self) -> Amount {
        self.index
            .txs
            .get(&self.id.txid)
            .expect("Tx should always exist")
            .output[self.id.vout as usize]
            .value
    }

    pub fn vout(&self) -> u32 {
        self.id.vout
    }
}

/// Sum of the short id of the txid and vin
#[derive(PartialEq, Eq, Hash, Copy, Clone, Debug)]
pub struct TxInId {
    txid: TxId,
    vin: u32,
}

impl TxInId {
    fn with<'a>(&self, index: &'a InMemoryIndex) -> TxInHandle<'a> {
        TxInHandle { id: *self, index }
    }
}

pub struct TxInHandle<'a> {
    id: TxInId,
    index: &'a InMemoryIndex,
}

#[derive(PartialEq, Eq, Hash, Copy, Clone, Debug)]
pub struct TxId(pub u32);

impl TxId {
    pub fn with<'a>(&self, index: &'a InMemoryIndex) -> TxHandle<'a> {
        TxHandle::new(*self, index)
    }

    // TODO: this should not be pub. Only pub'd for testing purposes.
    pub fn txout_id(&self, vout: u32) -> TxOutId {
        TxOutId { txid: *self, vout }
    }

    pub fn txin_id(&self, vin: u32) -> TxInId {
        TxInId { txid: *self, vin }
    }

    fn txout_handle<'a>(&self, index: &'a InMemoryIndex, vout: u32) -> TxOutHandle<'a> {
        self.txout_id(vout).with(index)
    }

    fn txin_handle<'a>(&self, index: &'a InMemoryIndex, vin: u32) -> TxInHandle<'a> {
        self.txin_id(vin).with(index)
    }
}

pub struct TxHandle<'a> {
    id: TxId,
    index: &'a InMemoryIndex,
}

impl<'a> TxHandle<'a> {
    fn new(id: TxId, index: &'a InMemoryIndex) -> Self {
        Self { id, index }
    }

    pub fn id(&self) -> TxId {
        self.id
    }

    fn spent_coins(&self) -> impl Iterator<Item = TxOutId> {
        self.index
            .prev_txouts
            .iter()
            .map(|(_, outid)| outid.clone())
    }

    pub fn outputs(&self) -> impl Iterator<Item = TxOutHandle<'a>> {
        let outputs_len = self
            .index
            .txs
            .get(&self.id)
            .expect("If I have a handle it must exist?")
            .output
            .len();
        (0..outputs_len)
            .into_iter()
            .map(|i| self.id.txout_handle(self.index, i as u32))
    }

    pub fn output_count(&self) -> usize {
        self.id.with(self.index).outputs().count()
    }
}

impl AbstractTxHandle for TxHandle<'_> {
    fn id(&self) -> TxId {
        self.id
    }
}

impl<'a> TxConstituent for TxOutHandle<'a> {
    type Handle = TxHandle<'a>;
    fn containing_tx(&self) -> Self::Handle {
        self.tx()
    }

    fn index(&self) -> usize {
        self.id.vout as usize
    }
}

impl OutputCount for TxHandle<'_> {
    fn output_count(&self) -> usize {
        self.output_count()
    }
}

// TODO: this should be a handle type generated by the handle
// TODO: also need an abstract transaction Data struct
impl<'a> EnumerateSpentTxOuts for TxHandle<'a> {
    fn spent_coins(&self) -> impl Iterator<Item = TxOutId> {
        self.spent_coins()
    }
}

impl<'a> EnumerateOutputValueInArbitraryOrder for TxHandle<'a> {
    fn output_values(&self) -> impl Iterator<Item = Amount> {
        self.outputs().map(|output| output.amount())
    }
}
