use bitcoin::consensus::Encodable;
use bitcoin::hashes::Hash;
use std::collections::HashMap;

pub trait ToShortId: bitcoin::consensus::Encodable {
    /// Produce 80 byte hash of the item.
    fn short_id(&self) -> u32;
}

macro_rules! impl_to_short_id {
    ($t:ty) => {
        impl ToShortId for $t {
            fn short_id(&self) -> u32 {
                let mut buf = Vec::new();
                self.consensus_encode(&mut buf).unwrap();
                // TODO: replace with rapid hash or siphash
                // to calculate ids we need a secret value so no one can grind txids that could create collissions.

                let hash = bitcoin::hashes::sha256::Hash::hash(buf.as_slice());

                u32::from_le_bytes(
                    hash.to_byte_array()
                        .to_vec()
                        .into_iter()
                        .take(4)
                        .collect::<Vec<u8>>()
                        .try_into()
                        .unwrap(),
                )
            }
        }
    };
}

impl_to_short_id!(bitcoin::Txid);

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

pub struct InMemoryIndex {
    prev_txouts: HashMap<TxInId, TxOutId>,
    spending_txins: HashMap<TxOutId, TxInId>,
    //  TODO: in the future replace with a trait
    // TODO: is the insertion order important / meaningfu?
    txs: HashMap<LooseTxId, bitcoin::Transaction>,
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
    pub fn add_tx<'a>(&'a mut self, tx: &bitcoin::Transaction) -> LooseTxHandle<'a> {
        let id = self.compute_loose_txid(tx.compute_txid());
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
            let prev_txid = self.compute_loose_txid(txin.previous_output.txid);
            let prev_outid = prev_txid.txout_id(prev_vout);
            self.spending_txins.insert(prev_outid, vin_id);
            self.prev_txouts.insert(vin_id, prev_outid);
        }

        LooseTxHandle { id, index: self }
    }

    fn compute_loose_txid(&self, txid: bitcoin::Txid) -> LooseTxId {
        // TODO: replace with rapid hash or siphash instead of hashing the txid
        LooseTxId(txid.short_id() as u32)
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

// Type defintions for loose txs and their ids

// TBD whether this is a generic or u32 specifically
/// Sum of the short id of the txid and vout.
#[derive(PartialEq, Eq, Hash, Copy, Clone, Debug)]
pub struct TxOutId(u32);
pub struct TxOutHandle<'a> {
    id: TxOutId,
    index: &'a InMemoryIndex,
}

impl<'a> TxOutHandle<'a> {
    fn new(id: TxOutId, index: &'a InMemoryIndex) -> Self {
        Self { id, index }
    }
}

/// Sum of the short id of the txid and vin
#[derive(PartialEq, Eq, Hash, Copy, Clone, Debug)]
pub struct TxInId(u32);
#[derive(PartialEq, Eq, Hash, Copy, Clone, Debug)]
pub struct LooseTxId(u32);

impl LooseTxId {
    pub fn new(txid: bitcoin::Txid) -> Self {
        let txid_short_id = txid.short_id();
        Self(txid_short_id)
    }

    pub fn txout_id(&self, vout: u32) -> TxOutId {
        TxOutId(self.0.saturating_add(vout))
    }

    pub fn txin_id(&self, vin: u32) -> TxInId {
        TxInId(self.0.saturating_add(vin))
    }
}
pub struct LooseTxData {
    /// Short ids of txouts of the previous transactions
    spent_coins: Vec<TxOutId>,
}

pub struct LooseTxHandle<'a> {
    id: LooseTxId,
    index: &'a InMemoryIndex,
}

impl<'a> LooseTxHandle<'a> {
    fn new(id: LooseTxId, index: &'a InMemoryIndex) -> Self {
        Self { id, index }
    }
}
pub trait EnumerateSpentTxOuts {
    // TODO: do iterator later (maybe)
    // TODO:  handle?
    fn spent_coins(&self) -> Vec<TxOutId>;
}

// TODO: this should be a handle type generated by the handle
// TODO: also need an abstract transaction Data struct
impl EnumerateSpentTxOuts for LooseTxData {
    fn spent_coins(&self) -> Vec<TxOutId> {
        self.spent_coins.clone()
    }
}

impl EnumerateSpentTxOuts for bitcoin::Transaction {
    fn spent_coins(&self) -> Vec<TxOutId> {
        self.input
            .iter()
            .enumerate()
            .map(|(vin, _)| {
                let prev_txid = LooseTxId::new(self.input[vin].previous_output.txid);
                let prev_vout = self.input[vin].previous_output.vout;
                prev_txid.txout_id(prev_vout)
            })
            .collect()
    }
}
