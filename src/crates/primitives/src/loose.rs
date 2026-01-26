use bitcoin::Amount;

use crate::abstract_types::AbstractTransaction;
use crate::abstract_types::EnumerateOutputValueInArbitraryOrder;
use crate::abstract_types::EnumerateSpentTxOuts;
use crate::abstract_types::OutputCount;
use crate::abstract_types::TxConstituent;
use crate::disjoint_set::DisJointSet;
use crate::storage::InMemoryIndex;

// Type defintions for loose txs and their ids

// TBD whether this is a generic or u32 specifically
/// Sum of the short id of the txid and vout.
#[derive(PartialEq, Eq, Hash, Copy, Clone, Debug)]
pub struct TxOutId {
    pub txid: TxId,
    pub vout: u32,
}

impl TxOutId {
    pub fn new(txid: TxId, vout: u32) -> Self {
        Self { txid, vout }
    }
}

impl TxOutId {
    pub fn with<'a>(&self, index: &'a InMemoryIndex) -> TxOutHandle<'a> {
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
            .output_at(self.id.vout as usize)
            .expect("Output should always exist")
            .value()
    }

    pub fn vout(&self) -> u32 {
        self.id.vout
    }

    pub fn spent_by(&self) -> Option<TxInHandle<'a>> {
        self.index
            .spending_txins
            .get(&self.id)
            .map(|txin_id| txin_id.with(self.index))
    }
}

pub struct ClusterHandle<'a> {
    txout_id: TxOutId,
    index: &'a InMemoryIndex,
    // TODO: this is specific for global clustering. not any particular clustering fact.
    // Should specify which disjoin sets structure it is using.
}

impl<'a> ClusterHandle<'a> {
    pub fn new(txout_id: TxOutId, index: &'a InMemoryIndex) -> Self {
        Self { txout_id, index }
    }

    pub fn iter_txouts(&self) -> impl Iterator<Item = TxOutHandle<'a>> {
        self.index
            .global_clustering
            .iter_set(self.txout_id)
            .map(|txout_id| txout_id.with(self.index))
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

    pub fn txid(&self) -> TxId {
        self.txid
    }

    pub fn vin(&self) -> u32 {
        self.vin
    }
}

pub struct TxInHandle<'a> {
    id: TxInId,
    index: &'a InMemoryIndex,
}

impl<'a> TxInHandle<'a> {
    pub fn id(&self) -> TxInId {
        self.id
    }

    pub fn tx(&self) -> TxHandle<'a> {
        self.id.txid.with(self.index)
    }
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

    #[allow(unused)]
    fn txin_handle<'a>(&self, index: &'a InMemoryIndex, vin: u32) -> TxInHandle<'a> {
        self.txin_id(vin).with(index)
    }
}

pub struct TxHandle<'a> {
    id: TxId,
    index: &'a InMemoryIndex,
}

impl<'a> TxHandle<'a> {
    pub fn new(id: TxId, index: &'a InMemoryIndex) -> Self {
        Self { id, index }
    }

    pub fn id(&self) -> TxId {
        self.id
    }

    fn spent_coins(&self) -> impl Iterator<Item = TxOutId> {
        self.index.prev_txouts.values().copied()
    }

    pub fn outputs(&self) -> impl Iterator<Item = TxOutHandle<'a>> {
        let outputs_len = self
            .index
            .txs
            .get(&self.id)
            .expect("If I have a handle it must exist?")
            .output_len();
        (0..outputs_len).map(|i| self.id.txout_handle(self.index, i as u32))
    }

    pub fn output_count(&self) -> usize {
        self.id.with(self.index).outputs().count()
    }

    pub fn is_coinbase(&self) -> bool {
        self.spent_coins().count() == 0
    }

    pub fn inputs_are_clustered(&self) -> bool {
        let inputs = self.spent_coins().collect::<Vec<_>>();
        if inputs.is_empty() {
            return false;
        }
        let first_root = self.index.global_clustering.find(inputs[0]);
        inputs.iter().all(|input| {
            let other = self.index.global_clustering.find(*input);

            other == first_root
        })
    }
}

impl AbstractTransaction for TxHandle<'_> {
    fn txid(&self) -> TxId {
        self.id
    }

    // TODO: are these expects correct when in a pruned node
    fn inputs(
        &self,
    ) -> Box<dyn Iterator<Item = Box<dyn crate::abstract_types::AbstractTxIn>> + '_> {
        // Delegate to the stored transaction in the index
        self.index
            .txs
            .get(&self.id)
            .expect("Tx should always exist if we have a handle")
            .inputs()
    }

    fn outputs(
        &self,
    ) -> Box<dyn Iterator<Item = Box<dyn crate::abstract_types::AbstractTxOut>> + '_> {
        // Delegate to the stored transaction in the index
        self.index
            .txs
            .get(&self.id)
            .expect("Tx should always exist if we have a handle")
            .outputs()
    }

    fn output_len(&self) -> usize {
        self.index
            .txs
            .get(&self.id)
            .expect("Tx should always exist if we have a handle")
            .output_len()
    }

    fn output_at(&self, index: usize) -> Option<Box<dyn crate::abstract_types::AbstractTxOut>> {
        // Delegate to the stored transaction in the index
        self.index
            .txs
            .get(&self.id)
            .expect("Tx should always exist if we have a handle")
            .output_at(index)
    }
}

impl<'a> TxConstituent for TxOutHandle<'a> {
    type Handle = TxHandle<'a>;
    fn containing_tx(&self) -> Self::Handle {
        self.tx()
    }

    fn vout(&self) -> usize {
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
