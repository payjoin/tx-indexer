use crate::{
    AnyTxId,
    traits::{
        abstract_types::{AbstractTransaction, AbstractTxIn, AbstractTxOut},
        graph_index::IndexedGraph,
    },
};

/// Handle for a transaction in a unified index.
pub struct TxHandle<'a> {
    tx_id: AnyTxId,
    index: &'a dyn IndexedGraph,
    // TODO: in the dense case we could cache the tx data
}

impl<'a> TxHandle<'a> {
    // TODO: should only be able to create handles using `with()`
    pub fn new(tx_id: AnyTxId, index: &'a dyn IndexedGraph) -> Self {
        Self { tx_id, index }
    }

    pub fn id(&self) -> AnyTxId {
        self.tx_id
    }
}

impl<'a> AbstractTransaction for TxHandle<'a> {
    fn id(&self) -> AnyTxId {
        self.tx_id
    }

    fn inputs(&self) -> Box<dyn Iterator<Item = Box<dyn AbstractTxIn>> + '_> {
        let tx = self.index.tx(&self.tx_id).expect(
            "Tx should always exist if we have a handle; ensure the index is built correctly",
        );
        let inputs: Vec<_> = tx.inputs().collect();
        Box::new(inputs.into_iter())
    }

    fn outputs(&self) -> Box<dyn Iterator<Item = Box<dyn AbstractTxOut>> + '_> {
        let tx = self.index.tx(&self.tx_id).expect(
            "Tx should always exist if we have a handle; ensure the index is built correctly",
        );
        let outputs: Vec<_> = tx.outputs().collect();
        Box::new(outputs.into_iter())
    }

    fn output_len(&self) -> usize {
        self.index
            .tx(&self.tx_id)
            .expect("Tx should always exist if we have a handle")
            .output_len()
    }

    fn output_at(&self, index: usize) -> Option<Box<dyn AbstractTxOut>> {
        self.index
            .tx(&self.tx_id)
            .expect("Tx should always exist if we have a handle")
            .output_at(index)
    }

    fn locktime(&self) -> u32 {
        self.index
            .tx(&self.tx_id)
            .expect("Tx should always exist if we have a handle")
            .locktime()
    }
}
