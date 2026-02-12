use crate::{
    abstract_types::{
        AbstractTransaction, AbstractTxIn, AbstractTxOut, IdFamily, IntoTxHandle, IntoTxOutHandle,
    },
    dense::{DenseIds, TxId, TxOutId},
    graph_index::IndexedGraph,
};

pub type DenseIndexedGraph<'a> = dyn IndexedGraph<DenseIds> + 'a;

pub struct TxHandle<'a> {
    tx_id: TxId,
    index: &'a DenseIndexedGraph<'a>,
}

impl<'a> TxHandle<'a> {
    pub fn new(tx_id: TxId, index: &'a DenseIndexedGraph<'a>) -> Self {
        Self { tx_id, index }
    }

    pub fn id(&self) -> TxId {
        self.tx_id
    }
}

impl<'a> AbstractTransaction for TxHandle<'a> {
    type I = DenseIds;
    fn id(&self) -> TxId {
        self.tx_id
    }

    fn inputs(&self) -> Box<dyn Iterator<Item = Box<dyn AbstractTxIn<I = Self::I>>> + '_> {
        let tx = self
            .index
            .tx(&self.tx_id)
            .expect("Tx should always exist if we have a handle; use Arc<InMemoryIndex> for dense");
        let inputs: Vec<_> = tx.inputs().collect();
        Box::new(inputs.into_iter())
    }

    fn outputs(&self) -> Box<dyn Iterator<Item = Box<dyn AbstractTxOut<I = Self::I>>> + '_> {
        let tx = self
            .index
            .tx(&self.tx_id)
            .expect("Tx should always exist if we have a handle; use Arc<InMemoryIndex> for dense");
        let outputs: Vec<_> = tx.outputs().collect();
        Box::new(outputs.into_iter())
    }

    fn output_len(&self) -> usize {
        self.index
            .tx(&self.tx_id)
            .expect("Tx should always exist if we have a handle; use Arc<InMemoryIndex> for dense")
            .output_len()
    }

    fn output_at(&self, index: usize) -> Option<Box<dyn AbstractTxOut<I = Self::I>>> {
        self.index
            .tx(&self.tx_id)
            .expect("Tx should always exist if we have a handle; use Arc<InMemoryIndex> for dense")
            .output_at(index)
    }

    fn locktime(&self) -> u32 {
        self.index
            .tx(&self.tx_id)
            .expect("Tx should always exist if we have a handle; use Arc<InMemoryIndex> for dense")
            .locktime()
    }
}

pub struct TxOutHandle<'a> {
    tx_out_id: TxOutId,
    index: &'a DenseIndexedGraph<'a>,
}

impl<'a> TxOutHandle<'a> {
    pub fn new(tx_out_id: TxOutId, index: &'a DenseIndexedGraph<'a>) -> Self {
        Self { tx_out_id, index }
    }
}

impl<'a> AbstractTxOut for TxOutHandle<'a> {
    type I = DenseIds;

    fn id(&self) -> <Self::I as IdFamily>::TxOutId {
        self.tx_out_id
    }

    fn value(&self) -> bitcoin::Amount {
        self.output_from_tx().value()
    }

    fn script_pubkey_hash(&self) -> crate::ScriptPubkeyHash {
        self.output_from_tx().script_pubkey_hash()
    }
}

impl<'a> TxOutHandle<'a> {
    fn output_from_tx(&self) -> Box<dyn AbstractTxOut<I = DenseIds>> {
        let tx = self
            .index
            .tx(&self.tx_out_id.txid())
            .expect("Tx should always exist; use Arc<InMemoryIndex> for dense");
        for i in 0..tx.output_len() {
            let out = tx.output_at(i).expect("output_at(i) within output_len");
            if out.id() == self.tx_out_id {
                return out;
            }
        }
        panic!(
            "output {} not found in its tx",
            self.tx_out_id.byte_offset()
        );
    }
}

impl IntoTxHandle<DenseIds> for TxId {
    fn with_index<'a>(
        self,
        index: &'a DenseIndexedGraph<'a>,
    ) -> Box<dyn AbstractTransaction<I = DenseIds> + 'a> {
        Box::new(TxHandle::new(self, index))
    }
}

impl IntoTxOutHandle<DenseIds> for TxOutId {
    fn with_index<'a>(
        self,
        index: &'a DenseIndexedGraph<'a>,
    ) -> Box<dyn AbstractTxOut<I = DenseIds> + 'a> {
        Box::new(TxOutHandle::new(self, index))
    }
}
