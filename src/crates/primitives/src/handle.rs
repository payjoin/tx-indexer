use crate::{
    AnyInId, AnyOutId, AnyTxId,
    traits::{
        abstract_types::{AbstractTransaction, AbstractTxIn, AbstractTxOut},
        graph_index::IndexedGraph,
    },
};

/// Handle for a transaction in a unified index.
pub struct TxHandle<'a> {
    pub(crate) tx_id: AnyTxId,
    pub(crate) index: &'a dyn IndexedGraph,
    // TODO: in the dense case we could cache the tx data
}

/// Handle for a transaction input in a unified index.
pub struct TxInHandle<'a> {
    pub(crate) in_id: AnyInId,
    pub(crate) index: &'a dyn IndexedGraph,
}

/// Handle for a transaction output in a unified index.
pub struct TxOutHandle<'a> {
    pub(crate) out_id: AnyOutId,
    pub(crate) index: &'a dyn IndexedGraph,
}

impl<'a> TxHandle<'a> {
    pub fn id(&self) -> AnyTxId {
        self.tx_id
    }
}

impl<'a> TxInHandle<'a> {
    pub fn id(&self) -> AnyInId {
        self.in_id
    }
}

impl<'a> TxOutHandle<'a> {
    pub fn id(&self) -> AnyOutId {
        self.out_id
    }

    fn output_data(&self) -> (bitcoin::Amount, crate::ScriptPubkeyHash) {
        self.index.tx_out_data(&self.out_id)
    }
}

impl<'a> AbstractTransaction for TxHandle<'a> {
    fn id(&self) -> AnyTxId {
        self.tx_id
    }

    fn inputs(&self) -> Box<dyn Iterator<Item = Box<dyn AbstractTxIn + '_>> + '_> {
        let input_ids = self.index.tx_in_ids(&self.tx_id);
        let inputs: Vec<_> = input_ids
            .into_iter()
            .map(|in_id| {
                Box::new(TxInHandle {
                    in_id,
                    index: self.index,
                }) as Box<dyn AbstractTxIn + '_>
            })
            .collect();
        Box::new(inputs.into_iter())
    }

    fn outputs(&self) -> Box<dyn Iterator<Item = Box<dyn AbstractTxOut + '_>> + '_> {
        let out_ids = self.index.tx_out_ids(&self.tx_id);
        let outputs: Vec<_> = out_ids
            .into_iter()
            .map(|out_id| {
                Box::new(TxOutHandle {
                    out_id,
                    index: self.index,
                }) as Box<dyn AbstractTxOut + '_>
            })
            .collect();
        Box::new(outputs.into_iter())
    }

    fn output_len(&self) -> usize {
        self.index.tx_out_ids(&self.tx_id).len()
    }

    fn output_at(&self, index: usize) -> Option<Box<dyn AbstractTxOut + '_>> {
        let out_ids = self.index.tx_out_ids(&self.tx_id);
        out_ids.get(index).copied().map(|out_id| {
            Box::new(TxOutHandle {
                out_id,
                index: self.index,
            }) as Box<dyn AbstractTxOut + '_>
        })
    }

    fn locktime(&self) -> u32 {
        self.index.locktime(&self.tx_id)
    }
}

impl<'a> AbstractTxIn for TxInHandle<'a> {
    fn prev_txid(&self) -> Option<AnyTxId> {
        self.index
            .prev_txout(&self.in_id)
            .map(|out_id| self.index.outpoint_for_out(&out_id).0)
    }

    fn prev_vout(&self) -> Option<u32> {
        self.index
            .prev_txout(&self.in_id)
            .map(|out_id| self.index.outpoint_for_out(&out_id).1)
    }

    fn prev_txout_id(&self) -> Option<AnyOutId> {
        self.index.prev_txout(&self.in_id)
    }
}

impl<'a> AbstractTxOut for TxOutHandle<'a> {
    fn id(&self) -> AnyOutId {
        self.out_id
    }

    fn value(&self) -> bitcoin::Amount {
        let (value, _spk_hash) = self.output_data();
        value
    }

    fn script_pubkey_hash(&self) -> crate::ScriptPubkeyHash {
        let (_value, spk_hash) = self.output_data();
        spk_hash
    }
}
