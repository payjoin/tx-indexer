use crate::{
    AnyInId, AnyOutId, AnyTxId, HasWitnessData,
    traits::{
        abstract_types::{
            AbstractTransaction, AbstractTxIn, AbstractTxOut, EnumerateInputValueInArbitraryOrder,
            EnumerateOutputValueInArbitraryOrder, EnumerateSpentTxOuts, HasNLockTime,
            HasScriptPubkey, HasSequence, InputCount, OutputCount, TxConstituent,
        },
        graph_index::IndexedGraph,
    },
};

/// Handle for a transaction in a unified index.
pub struct TxHandle<'a> {
    pub(crate) tx_id: AnyTxId,
    pub(crate) index: &'a dyn IndexedGraph,
    // TODO: in the dense case we could cache the tx data
}

impl<'a> TxHandle<'a> {
    pub fn id(&self) -> AnyTxId {
        self.tx_id
    }

    pub fn output_at(&self, index: usize) -> TxOutHandle<'a> {
        TxOutHandle {
            out_id: self
                .index
                .tx_out_ids(&self.tx_id)
                .get(index)
                .copied()
                .unwrap(),
            index: self.index,
        }
    }

    pub fn inputs(&self) -> impl Iterator<Item = TxInHandle<'a>> {
        self.index
            .tx_in_ids(&self.tx_id)
            .into_iter()
            .map(|in_id| TxInHandle {
                in_id,
                index: self.index,
            })
    }

    pub fn outputs(&self) -> impl Iterator<Item = TxOutHandle<'a>> {
        self.index
            .tx_out_ids(&self.tx_id)
            .into_iter()
            .map(|out_id| TxOutHandle {
                out_id,
                index: self.index,
            })
    }

    pub fn block_height(&self) -> Option<u64> {
        self.index.block_height(&self.tx_id)
    }
}

/// Handle for a transaction output in a unified index.
pub struct TxOutHandle<'a> {
    pub(crate) out_id: AnyOutId,
    pub(crate) index: &'a dyn IndexedGraph,
}

impl<'a> TxOutHandle<'a> {
    pub fn id(&self) -> AnyOutId {
        self.out_id
    }

    pub fn txid(&self) -> AnyTxId {
        self.index.outpoint_for_out(&self.out_id).0
    }

    pub fn vout(&self) -> u32 {
        self.index.outpoint_for_out(&self.out_id).1
    }

    pub fn containing_tx(&self) -> TxHandle<'a> {
        TxHandle {
            tx_id: self.txid(),
            index: self.index,
        }
    }

    pub fn spender_txin_id(&self) -> Option<AnyInId> {
        self.index.spending_txin(&self.out_id)
    }

    pub fn spender_txin(&self) -> Option<TxInHandle<'a>> {
        self.spender_txin_id().map(|in_id| TxInHandle {
            in_id,
            index: self.index,
        })
    }

    pub fn first_with_same_spk(&self) -> Option<TxOutHandle<'a>> {
        let spk_hash = self.script_pubkey_hash();
        self.index
            .script_pubkey_to_txout_id(&spk_hash)
            .map(|out_id| TxOutHandle {
                out_id,
                index: self.index,
            })
    }

    pub fn value(&self) -> bitcoin::Amount {
        self.index.value(&self.out_id)
    }

    pub fn script_pubkey_hash(&self) -> crate::ScriptPubkeyHash {
        self.index.script_pubkey_hash(&self.out_id)
    }
}

impl<'a> HasScriptPubkey for TxOutHandle<'a> {
    fn script_pubkey_bytes(&self) -> Vec<u8> {
        self.index.script_pubkey_bytes(&self.out_id)
    }
}

/// Handle for a transaction input in a unified index.
pub struct TxInHandle<'a> {
    pub(crate) in_id: AnyInId,
    pub(crate) index: &'a dyn IndexedGraph,
}

impl<'a> TxInHandle<'a> {
    pub fn id(&self) -> AnyInId {
        self.in_id
    }

    pub fn txid(&self) -> AnyTxId {
        self.index.txid_for_in(&self.in_id)
    }

    pub fn containing_tx(&self) -> TxHandle<'a> {
        TxHandle {
            tx_id: self.txid(),
            index: self.index,
        }
    }
}

impl<'a> HasSequence for TxInHandle<'a> {
    fn sequence(&self) -> u32 {
        self.index.input_sequence(&self.in_id)
    }
}

impl<'a> HasWitnessData for TxInHandle<'a> {
    fn witness_items(&self) -> Vec<Vec<u8>> {
        self.index.witness_items(&self.in_id)
    }

    fn script_sig_bytes(&self) -> Vec<u8> {
        self.index.script_sig_bytes(&self.in_id)
    }
}

impl<'a> EnumerateSpentTxOuts for TxHandle<'a> {
    fn spent_coins(&self) -> impl Iterator<Item = AnyOutId> {
        self.inputs().filter_map(|input| input.prev_txout_id())
    }
}

impl<'a> EnumerateOutputValueInArbitraryOrder for TxHandle<'a> {
    fn output_values(&self) -> impl Iterator<Item = bitcoin::Amount> {
        self.outputs().map(|output| output.value())
    }
}

impl<'a> AbstractTransaction for TxHandle<'a> {
    fn input_len(&self) -> usize {
        self.index.tx_in_ids(&self.tx_id).len()
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

    fn is_coinbase(&self) -> bool {
        let mut inputs = self.inputs();
        if let Some(first_input) = inputs.next() {
            return first_input.prev_txout_id().is_none();
        }

        false
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
    fn value(&self) -> bitcoin::Amount {
        TxOutHandle::value(self)
    }

    fn script_pubkey_hash(&self) -> crate::ScriptPubkeyHash {
        TxOutHandle::script_pubkey_hash(self)
    }

    fn script_pubkey_bytes(&self) -> Vec<u8> {
        HasScriptPubkey::script_pubkey_bytes(self)
    }
}

impl<'a> EnumerateInputValueInArbitraryOrder for TxHandle<'a> {
    fn input_values(&self) -> impl Iterator<Item = bitcoin::Amount> {
        self.inputs().filter_map(|input| {
            input
                .prev_txout_id()
                .map(|out_id| out_id.with(self.index).value())
        })
    }
}

impl<'a> OutputCount for TxHandle<'a> {
    fn output_count(&self) -> usize {
        self.output_len()
    }
}

impl<'a> InputCount for TxHandle<'a> {
    fn input_count(&self) -> usize {
        self.input_len()
    }
}

impl<'a> HasNLockTime for TxHandle<'a> {
    fn n_locktime(&self) -> u32 {
        self.locktime()
    }
}

impl<'a> TxConstituent for TxOutHandle<'a> {
    type Handle = TxHandle<'a>;

    fn containing_tx(&self) -> Self::Handle {
        TxOutHandle::containing_tx(self)
    }

    fn vout(&self) -> usize {
        self.vout() as usize
    }
}
