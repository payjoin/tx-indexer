pub mod abstract_fingerprints;
pub mod abstract_types;
pub mod dense;
pub mod disjoint_set;
pub mod graph_index;
pub mod loose;

pub type ScriptPubkeyHash = [u8; 20];

// TODO: should be configured for testing only
pub mod test_utils {

    use bitcoin::Amount;

    use crate::{
        ScriptPubkeyHash,
        abstract_fingerprints::HasNLockTime,
        abstract_types::{
            AbstractTransaction, AbstractTxIn, AbstractTxOut, EnumerateOutputValueInArbitraryOrder,
            EnumerateSpentTxOuts, LooseIds, OutputCount, TxConstituent,
        },
        loose::{TxId, TxInId, TxOutId},
    };

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    pub struct DummyTxData {
        pub id: TxId,
        pub outputs: Vec<DummyTxOutData>,
        /// The outputs that are spent by this transaction
        pub spent_coins: Vec<TxOutId>,
        pub n_locktime: u32,
    }

    impl DummyTxData {
        pub fn new(
            id: TxId,
            outputs: Vec<DummyTxOutData>,
            spent_coins: Vec<TxOutId>,
            n_locktime: u32,
        ) -> Self {
            Self {
                id,
                outputs,
                spent_coins,
                n_locktime,
            }
        }
    }

    impl HasNLockTime for DummyTxData {
        fn n_locktime(&self) -> u32 {
            self.n_locktime
        }
    }

    // Wrapper types for implementing abstract traits on dummy types
    struct DummyTxInWrapper {
        prev_txid: TxId,
        prev_vout: u32,
    }

    impl AbstractTxIn for DummyTxInWrapper {
        type I = LooseIds;
        fn prev_txid(&self) -> Self::I::TxId {
            self.prev_txid
        }

        fn prev_vout(&self) -> u32 {
            self.prev_vout
        }

        fn prev_txout_id(&self) -> TxOutId {
            TxOutId::new(self.prev_txid, self.prev_vout)
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    pub struct DummyTxOutData {
        value: u64,
        spk_hash: ScriptPubkeyHash,
    }

    impl DummyTxOutData {
        pub fn new(value: u64, spk_hash: ScriptPubkeyHash) -> Self {
            Self { value, spk_hash }
        }

        /// Create a new DummyTxOutData with a given amount and a dummy spk hash
        pub fn new_with_amount(amount: u64) -> Self {
            Self {
                value: amount,
                spk_hash: [0u8; 20],
            }
        }
    }

    impl AbstractTxOut for DummyTxOutData {
        fn value(&self) -> Amount {
            Amount::from_sat(self.value)
        }

        fn script_pubkey_hash(&self) -> ScriptPubkeyHash {
            self.spk_hash
        }
    }

    impl AbstractTransaction for DummyTxData {
        type I = LooseIds;
        fn id(&self) -> Self::I::TxId {
            self.id
        }

        fn inputs(&self) -> Box<dyn Iterator<Item = Box<dyn AbstractTxIn<I = Self::I>>> + '_> {
            // Collect into a vector to avoid lifetime issues
            let inputs: Vec<Box<dyn AbstractTxIn<I = Self::I>>> = self
                .spent_coins
                .iter()
                .map(|spent| {
                    Box::new(DummyTxInWrapper {
                        prev_txid: spent.txid(),
                        prev_vout: spent.vout(),
                    }) as Box<dyn AbstractTxIn<I = Self::I>>
                })
                .collect();
            Box::new(inputs.into_iter())
        }

        fn outputs(&self) -> Box<dyn Iterator<Item = Box<dyn AbstractTxOut>> + '_> {
            // Collect into a vector to avoid lifetime issues
            let outputs: Vec<Box<dyn AbstractTxOut>> = self
                .outputs
                .iter()
                .map(|output| Box::new(output.clone()) as Box<dyn AbstractTxOut>)
                .collect();
            Box::new(outputs.into_iter())
        }

        fn output_len(&self) -> usize {
            self.outputs.len()
        }

        fn output_at(&self, index: usize) -> Option<Box<dyn AbstractTxOut>> {
            self.outputs
                .get(index)
                .map(|output| Box::new(output.clone()) as Box<dyn AbstractTxOut>)
        }

        fn locktime(&self) -> u32 {
            self.n_locktime
        }
    }

    impl OutputCount for DummyTxData {
        fn output_count(&self) -> usize {
            self.outputs.len()
        }
    }

    impl EnumerateOutputValueInArbitraryOrder for DummyTxData {
        fn output_values(&self) -> impl Iterator<Item = Amount> {
            self.outputs.iter().map(|output| output.value())
        }
    }

    impl EnumerateSpentTxOuts for DummyTxData {
        fn spent_coins(&self) -> impl Iterator<Item = TxOutId> {
            self.spent_coins.iter().copied()
        }
    }

    impl From<DummyTxData>
        for Box<
            dyn AbstractTransaction<TxId = TxId, TxOutId = TxOutId, TxInId = TxInId> + Send + Sync,
        >
    {
        fn from(val: DummyTxData) -> Self {
            Box::new(val)
        }
    }

    pub struct DummyTxOut {
        pub vout: usize,
        pub containing_tx: DummyTxData,
    }

    impl TxConstituent for DummyTxOut {
        type Handle = DummyTxData;
        fn containing_tx(&self) -> Self::Handle {
            self.containing_tx.clone()
        }

        fn vout(&self) -> usize {
            self.vout
        }
    }
}
