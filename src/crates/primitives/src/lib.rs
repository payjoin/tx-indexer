pub mod abstract_types;
pub mod datalog;
pub mod disjoint_set;
pub mod loose;
pub mod pass;
pub mod storage;

pub type ScriptPubkeyHash = [u8; 20];

pub mod test_utils {

    use bitcoin::Amount;

    use crate::{
        ScriptPubkeyHash,
        abstract_types::{
            AbstractTransaction, AbstractTxIn, AbstractTxOut, AbstractTxWrapper,
            EnumerateOutputValueInArbitraryOrder, EnumerateSpentTxOuts, OutputCount, TxConstituent,
        },
        loose::{TxId, TxOutId},
    };

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    pub struct DummyTxData {
        pub id: TxId,
        pub outputs: Vec<DummyTxOutData>,
        /// The outputs that are spent by this transaction
        pub spent_coins: Vec<TxOutId>,
    }

    impl Into<AbstractTxWrapper> for DummyTxData {
        fn into(self) -> AbstractTxWrapper {
            AbstractTxWrapper::new(Box::new(self))
        }
    }

    // Wrapper types for implementing abstract traits on dummy types
    struct DummyTxInWrapper {
        prev_txid: TxId,
        prev_vout: u32,
    }

    impl AbstractTxIn for DummyTxInWrapper {
        fn prev_txid(&self) -> TxId {
            self.prev_txid
        }

        fn prev_vout(&self) -> u32 {
            self.prev_vout
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
        fn txid(&self) -> TxId {
            self.id
        }

        fn inputs(&self) -> Box<dyn Iterator<Item = Box<dyn AbstractTxIn>> + '_> {
            // Collect into a vector to avoid lifetime issues
            let inputs: Vec<Box<dyn AbstractTxIn>> = self
                .spent_coins
                .iter()
                .map(|spent| {
                    Box::new(DummyTxInWrapper {
                        prev_txid: spent.txid,
                        prev_vout: spent.vout,
                    }) as Box<dyn AbstractTxIn>
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

    impl From<DummyTxData> for Box<dyn AbstractTransaction + Send + Sync> {
        fn from(val: DummyTxData) -> Self {
            Box::new(val)
        }
    }

    impl DummyTxData {
        /// Convert DummyTxData to a boxed AbstractTransaction
        pub fn into_abstract_tx(self) -> Box<dyn AbstractTransaction + Send + Sync> {
            self.into()
        }
    }

    pub struct DummyTxOut {
        pub vout: usize,
        pub containing_tx: DummyTxData,
    }

    impl DummyTxOut {
        pub fn id(&self) -> TxOutId {
            self.containing_tx.id().txout_id(self.vout as u32)
        }
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
