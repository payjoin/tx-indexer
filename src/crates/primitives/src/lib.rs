pub mod abstract_types;
pub mod datalog;
pub mod disjoint_set;
pub mod loose;
pub mod pass;
pub mod storage;

pub mod test_utils {

    use bitcoin::Amount;

    use crate::{
        abstract_types::{
            AbstractTransaction, AbstractTxIn, AbstractTxOut, EnumerateOutputValueInArbitraryOrder,
            EnumerateSpentTxOuts, OutputCount, TxConstituent,
        },
        loose::{TxId, TxOutId},
    };

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    pub struct DummyTxData {
        pub id: TxId,
        /// Amounts of the outputs in satoshis
        pub outputs_amounts: Vec<u64>,
        /// The outputs that are spent by this transaction
        pub spent_coins: Vec<TxOutId>,
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

    struct DummyTxOutWrapper {
        value: Amount,
    }

    impl AbstractTxOut for DummyTxOutWrapper {
        fn value(&self) -> Amount {
            self.value
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
                .outputs_amounts
                .iter()
                .map(|amount| {
                    Box::new(DummyTxOutWrapper {
                        value: Amount::from_sat(*amount),
                    }) as Box<dyn AbstractTxOut>
                })
                .collect();
            Box::new(outputs.into_iter())
        }

        fn output_len(&self) -> usize {
            self.outputs_amounts.len()
        }

        fn output_at(&self, index: usize) -> Option<Box<dyn AbstractTxOut>> {
            self.outputs_amounts.get(index).map(|amount| {
                Box::new(DummyTxOutWrapper {
                    value: Amount::from_sat(*amount),
                }) as Box<dyn AbstractTxOut>
            })
        }
    }

    impl OutputCount for DummyTxData {
        fn output_count(&self) -> usize {
            self.outputs_amounts.len()
        }
    }

    impl EnumerateOutputValueInArbitraryOrder for DummyTxData {
        fn output_values(&self) -> impl Iterator<Item = Amount> {
            self.outputs_amounts
                .iter()
                .map(|amount| Amount::from_sat(*amount))
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
        pub index: usize,
        pub containing_tx: DummyTxData,
    }

    impl DummyTxOut {
        pub fn id(&self) -> TxOutId {
            self.containing_tx.id().txout_id(self.index as u32)
        }
    }

    impl TxConstituent for DummyTxOut {
        type Handle = DummyTxData;
        fn containing_tx(&self) -> Self::Handle {
            self.containing_tx.clone()
        }

        fn vout(&self) -> usize {
            self.index
        }
    }
}
