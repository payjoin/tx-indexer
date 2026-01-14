pub mod abstract_types;
pub mod disjoint_set;
pub mod loose;

pub mod test_utils {
    use std::collections::HashMap;

    use bitcoin::Amount;

    use crate::{
        abstract_types::{
            AbstractTxHandle, EnumerateOutputValueInArbitraryOrder, EnumerateSpentTxOuts,
            OutputCount, TxConstituent,
        },
        disjoint_set::SparseDisjointSet,
        loose::{TxId, TxOutId},
    };

    #[derive(Clone)]
    pub struct DummyTxHandle {
        pub id: TxId,
        pub outputs: Vec<u64>,
        pub spent_coins: Vec<TxOutId>,
    }

    impl AbstractTxHandle for DummyTxHandle {
        fn id(&self) -> TxId {
            self.id
        }
    }

    impl OutputCount for DummyTxHandle {
        fn output_count(&self) -> usize {
            self.outputs.len()
        }
    }

    impl EnumerateOutputValueInArbitraryOrder for DummyTxHandle {
        fn output_values(&self) -> impl Iterator<Item = Amount> {
            self.outputs.iter().map(|amount| Amount::from_sat(*amount))
        }
    }

    impl EnumerateSpentTxOuts for DummyTxHandle {
        fn spent_coins(&self) -> impl Iterator<Item = TxOutId> {
            self.spent_coins.iter().copied()
        }
    }

    pub struct DummyTxOut {
        pub index: usize,
        pub containing_tx: DummyTxHandle,
    }

    impl TxConstituent for DummyTxOut {
        type Handle = DummyTxHandle;
        fn containing_tx(&self) -> Self::Handle {
            self.containing_tx.clone()
        }

        fn index(&self) -> usize {
            self.index
        }
    }

    #[derive(Default)]
    pub struct DummyIndex {
        pub coinjoin_tags: HashMap<TxId, bool>,
        pub change_tags: HashMap<TxOutId, bool>,
        pub clustered_txouts: SparseDisjointSet<TxOutId>,
    }
}
