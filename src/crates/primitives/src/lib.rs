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

    #[derive(Debug, Clone)]
    pub struct DummyTxData {
        pub id: TxId,
        pub outputs_amounts: Vec<u64>,
        pub spent_coins: Vec<TxOutId>,
    }

    impl AbstractTxHandle for DummyTxData {
        fn id(&self) -> TxId {
            self.id
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

        fn index(&self) -> usize {
            self.index
        }
    }

    #[derive(Default)]
    pub struct DummyIndex {
        pub coinjoin_tags: HashMap<TxId, bool>,
        pub change_tags: HashMap<TxOutId, bool>,
        pub clustered_txouts: SparseDisjointSet<TxOutId>,
        pub txs: HashMap<TxId, DummyTxData>,
    }
}
