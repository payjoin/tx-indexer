pub mod abstract_types;
pub mod disjoint_set;
pub mod loose;

pub mod test_utils {
    use crate::abstract_types::{
        AbstractTxHandle, EnumerateOutputValueInArbitraryOrder, OutputCount, TxConstituent,
    };

    #[derive(Clone)]
    pub struct DummyTxHandle {
        pub output_count: usize,
    }

    impl AbstractTxHandle for DummyTxHandle {}

    impl OutputCount for DummyTxHandle {
        fn output_count(&self) -> usize {
            self.output_count
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
}
