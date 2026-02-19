use tx_indexer_disjoint_set::{DisJointSet, SparseDisjointSet};
use tx_indexer_primitives::abstract_types::{AbstractTransaction, EnumerateSpentTxOuts, IdFamily};

pub struct MultiInputHeuristic;

impl MultiInputHeuristic {
    pub fn merge_prevouts<E>(
        tx: &E,
    ) -> SparseDisjointSet<<<E as AbstractTransaction>::I as IdFamily>::TxOutId>
    where
        E: EnumerateSpentTxOuts,
    {
        let set = SparseDisjointSet::new();
        tx.spent_coins().reduce(|a, b| {
            set.union(a, b);
            a
        });
        set
    }
}

#[cfg(test)]
mod tests {
    use tx_indexer_disjoint_set::DisJointSet;
    use tx_indexer_primitives::{
        loose::{TxId, TxOutId},
        test_utils::{DummyTxData, DummyTxOutData},
    };

    use super::MultiInputHeuristic;

    #[test]
    fn test_multi_input_heuristic_merge_prevouts() {
        let tx = DummyTxData {
            id: TxId(100),
            outputs: vec![
                DummyTxOutData::new_with_amount(500, 0, TxId(100)),
                DummyTxOutData::new_with_amount(300, 1, TxId(100)),
            ],
            spent_coins: vec![
                TxOutId::new(TxId(1), 0),
                TxOutId::new(TxId(2), 1),
                TxOutId::new(TxId(3), 0),
            ],
            n_locktime: 0,
        };

        let cluster = MultiInputHeuristic::merge_prevouts(&tx);

        // All three inputs should be in the same cluster
        let input1 = TxOutId::new(TxId(1), 0);
        let input2 = TxOutId::new(TxId(2), 1);
        let input3 = TxOutId::new(TxId(3), 0);

        assert_eq!(cluster.find(input1), cluster.find(input2));
        assert_eq!(cluster.find(input2), cluster.find(input3));
        assert_eq!(cluster.find(input1), cluster.find(input3));
    }
}
