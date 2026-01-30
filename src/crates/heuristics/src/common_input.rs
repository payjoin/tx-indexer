use tx_indexer_primitives::{
    abstract_types::EnumerateSpentTxOuts,
    disjoint_set::{DisJointSet, SparseDisjointSet},
    loose::TxOutId,
};

pub struct MultiInputHeuristic;

impl MultiInputHeuristic {
    pub fn merge_prevouts(&self, tx: &impl EnumerateSpentTxOuts) -> SparseDisjointSet<TxOutId> {
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
    use tx_indexer_primitives::{
        disjoint_set::DisJointSet,
        loose::{TxId, TxOutId},
        test_utils::{DummyTxData, DummyTxOutData},
    };

    use super::MultiInputHeuristic;

    #[test]
    fn test_multi_input_heuristic_merge_prevouts() {
        let tx = DummyTxData {
            id: TxId(100),
            outputs: vec![
                DummyTxOutData::new_with_amount(500),
                DummyTxOutData::new_with_amount(300),
            ],
            spent_coins: vec![
                TxOutId::new(TxId(1), 0),
                TxOutId::new(TxId(2), 1),
                TxOutId::new(TxId(3), 0),
            ],
        };

        let heuristic = MultiInputHeuristic;
        let cluster = heuristic.merge_prevouts(&tx);

        // All three inputs should be in the same cluster
        let input1 = TxOutId::new(TxId(1), 0);
        let input2 = TxOutId::new(TxId(2), 1);
        let input3 = TxOutId::new(TxId(3), 0);

        assert_eq!(cluster.find(input1), cluster.find(input2));
        assert_eq!(cluster.find(input2), cluster.find(input3));
        assert_eq!(cluster.find(input1), cluster.find(input3));
    }
}
