use tx_indexer_primitives::{
    disjoint_set::{DisJointSet, SparseDisjointSet},
    loose::{TxId, TxOutId},
    storage::InMemoryClusteringIndex,
    test_utils::DummyIndex,
};

pub mod change_identification;
pub mod coinjoin_detection;
pub mod common_input;

// TODO: in the future we will want to express that some heuristics return more concrete types.
#[derive(Debug, PartialEq, Eq)]
pub enum MutableOperation {
    Cluster(TxOutId, TxOutId),
    AnnotateTx(TxId, bool),
    AnnotateChange(TxOutId, bool),
}

pub trait OperationExecutor {
    fn execute(&mut self, operation: &MutableOperation);
}

impl OperationExecutor for InMemoryClusteringIndex<SparseDisjointSet<TxOutId>> {
    fn execute(&mut self, operation: &MutableOperation) {
        match operation {
            MutableOperation::Cluster(a, b) => {
                self.union(a, b);
            }
            MutableOperation::AnnotateTx(tx_id, is_coinjoin) => {
                self.annotate_coinjoin(tx_id, *is_coinjoin);
            }
            MutableOperation::AnnotateChange(tx_out_id, is_change) => {
                self.annotate_change(tx_out_id, *is_change);
            }
        }
    }
}

// TODO: should be configured away as test
impl OperationExecutor for DummyIndex {
    fn execute(&mut self, operation: &MutableOperation) {
        match operation {
            MutableOperation::Cluster(a, b) => {
                self.clustered_txouts.union(*a, *b);
            }
            MutableOperation::AnnotateTx(tx_id, is_coinjoin) => {
                self.coinjoin_tags.insert(*tx_id, *is_coinjoin);
            }
            MutableOperation::AnnotateChange(tx_out_id, is_change) => {
                self.change_tags.insert(*tx_out_id, *is_change);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use tx_indexer_primitives::{
        disjoint_set::DisJointSet,
        loose::{TxId, TxOutId},
        pass::{AnalysisPass, FnMapPass, FnMapToBoolMapPass, FnMergePass, PredicateFilterPass},
        test_utils::{DummyIndex, DummyTxData},
    };

    use crate::{
        change_identification::change_identification_map_pass_fn,
        coinjoin_detection::coinjoin_detection_filter_pass_fn,
        common_input::common_input_map_pass_fn,
    };

    #[test]
    fn test_filtered() {
        let src = vec![
            // Coinbase 1
            DummyTxData {
                id: TxId(0),
                outputs_amounts: vec![100, 200, 300],
                spent_coins: vec![],
            },
            // Coinbase 2
            DummyTxData {
                id: TxId(1),
                outputs_amounts: vec![100, 100],
                spent_coins: vec![],
            },
            // Non-coinjoin spending the two coinbases
            DummyTxData {
                id: TxId(2),
                // Creating a change output and a payment output
                outputs_amounts: vec![100, 200],
                // Spending the vout = 0 of the two coinbases
                spent_coins: vec![TxOutId::new(TxId(0), 0), TxOutId::new(TxId(1), 0)],
            },
            // Spending the change output
            DummyTxData {
                id: TxId(3),
                outputs_amounts: vec![100],
                // Spending the change output
                spent_coins: vec![TxOutId::new(TxId(2), 0)],
            },
            // Spending the payment output
            DummyTxData {
                id: TxId(4),
                outputs_amounts: vec![200],
                // Spending the payment output
                spent_coins: vec![TxOutId::new(TxId(2), 1)],
            },
        ];

        let mut index = DummyIndex::default();

        for tx in src.iter() {
            index.txs.insert(tx.id, tx.clone());
        }

        let change_identification_map = index
            .clone()
            .map(FnMapToBoolMapPass::new(change_identification_map_pass_fn));

        let mut analysis = index
            .clone()
            // First filter out coinjoins
            .filter(PredicateFilterPass::new(coinjoin_detection_filter_pass_fn))
            // Then merge prevouts according to the MultiInputHeuristic
            .map(FnMapPass::new(common_input_map_pass_fn))
            // Then merge the change identification map with the prevouts
            // Effectively clustering the change output with the spending txouts
            .merge(
                change_identification_map,
                FnMergePass::new(
                    |txout_id: &TxOutId, index: &DummyIndex| {
                        if let Some(tx) = index.txs.get(&txout_id.txid) {
                            tx.spent_coins
                                .iter()
                                .next()
                                .map(|spent_txout| (*txout_id, *spent_txout))
                        } else {
                            None
                        }
                    },
                    &index,
                ),
            )
            .output();

        let change_txout = TxOutId::new(TxId(2), 1);
        let coinbase_txout0 = TxOutId::new(TxId(0), 0);
        let coinbase_txout1 = TxOutId::new(TxId(1), 0);
        assert_eq!(analysis.find(change_txout), analysis.find(coinbase_txout0));
        assert_eq!(analysis.find(change_txout), analysis.find(coinbase_txout1));

        let other_cluster_txout = TxOutId::new(TxId(2), 0);
        assert_ne!(
            analysis.find(change_txout),
            analysis.find(other_cluster_txout)
        );
    }
}
