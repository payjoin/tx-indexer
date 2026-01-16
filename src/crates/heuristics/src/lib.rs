use tx_indexer_primitives::{
    disjoint_set::{DisJointSet, SparseDisjointSet},
    loose::{InMemoryClusteringIndex, TxId, TxOutId},
    test_utils::DummyIndex,
};

pub mod change_identification;
pub mod coinjoin_detection;
pub mod common_input;
pub mod pass;

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
