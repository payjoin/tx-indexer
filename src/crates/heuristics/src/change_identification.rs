use tx_indexer_primitives::{
    abstract_types::{OutputCount, TxConstituent},
    disjoint_set::SparseDisjointSet,
    loose::{InMemoryClusteringIndex, TxHandle, TxOutId},
};

pub struct NaiveChangeIdentificationHueristic;

impl NaiveChangeIdentificationHueristic {
    // TODO: this method should be removed and replaced a with a loop over txs and the analysis operations we need to perform on each tx.
    pub fn classify_change(
        &self,
        tx: &TxHandle,
        in_memory_clustering_index: &mut InMemoryClusteringIndex<SparseDisjointSet<TxOutId>>,
    ) {
        for txout in tx.outputs() {
            in_memory_clustering_index
                .tagged_change_outputs
                .insert(txout.id(), self.is_change(txout));
        }
    }

    pub fn is_change(&self, txout: impl TxConstituent<Handle: OutputCount>) -> bool {
        let constituent_tx = txout.containing_tx();
        constituent_tx.output_count() - 1 == txout.index()
        // TODO: instead of the naive heuristic, simulate a strawman version of wallet fingerprint detection by looking at the spending tx txin
    }
}

#[cfg(test)]
mod tests {

    use tx_indexer_primitives::test_utils::{DummyTxHandle, DummyTxOut};

    use super::*;

    #[test]
    fn test_classify_change() {
        let heuristic = NaiveChangeIdentificationHueristic;
        let txout = DummyTxOut {
            index: 0,
            containing_tx: DummyTxHandle { output_count: 1 },
        };
        assert!(heuristic.is_change(txout));
    }
}
