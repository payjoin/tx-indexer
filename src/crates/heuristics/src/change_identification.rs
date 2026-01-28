use std::any::TypeId;

use tx_indexer_primitives::{
    abstract_types::{EnumerateSpentTxOuts, OutputCount, TxConstituent},
    datalog::{
        ChangeIdentificationRel, ClusterRel, GlobalClusteringRel, Rule, TxOutAnnotation, TxRel,
    },
    disjoint_set::{DisJointSet, SparseDisjointSet},
    loose::{TxHandle, TxOutId},
    storage::{FactStore, MemStore},
};

pub struct NaiveChangeIdentificationHueristic;

impl NaiveChangeIdentificationHueristic {
    pub fn is_change(txout: impl TxConstituent<Handle: OutputCount>) -> TxOutAnnotation {
        let constituent_tx = txout.containing_tx();
        if constituent_tx.output_count() - 1 == txout.vout() {
            TxOutAnnotation::Change
        } else {
            TxOutAnnotation::NotChange
        }
    }
}

pub struct ChangeIdentificationRule;

impl ChangeIdentificationRule {
    fn analyze_change_clustering(
        &self,
        tx_handle: TxHandle<'_>,
        change_outputs: &[TxOutId],
    ) -> Option<SparseDisjointSet<TxOutId>> {
        if tx_handle.is_coinbase() {
            return None;
        }

        // Check if all inputs are clustered together
        if !tx_handle.inputs_are_clustered() {
            return None;
        }

        if change_outputs.is_empty() {
            return None;
        }

        let root = tx_handle
            .spent_coins()
            .next()
            .expect("to have one spent coin");

        let set = SparseDisjointSet::<TxOutId>::new();
        // Union each change output with the clustered inputs
        for change_output in change_outputs {
            set.union(*change_output, root);
        }

        Some(set)
    }
}

impl Rule for ChangeIdentificationRule {
    type Input = tx_indexer_primitives::datalog::TransactionInput;

    fn name(&self) -> &'static str {
        "change_identification"
    }

    fn inputs(&self) -> &'static [TypeId] {
        const INS: &[TypeId] = &[TypeId::of::<TxRel>(), TypeId::of::<GlobalClusteringRel>()];
        INS
    }

    fn step(&mut self, input: Self::Input, store: &mut MemStore) -> usize {
        // The engine has already computed all affected transaction IDs from:
        // - TxRel: newly ingested transactions
        // - GlobalClusteringRel: transactions containing or spending txouts in updated clusters

        // Collect all change annotations first to avoid borrowing headaches
        let mut change_annotations = Vec::new();
        let mut clustering_sets = Vec::new();

        for tx_id in input.iter() {
            let tx_handle = tx_id.with(store.index());

            // Skip coinbases - they don't have "change"
            if tx_handle.is_coinbase() {
                continue;
            }

            // Collect change identification annotations and identify change outputs
            let mut change_outputs = Vec::new();
            for txout_handle in tx_handle.outputs() {
                let txout_id = txout_handle.id();
                let is_change = NaiveChangeIdentificationHueristic::is_change(txout_handle);
                change_annotations.push((txout_id, is_change));
                if is_change == TxOutAnnotation::Change {
                    change_outputs.push(txout_id);
                }
            }

            // Collect clustering sets if inputs are clustered
            if let Some(set) = self.analyze_change_clustering(tx_handle, &change_outputs) {
                clustering_sets.push(set);
            }
        }

        // Save counts before moving the vectors
        let change_count = change_annotations.len();
        let clustering_count = clustering_sets.len();

        // Now insert all results
        for (txout_id, is_change) in change_annotations {
            store.insert::<ChangeIdentificationRel>((txout_id, is_change));
        }

        for set in clustering_sets {
            store.insert::<ClusterRel>(set);
        }

        change_count + clustering_count
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, sync::Arc};
    use tx_indexer_primitives::{
        datalog::TransactionInput,
        loose::{TxId, TxOutId},
        storage::{FactStore, InMemoryIndex, MemStore},
        test_utils::{DummyTxData, DummyTxOutData},
    };

    use super::*;

    fn setup_store(index: InMemoryIndex) -> MemStore {
        let mut store = MemStore::new(index);
        store.initialize::<TxRel>();
        store.initialize::<GlobalClusteringRel>();
        store.initialize::<ChangeIdentificationRel>();
        store.initialize::<ClusterRel>();
        store
    }

    #[test]
    fn test_classify_change() {
        let txout = tx_indexer_primitives::test_utils::DummyTxOut {
            vout: 0,
            containing_tx: DummyTxData {
                id: TxId(1),
                outputs: vec![DummyTxOutData::new_with_amount(100)],
                spent_coins: vec![],
            },
        };
        assert_eq!(
            NaiveChangeIdentificationHueristic::is_change(txout),
            TxOutAnnotation::Change
        );
    }

    #[test]
    fn test_change_identification_annotates_outputs() {
        let mut index = InMemoryIndex::new();
        let prev_tx = DummyTxData {
            id: TxId(0),
            outputs: vec![DummyTxOutData::new_with_amount(500)],
            spent_coins: vec![],
        };
        let tx = DummyTxData {
            id: TxId(1),
            outputs: vec![
                DummyTxOutData::new_with_amount(100),
                DummyTxOutData::new_with_amount(200),
            ],
            spent_coins: vec![TxOutId::new(TxId(0), 0)],
        };
        index.add_tx(Arc::new(prev_tx));
        index.add_tx(Arc::new(tx.clone()));

        let mut store = setup_store(index);
        store.insert::<TxRel>(TxId(1));

        let input = TransactionInput::new(HashSet::from([TxId(1)]));

        let mut rule = ChangeIdentificationRule;
        rule.step(input, &mut store);

        assert!(store.contains::<ChangeIdentificationRel>(&(
            TxOutId::new(TxId(1), 0),
            TxOutAnnotation::NotChange
        )));
        assert!(store.contains::<ChangeIdentificationRel>(&(
            TxOutId::new(TxId(1), 1),
            TxOutAnnotation::Change
        )));
    }

    #[test]
    fn test_skips_coinbase() {
        let mut index = InMemoryIndex::new();
        let tx = DummyTxData {
            id: TxId(1),
            outputs: vec![
                DummyTxOutData::new_with_amount(100),
                DummyTxOutData::new_with_amount(200),
            ],
            spent_coins: vec![],
        };
        index.add_tx(Arc::new(tx.clone()));

        let mut store = setup_store(index);
        store.insert::<TxRel>(TxId(1));

        let input = TransactionInput::new(HashSet::from([TxId(1)]));

        let mut rule = ChangeIdentificationRule;
        rule.step(input, &mut store);
    }

    #[test]
    fn test_clusters_change_with_inputs() {
        let mut index = InMemoryIndex::new();
        let prev_tx = DummyTxData {
            id: TxId(0),
            outputs: vec![
                DummyTxOutData::new_with_amount(500),
                DummyTxOutData::new_with_amount(500),
            ],
            spent_coins: vec![],
        };
        let input1 = TxOutId::new(TxId(0), 0);
        let input2 = TxOutId::new(TxId(0), 1);
        let tx = DummyTxData {
            id: TxId(1),
            outputs: vec![
                DummyTxOutData::new_with_amount(100),
                DummyTxOutData::new_with_amount(200),
            ],
            spent_coins: vec![input1, input2],
        };
        index.add_tx(Arc::new(prev_tx));
        index.add_tx(Arc::new(tx.clone()));
        index.global_clustering.union(input1, input2);

        let mut store = setup_store(index);
        store.insert::<TxRel>(TxId(1));

        let input = TransactionInput::new(HashSet::from([TxId(1)]));

        let mut rule = ChangeIdentificationRule;
        rule.step(input, &mut store);

        assert!(store.contains::<ChangeIdentificationRel>(&(
            TxOutId::new(TxId(1), 1),
            TxOutAnnotation::Change
        )));

        let clusters: Vec<_> = store.read_range::<ClusterRel>(0, store.len::<ClusterRel>());
        assert!(clusters.len() == 1);
        let cluster = &clusters[0];
        // Note: only clustered with the first input. When global clustering is applied this will be joined with the other input.
        assert_eq!(cluster.find(TxOutId::new(TxId(1), 1)), cluster.find(input1));
    }

    #[test]
    fn test_no_clustering_when_inputs_not_clustered() {
        let mut index = InMemoryIndex::new();
        let prev_tx = DummyTxData {
            id: TxId(0),
            outputs: vec![
                DummyTxOutData::new_with_amount(500),
                DummyTxOutData::new_with_amount(500),
            ],
            spent_coins: vec![],
        };
        let input1 = TxOutId::new(TxId(0), 0);
        let input2 = TxOutId::new(TxId(0), 1);
        let tx = DummyTxData {
            id: TxId(1),
            outputs: vec![
                DummyTxOutData::new_with_amount(100),
                DummyTxOutData::new_with_amount(200),
            ],
            spent_coins: vec![input1, input2],
        };
        index.add_tx(Arc::new(prev_tx));
        index.add_tx(Arc::new(tx.clone()));

        let mut store = setup_store(index);
        store.insert::<TxRel>(TxId(1));

        let input = TransactionInput::new(HashSet::from([TxId(1)]));

        let mut rule = ChangeIdentificationRule;
        rule.step(input, &mut store);

        assert!(store.contains::<ChangeIdentificationRel>(&(
            TxOutId::new(TxId(1), 1),
            TxOutAnnotation::Change
        )));

        let clusters: Vec<_> = store.read_range::<ClusterRel>(0, store.len::<ClusterRel>());
        assert_eq!(clusters.len(), 0);
    }

    #[test]
    fn test_processes_multiple_transactions() {
        let mut index = InMemoryIndex::new();
        let prev_tx = DummyTxData {
            id: TxId(0),
            outputs: vec![
                DummyTxOutData::new_with_amount(500),
                DummyTxOutData::new_with_amount(500),
            ],
            spent_coins: vec![],
        };
        let tx1 = DummyTxData {
            id: TxId(1),
            outputs: vec![
                DummyTxOutData::new_with_amount(100),
                DummyTxOutData::new_with_amount(200),
            ],
            spent_coins: vec![TxOutId::new(TxId(0), 0)],
        };
        let tx2 = DummyTxData {
            id: TxId(2),
            outputs: vec![DummyTxOutData::new_with_amount(300)],
            spent_coins: vec![TxOutId::new(TxId(0), 1)],
        };
        index.add_tx(Arc::new(prev_tx));
        index.add_tx(Arc::new(tx1.clone()));
        index.add_tx(Arc::new(tx2.clone()));

        let mut store = setup_store(index);
        store.insert::<TxRel>(TxId(1));
        store.insert::<TxRel>(TxId(2));

        let input = TransactionInput::new(HashSet::from([TxId(1), TxId(2)]));

        let mut rule = ChangeIdentificationRule;
        rule.step(input, &mut store);

        assert!(store.contains::<ChangeIdentificationRel>(&(
            TxOutId::new(TxId(1), 0),
            TxOutAnnotation::NotChange
        )));
        assert!(store.contains::<ChangeIdentificationRel>(&(
            TxOutId::new(TxId(1), 1),
            TxOutAnnotation::Change
        )));
        assert!(store.contains::<ChangeIdentificationRel>(&(
            TxOutId::new(TxId(2), 0),
            TxOutAnnotation::Change
        )));
    }
}
