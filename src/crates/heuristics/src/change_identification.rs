use std::{any::TypeId, collections::HashSet};

use tx_indexer_primitives::{
    abstract_types::{EnumerateSpentTxOuts, OutputCount, TxConstituent},
    datalog::{ChangeIdentificationRel, ClusterRel, GlobalClusteringRel, Rule, TxRel},
    disjoint_set::{DisJointSet, SparseDisjointSet},
    loose::{ClusterHandle, TxHandle, TxId, TxOutId},
    storage::{FactStore, MemStore},
};

#[derive(Debug, PartialEq, Eq)]
pub enum ChangeIdentificationResult {
    Change,
    NotChange,
}

pub struct NaiveChangeIdentificationHueristic;

impl NaiveChangeIdentificationHueristic {
    pub fn is_change(txout: impl TxConstituent<Handle: OutputCount>) -> ChangeIdentificationResult {
        let constituent_tx = txout.containing_tx();
        if constituent_tx.output_count() - 1 == txout.vout() {
            ChangeIdentificationResult::Change
        } else {
            ChangeIdentificationResult::NotChange
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
    type Input = tx_indexer_primitives::datalog::TxOutInput;

    fn name(&self) -> &'static str {
        "change_identification"
    }

    fn inputs(&self) -> &'static [TypeId] {
        const INS: &[TypeId] = &[TypeId::of::<TxRel>(), TypeId::of::<GlobalClusteringRel>()];
        INS
    }

    fn step(&mut self, input: Self::Input, store: &mut MemStore) -> usize {
        // Transactions that have been affected by clustering or they were just ingested
        let mut affected_txs = HashSet::new();

        // TODO: All this data wrangling should be done in the engine (or just not in the rule)
        // This change rule just needs to subscribe to the txs that were affected by clustering or ingestion or spent txouts (where MIH does not hold otherwise it would covered by clustering)
        for txout_id in input.iter() {
            // Get the transaction that directly contains this txout
            let tx_handle = txout_id.txid.with(store.index());
            affected_txs.insert(tx_handle.id());

            // Also get all transactions containing txouts in the same cluster
            let cluster_handle = ClusterHandle::new(txout_id, store.index());
            for cluster_txout in cluster_handle.iter_txouts() {
                let tx_handle = cluster_txout.tx();
                affected_txs.insert(tx_handle.id());
            }

            // For all the txs that spend those txouts, add them to the affected_txs set
            store
                .index()
                .spending_txins
                .get(&txout_id)
                .iter()
                .for_each(|txin_id| {
                    affected_txs.insert(txin_id.txid());
                });
        }

        // Collect all change annotations first to avoid borrowing headaches
        let mut change_annotations = Vec::new();
        let mut clustering_sets: Vec<SparseDisjointSet<TxOutId>> = Vec::new();

        for tx_id in affected_txs {
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
                let is_change_bool = is_change == ChangeIdentificationResult::Change;
                change_annotations.push((txout_id, is_change_bool));
                if is_change_bool {
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
    use std::sync::Arc;
    use tx_indexer_primitives::{
        datalog::TxOutInput,
        loose::{TxId, TxOutId},
        storage::{FactStore, InMemoryIndex, MemStore},
        test_utils::DummyTxData,
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
            index: 0,
            containing_tx: DummyTxData {
                id: TxId(1),
                outputs_amounts: vec![100],
                spent_coins: vec![],
            },
        };
        assert_eq!(
            NaiveChangeIdentificationHueristic::is_change(txout),
            ChangeIdentificationResult::Change
        );
    }

    #[test]
    fn test_change_identification_annotates_outputs() {
        let mut index = InMemoryIndex::new();
        let prev_tx = DummyTxData {
            id: TxId(0),
            outputs_amounts: vec![500],
            spent_coins: vec![],
        };
        let tx = DummyTxData {
            id: TxId(1),
            outputs_amounts: vec![100, 200],
            spent_coins: vec![TxOutId::new(TxId(0), 0)],
        };
        index.add_tx(Arc::new(prev_tx));
        index.add_tx(Arc::new(tx.clone()));

        let mut store = setup_store(index);
        store.insert::<TxRel>(TxId(1));

        let input = TxOutInput::new(vec![TxOutId::new(TxId(1), 0)]);

        let mut rule = ChangeIdentificationRule;
        rule.step(input, &mut store);

        assert!(store.contains::<ChangeIdentificationRel>(&(TxOutId::new(TxId(1), 0), false)));
        assert!(store.contains::<ChangeIdentificationRel>(&(TxOutId::new(TxId(1), 1), true)));
    }

    #[test]
    fn test_skips_coinbase() {
        let mut index = InMemoryIndex::new();
        let tx = DummyTxData {
            id: TxId(1),
            outputs_amounts: vec![100, 200],
            spent_coins: vec![],
        };
        index.add_tx(Arc::new(tx.clone()));

        let mut store = setup_store(index);
        store.insert::<TxRel>(TxId(1));

        let input = TxOutInput::new(vec![TxOutId::new(TxId(1), 0)]);

        let mut rule = ChangeIdentificationRule;
        rule.step(input, &mut store);
    }

    #[test]
    fn test_clusters_change_with_inputs() {
        let mut index = InMemoryIndex::new();
        let prev_tx = DummyTxData {
            id: TxId(0),
            outputs_amounts: vec![500, 500],
            spent_coins: vec![],
        };
        let input1 = TxOutId::new(TxId(0), 0);
        let input2 = TxOutId::new(TxId(0), 1);
        let tx = DummyTxData {
            id: TxId(1),
            outputs_amounts: vec![100, 200],
            spent_coins: vec![input1, input2],
        };
        index.add_tx(Arc::new(prev_tx));
        index.add_tx(Arc::new(tx.clone()));
        index.global_clustering.union(input1, input2);

        let mut store = setup_store(index);
        store.insert::<TxRel>(TxId(1));

        let input = TxOutInput::new(vec![TxOutId::new(TxId(1), 0)]);

        let mut rule = ChangeIdentificationRule;
        rule.step(input, &mut store);

        assert!(store.contains::<ChangeIdentificationRel>(&(TxOutId::new(TxId(1), 1), true)));

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
            outputs_amounts: vec![500, 500],
            spent_coins: vec![],
        };
        let input1 = TxOutId::new(TxId(0), 0);
        let input2 = TxOutId::new(TxId(0), 1);
        let tx = DummyTxData {
            id: TxId(1),
            outputs_amounts: vec![100, 200],
            spent_coins: vec![input1, input2],
        };
        index.add_tx(Arc::new(prev_tx));
        index.add_tx(Arc::new(tx.clone()));

        let mut store = setup_store(index);
        store.insert::<TxRel>(TxId(1));

        let input = TxOutInput::new(vec![TxOutId::new(TxId(1), 0)]);

        let mut rule = ChangeIdentificationRule;
        rule.step(input, &mut store);

        assert!(store.contains::<ChangeIdentificationRel>(&(TxOutId::new(TxId(1), 1), true)));

        let clusters: Vec<_> = store.read_range::<ClusterRel>(0, store.len::<ClusterRel>());
        assert_eq!(clusters.len(), 0);
    }

    #[test]
    fn test_processes_multiple_transactions() {
        let mut index = InMemoryIndex::new();
        let prev_tx = DummyTxData {
            id: TxId(0),
            outputs_amounts: vec![500, 500],
            spent_coins: vec![],
        };
        let tx1 = DummyTxData {
            id: TxId(1),
            outputs_amounts: vec![100, 200],
            spent_coins: vec![TxOutId::new(TxId(0), 0)],
        };
        let tx2 = DummyTxData {
            id: TxId(2),
            outputs_amounts: vec![300],
            spent_coins: vec![TxOutId::new(TxId(0), 1)],
        };
        index.add_tx(Arc::new(prev_tx));
        index.add_tx(Arc::new(tx1.clone()));
        index.add_tx(Arc::new(tx2.clone()));

        let mut store = setup_store(index);
        store.insert::<TxRel>(TxId(1));
        store.insert::<TxRel>(TxId(2));

        let input = TxOutInput::new(vec![TxOutId::new(TxId(1), 0), TxOutId::new(TxId(2), 0)]);

        let mut rule = ChangeIdentificationRule;
        rule.step(input, &mut store);

        assert!(store.contains::<ChangeIdentificationRel>(&(TxOutId::new(TxId(1), 0), false)));
        assert!(store.contains::<ChangeIdentificationRel>(&(TxOutId::new(TxId(1), 1), true)));
        assert!(store.contains::<ChangeIdentificationRel>(&(TxOutId::new(TxId(2), 0), true)));
    }
}
