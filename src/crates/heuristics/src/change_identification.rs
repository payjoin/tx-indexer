use std::{any::TypeId, collections::HashSet};

use tx_indexer_primitives::{
    abstract_types::{AbstractTransaction, EnumerateSpentTxOuts, OutputCount, TxConstituent},
    datalog::{ChangeIdentificationRel, ClusterRel, CursorBook, GlobalClusteringRel, Rule, TxRel},
    disjoint_set::{DisJointSet, SparseDisjointSet},
    loose::{ClusterHandle, TxId, TxOutId},
    storage::{FactStore, MemStore},
    test_utils::DummyTxOut,
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

impl Rule for ChangeIdentificationRule {
    fn name(&self) -> &'static str {
        "change_identification"
    }

    fn inputs(&self) -> &'static [TypeId] {
        const INS: &[TypeId] = &[TypeId::of::<TxRel>(), TypeId::of::<ClusterRel>()];
        INS
    }

    fn step(&mut self, rid: usize, store: &mut MemStore, cursors: &mut CursorBook) -> usize {
        let delta_txs = cursors.read_delta::<TxRel>(rid, store);
        if delta_txs.is_empty() {
            return 0;
        }

        let delta_clusters: Vec<SparseDisjointSet<TxOutId>> =
            cursors.read_delta::<ClusterRel>(rid, store);
        // Transactions that now are clustered. We may have known change but now can cluster its change with the other clustered outputs.
        // Collect txids first to avoid borrowing store during iteration
        let txids = delta_clusters
            .into_iter()
            .flat_map(|cluster| cluster.iter_parent_ids().collect::<Vec<_>>())
            .collect::<HashSet<TxOutId>>();

        let mut out = 0;
        // TODO: collapse into one loop
        for txid in txids {
            let tx = txid.with(store.index()).spent_by().map(|txin| txin.tx());
            if let Some(tx) = tx {
                let output_results: Vec<(TxOutId, bool)> = tx
                    .outputs()
                    .map(|output| {
                        let txout_id = output.id();
                        let is_change = NaiveChangeIdentificationHueristic::is_change(output);
                        (txout_id, is_change)
                    })
                    .map(|(txout_id, is_change)| {
                        (
                            txout_id,
                            matches!(is_change, ChangeIdentificationResult::Change),
                        )
                    })
                    .collect();

                for (txout_id, is_change) in output_results {
                    store.insert::<ChangeIdentificationRel>((txout_id, is_change));
                    out += 1;
                }
            }
        }

        for tx in delta_txs {
            if tx.spent_coins().next().is_none() {
                // Coinbases don't have "change"
                continue;
            }
            for (i, _amount) in tx.outputs().enumerate() {
                let txout_id = TxOutId::new(tx.id, i as u32);
                let txout = DummyTxOut {
                    index: i,
                    containing_tx: tx.clone(),
                };
                let is_change = NaiveChangeIdentificationHueristic::is_change(txout);
                if is_change == ChangeIdentificationResult::Change {
                    store.insert::<ChangeIdentificationRel>((txout_id, true));
                } else {
                    store.insert::<ChangeIdentificationRel>((txout_id, false));
                }
                out += 1;
            }
        }
        out
    }
}

pub struct ChangeIdentificationClusterRule;

impl ChangeIdentificationClusterRule {
    fn analyze_change(&self, tx_id: TxId, store: &mut MemStore) -> SparseDisjointSet<TxOutId> {
        println!("analyze_change: {:?}", tx_id);
        let set = SparseDisjointSet::<TxOutId>::new();
        let tx_handle = tx_id.with(store.index());

        // Check if all inputs are clustered together
        if !tx_handle.inputs_are_clustered() {
            println!("inputs are not clustered");
            return set;
        }
        let root = tx_handle
            .spent_coins()
            .next()
            .expect("to have one spent coin");

        let change_outputs = tx_handle
            .outputs()
            .filter(|txout| {
                let txout_id = txout.id();
                // TODO: This is a big look up. How can we do better in the sparse repr.
                store.contains::<ChangeIdentificationRel>(&(txout_id, true))
            })
            .map(|txout| txout.id())
            .collect::<Vec<_>>();

        println!("change_outputs: {:?}", change_outputs);

        // Union each change output with the clustered inputs
        for change_output in change_outputs {
            set.union(change_output, root);
        }

        set
    }
}

impl Rule for ChangeIdentificationClusterRule {
    fn name(&self) -> &'static str {
        "change_identification_cluster"
    }

    fn inputs(&self) -> &'static [TypeId] {
        const INS: &[TypeId] = &[
            TypeId::of::<ChangeIdentificationRel>(),
            TypeId::of::<GlobalClusteringRel>(),
        ];
        INS
    }

    fn step(&mut self, rid: usize, store: &mut MemStore, cursors: &mut CursorBook) -> usize {
        // Gather all change outputs that have been labeled
        let binding = cursors.read_delta::<ChangeIdentificationRel>(rid, store);
        let delta_change_outputs = binding.iter().map(|(txout_id, _)| *txout_id);

        // Gather all global clustering facts that have been updated
        let binding = cursors.read_delta::<GlobalClusteringRel>(rid, store);
        let delta_global_clustering = binding.iter().flat_map(|set| set.iter_parent_ids());
        // .flat_map(|txout_id| store.index().global_clustering.iter_set(txout_id));

        let mut affected_txs = HashSet::new();

        for txout_id in delta_change_outputs.chain(delta_global_clustering) {
            let cluster_handle = ClusterHandle::new(txout_id, store.index());

            // Get all txouts in the cluster
            for cluster_txout in cluster_handle.iter_txouts() {
                // Get the transaction containing this txout
                let tx_handle = cluster_txout.tx();
                affected_txs.insert(tx_handle.id());
            }
        }

        // Analyze each affected transaction
        let mut out = 0;
        for tx_id in affected_txs {
            println!("affected_txs: {:?}", tx_id);
            let set = self.analyze_change(tx_id, store);
            store.insert::<ClusterRel>(set);

            out += 1;
        }

        out
    }
}

#[cfg(test)]
mod tests {
    use tx_indexer_primitives::{
        loose::TxId,
        test_utils::{DummyTxData, DummyTxOut},
    };

    use super::*;

    #[test]
    fn test_classify_change() {
        let txout = DummyTxOut {
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
}
