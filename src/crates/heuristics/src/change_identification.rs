use std::{any::TypeId, collections::HashSet};

use tx_indexer_primitives::{
    abstract_types::{EnumerateSpentTxOuts, OutputCount, TxConstituent},
    datalog::{
        ChangeIdentificationRel, ClusterRel, GlobalClusteringRel, Rule, TransactionInput, TxRel,
    },
    disjoint_set::{DisJointSet, SparseDisjointSet},
    loose::{ClusterHandle, TxId, TxOutId},
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

impl Rule for ChangeIdentificationRule {
    type Input = TransactionInput;

    fn name(&self) -> &'static str {
        "change_identification"
    }

    fn inputs(&self) -> &'static [TypeId] {
        const INS: &[TypeId] = &[TypeId::of::<TxRel>(), TypeId::of::<ClusterRel>()];
        INS
    }

    fn step(&mut self, input: Self::Input, store: &mut MemStore) -> usize {
        // Collect all results first to avoid borrowing issues
        let mut results = Vec::new();
        for tx_id in input.iter() {
            let tx_handle = tx_id.with(store.index());
            if tx_handle.spent_coins().next().is_none() {
                // Coinbases don't have "change"
                continue;
            }
            for txout_handle in tx_handle.outputs() {
                let txout_id = txout_handle.id();
                let is_change = NaiveChangeIdentificationHueristic::is_change(txout_handle);
                results.push((txout_id, is_change == ChangeIdentificationResult::Change));
            }
        }

        // Now insert all results
        let mut out = 0;
        for (txout_id, is_change) in results {
            store.insert::<ChangeIdentificationRel>((txout_id, is_change));
            out += 1;
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
    type Input = tx_indexer_primitives::datalog::TxOutInput;

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

    fn step(&mut self, input: Self::Input, store: &mut MemStore) -> usize {
        let mut affected_txs = HashSet::new();

        for txout_id in input.iter() {
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

            // Only write if the set is not empty (i.e., we actually found change outputs to cluster)
            if !set.is_empty() {
                store.insert::<ClusterRel>(set);
                out += 1;
            }
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
