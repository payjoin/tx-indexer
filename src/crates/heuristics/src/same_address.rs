use std::any::TypeId;

use tx_indexer_primitives::{
    abstract_types::EnumerateSpentTxOuts,
    datalog::{
        ChangeIdentificationRel, ClusterRel, Rule, TransactionInput, TxOutAnnotation, TxRel,
    },
    disjoint_set::{DisJointSet, SparseDisjointSet},
    loose::TxOutId,
    storage::{FactStore, MemStore},
};

pub struct SameAddressRule;

impl Rule for SameAddressRule {
    type Input = TransactionInput;

    fn name(&self) -> &'static str {
        "same_address"
    }

    fn inputs(&self) -> &'static [TypeId] {
        const INS: &[TypeId] = &[TypeId::of::<TxRel>()];
        INS
    }

    fn step(&mut self, input: Self::Input, store: &mut MemStore) -> usize {
        let mut clusters_created = 0;

        for tx_id in input.iter() {
            let cluster = SparseDisjointSet::<TxOutId>::new();

            let tx_handle = tx_id.with(store.index());
            let first_spent_input = tx_handle
                .spent_coins()
                .next()
                .expect("Transaction should have at least one input if clustered");

            // Iterate through all outputs of the transaction
            for output_handle in tx_handle.outputs() {
                let tx_out_id = output_handle.id();
                let spk_hash = output_handle.script_pubkey_hash();
                // Check if this SPK has been seen before
                if let Some(shared_spk_txout_ids) = store.index().spk_to_txout_ids.get(&spk_hash) {
                    // First cluster with the other outputs
                    // This SPK has been seen before - cluster with previous txouts TODO only if the shared spk is change
                    for &shared_spk_txout_id in shared_spk_txout_ids {
                        if shared_spk_txout_id == tx_out_id {
                            continue;
                        }
                        // Cluster the shared spk outputs
                        cluster.union(tx_out_id, shared_spk_txout_id);
                        let other_tx_handle = shared_spk_txout_id.with(store.index()).tx();
                        if tx_handle.inputs_are_clustered()
                            && other_tx_handle.inputs_are_clustered()
                            // TODO: should be a helper on tx handle or txout handle
                            && store.contains::<ChangeIdentificationRel>(&(tx_out_id, TxOutAnnotation::Change))
                        {
                            // Shared spk output can be clustered with the inputs of the other transactions
                            let other_first_input = other_tx_handle.spent_coins().next().expect(
                                "Previous transaction should have at least one input if clustered",
                            );
                            cluster.union(first_spent_input, other_first_input);
                        }
                    }
                }
            }

            if !cluster.is_empty() {
                store.insert::<ClusterRel>(cluster);
                clusters_created += 1;
            }
        }

        clusters_created
    }
}

#[cfg(test)]
mod tests {
    use tx_indexer_primitives::{
        datalog::{
            ChangeIdentificationRel, ClusterRel, EngineBuilder, RawTxRel, TxOutAnnotation, TxRel,
        },
        disjoint_set::{DisJointSet, SparseDisjointSet},
        loose::{TxId, TxOutId},
        storage::{FactStore, InMemoryIndex, MemStore},
        test_utils::{DummyTxData, DummyTxOutData},
    };

    use super::SameAddressRule;

    #[test]
    fn test_same_address_clusters_outputs() {
        // Create two transactions with the same SPK hash
        let spk_hash = [1u8; 20];

        let tx1 = DummyTxData {
            id: TxId(2),
            outputs: vec![DummyTxOutData::new(100, spk_hash)],
            spent_coins: vec![TxOutId::new(TxId(0), 0), TxOutId::new(TxId(0), 1)],
        };

        let tx2 = DummyTxData {
            id: TxId(3),
            outputs: vec![DummyTxOutData::new(200, spk_hash)],
            spent_coins: vec![TxOutId::new(TxId(1), 0)],
        };

        let mut store = MemStore::new(InMemoryIndex::new());
        store.initialize::<RawTxRel>();
        store.initialize::<TxRel>();
        store.initialize::<ClusterRel>();

        // Union the two inputs of the first transaction
        // At the end of this test all the spent coins should be unioned together
        store
            .index_mut()
            .global_clustering
            .union(TxOutId::new(TxId(0), 0), TxOutId::new(TxId(0), 1));

        // Add id label to the only txout of tx1
        store
            .insert::<ChangeIdentificationRel>((TxOutId::new(TxId(2), 0), TxOutAnnotation::Change));
        store
            .insert::<ChangeIdentificationRel>((TxOutId::new(TxId(3), 0), TxOutAnnotation::Change));
        store.insert::<RawTxRel>(tx1.into());

        let mut engine = EngineBuilder::new()
            .add_rule(crate::TransactionIngestionRule)
            .add_rule(SameAddressRule)
            .build();
        engine.run_to_fixpoint(&mut store);

        // Add second transaction
        store.insert::<RawTxRel>(tx2.into());
        engine.run_to_fixpoint(&mut store);

        // Check that outputs with same SPK are clustered
        let clusters: Vec<SparseDisjointSet<TxOutId>> =
            store.read_range::<ClusterRel>(0, store.len::<ClusterRel>());
        assert!(clusters.len() > 0);

        let cluster = clusters.first().expect("Should have at least one cluster");

        assert_eq!(
            cluster.find(TxOutId::new(TxId(0), 0)),
            cluster.find(TxOutId::new(TxId(1), 0))
        );

        // Shared spk should also be clustered
        assert_eq!(
            cluster.find(TxOutId::new(TxId(2), 0)),
            cluster.find(TxOutId::new(TxId(3), 0))
        );
    }
}
