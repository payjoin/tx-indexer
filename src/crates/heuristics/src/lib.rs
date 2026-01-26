pub mod change_identification;
pub mod coinjoin_detection;
pub mod common_input;

use std::any::TypeId;

use tx_indexer_primitives::{
    datalog::{ClusterRel, GlobalClusteringRel, RawTransactionInput, RawTxRel, Rule, TxRel},
    disjoint_set::{DisJointSet, SparseDisjointSet},
    loose::TxOutId,
    storage::{FactStore, MemStore},
};

pub struct TransactionIngestionRule;

impl Rule for TransactionIngestionRule {
    type Input = RawTransactionInput;

    fn name(&self) -> &'static str {
        "transaction_ingestion"
    }

    fn inputs(&self) -> &'static [TypeId] {
        const INS: &[TypeId] = &[TypeId::of::<RawTxRel>()];
        INS
    }

    fn step(&mut self, input: Self::Input, store: &mut MemStore) -> usize {
        let mut count = 0;
        for tx_wrapper in input.iter() {
            let tx_arc = tx_wrapper.clone().into_arc();
            let tx_id = tx_arc.txid();

            let _ = store.index_mut().add_tx(tx_arc);

            // Emit TxId to TxRel
            store.insert::<TxRel>(tx_id);
            count += 1;
        }

        count
    }
}

pub struct GlobalClustering;

impl Rule for GlobalClustering {
    type Input = tx_indexer_primitives::datalog::ClusterInput;

    fn name(&self) -> &'static str {
        "global_clustering"
    }

    fn inputs(&self) -> &'static [TypeId] {
        const INS: &[TypeId] = &[TypeId::of::<ClusterRel>()];
        INS
    }

    fn step(&mut self, input: Self::Input, store: &mut MemStore) -> usize {
        let clusters: Vec<_> = input.iter().collect();
        println!("Global delta_clusters: {:?}", clusters);
        if clusters.is_empty() {
            return 0;
        }

        let unified_cluster = clusters
            .into_iter()
            .cloned()
            .reduce(|acc, cluster| acc.join(&cluster))
            .unwrap();
        println!("unified_cluster: {:?}", unified_cluster);

        if unified_cluster.is_empty() {
            return 0;
        }

        let old_global = store.index().global_clustering.clone();
        let new_global = old_global.join(&unified_cluster);
        
        if unified_cluster_has_new_info(&unified_cluster, &old_global) {
            store.index_mut().global_clustering = new_global;
            store.insert::<GlobalClusteringRel>(unified_cluster.clone());
            1
        } else {
            0
        }
    }
}

fn unified_cluster_has_new_info(
    unified_cluster: &SparseDisjointSet<TxOutId>,
    global_clustering: &SparseDisjointSet<TxOutId>,
) -> bool {
    if unified_cluster.is_empty() {
        return false;
    }
    
    let roots: Vec<_> = unified_cluster.iter_parent_ids().collect();
    if roots.is_empty() {
        return false;
    }
    
    let mut all_elements = Vec::new();
    for &root in &roots {
        all_elements.extend(unified_cluster.iter_set(root));
    }
    
    if all_elements.len() < 2 {
        return false;
    }
    
    let unified_root = unified_cluster.find(all_elements[0]);
    for &elem in &all_elements {
        if unified_cluster.find(elem) != unified_root {
            continue;
        }
        if global_clustering.find(elem) != global_clustering.find(all_elements[0]) {
            return true;
        }
    }
    
    false
}

#[cfg(test)]
mod tests {
    use tx_indexer_primitives::{
        datalog::{
            AbstractTxWrapper, ChangeIdentificationRel, ClusterRel, EngineBuilder,
            GlobalClusteringRel, IsCoinJoinRel, RawTxRel, TxRel,
        },
        disjoint_set::DisJointSet,
        loose::{TxId, TxOutId},
        storage::{FactStore, InMemoryIndex},
        test_utils::DummyTxData,
    };

    use crate::{
        GlobalClustering, TransactionIngestionRule,
        change_identification::ChangeIdentificationRule,
        coinjoin_detection::CoinJoinRule,
        common_input::MihRule,
    };

    /// A test fixture for the heuristics pipeline
    /// This test is two coinbase txs, a spending tx that spends the two coinbases, a change output and a payment output.
    struct TestFixture {
        txs: Vec<DummyTxData>,
    }

    impl TestFixture {
        fn new() -> Self {
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
            Self { txs: src }
        }

        fn all(&self) -> Vec<DummyTxData> {
            self.txs.clone()
        }

        fn coinbases(&self) -> Vec<DummyTxData> {
            vec![self.txs[0].clone(), self.txs[1].clone()]
        }

        fn spending_tx(&self) -> DummyTxData {
            self.txs[2].clone()
        }

        fn payment_output(&self) -> TxOutId {
            TxOutId::new(self.txs[2].id, 0)
        }

        fn change_output(&self) -> TxOutId {
            TxOutId::new(self.txs[2].id, 1)
        }
    }

    // #[test]
    // fn test_filtered() {
    // let fixture = TestFixture::new();
    // let mut index = InMemoryIndex::new();

    //     for tx in src.iter() {
    //         index.txs.insert(tx.id, tx.clone().into());
    //     }

    //     let change_identification_map = index
    //         .clone()
    //         .map(FnMapToBoolMapPass::new(change_identification_map_pass_fn));

    //     let mut analysis = index
    //         .clone()
    //         // First filter out coinjoins
    //         .filter(PredicateFilterPass::new(coinjoin_detection_filter_pass_fn))
    //         // Then merge prevouts according to the MultiInputHeuristic
    //         .map(FnMapPass::new(common_input_map_pass_fn))
    //         // Then merge the change identification map with the prevouts
    //         // Effectively clustering the change output with the spending txouts
    //         .merge(
    //             change_identification_map,
    //             FnMergePass::new(
    //                 |txout_id: &TxOutId, index: &DummyIndex| {
    //                     if let Some(tx) = index.txs.get(&txout_id.txid) {
    //                         tx.spent_coins
    //                             .iter()
    //                             .next()
    //                             .map(|spent_txout| (*txout_id, *spent_txout))
    //                     } else {
    //                         None
    //                     }
    //                 },
    //                 &index,
    //             ),
    //         )
    //         .output();

    //     let change_txout = TxOutId::new(TxId(2), 1);
    //     let coinbase_txout0 = TxOutId::new(TxId(0), 0);
    //     let coinbase_txout1 = TxOutId::new(TxId(1), 0);
    //     assert_eq!(analysis.find(change_txout), analysis.find(coinbase_txout0));
    //     assert_eq!(analysis.find(change_txout), analysis.find(coinbase_txout1));

    //     let other_cluster_txout = TxOutId::new(TxId(2), 0);
    //     assert_ne!(
    //         analysis.find(change_txout),
    //         analysis.find(other_cluster_txout)
    //     );
    // }

    #[test]
    fn test_e2e_pipeline() {
        let fixture = TestFixture::new();

        let mut engine = EngineBuilder::new()
            .add_rule(TransactionIngestionRule)
            .add_rule(CoinJoinRule)
            .add_rule(MihRule)
            .add_rule(ChangeIdentificationRule)
            .add_rule(GlobalClustering)
            .build();

        // TODO: eliminate memstore, store.initialized.
        // TODO: repr data dependency with rust expressions.
        // TODO: datatype for the entire computation -> what the engine takes as input
        let mut store = tx_indexer_primitives::storage::MemStore::new(InMemoryIndex::new());
        store.initialize::<RawTxRel>();
        store.initialize::<TxRel>();
        store.initialize::<IsCoinJoinRel>();
        store.initialize::<ClusterRel>();
        store.initialize::<ChangeIdentificationRel>();
        store.initialize::<GlobalClusteringRel>();

        for tx in fixture.coinbases() {
            let tx_wrapper = AbstractTxWrapper::new(tx.into());
            store.insert::<RawTxRel>(tx_wrapper);
        }

        engine.run_to_fixpoint(&mut store);

        // Lets add more txs (this time with inputs) and re-run with deltas causing writes
        let tx_wrapper = AbstractTxWrapper::new(fixture.spending_tx().into());
        store.insert::<RawTxRel>(tx_wrapper);
        engine.run_to_fixpoint(&mut store);

        // Two new clustering facts should be present:
        // 1. MIH on the spending tx
        // 2. Change clustering on the change output of the spending tx
        // Under global clustering we should have one cluster with all these txouts and one empty cluster because of the first run above
        let cluster = store.index().global_clustering.clone();
        println!("cluster: {:?}", cluster);
        assert_eq!(
            cluster.find(fixture.spending_tx().spent_coins[0]),
            cluster.find(fixture.spending_tx().spent_coins[1])
        );
        assert_eq!(
            cluster.find(fixture.change_output()),
            cluster.find(fixture.spending_tx().spent_coins[0])
        );
        assert_ne!(
            cluster.find(fixture.change_output()),
            cluster.find(fixture.payment_output())
        );
    }
}
