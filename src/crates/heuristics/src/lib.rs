pub mod change_identification;
pub mod coinjoin_detection;
pub mod common_input;

#[cfg(test)]
mod tests {
    use tx_indexer_primitives::{
        datalog::{ClusterRel, EngineBuilder, IsCoinJoinRel, TxRel},
        disjoint_set::DisJointSet,
        loose::{TxId, TxOutId},
        pass::{AnalysisPass, FnMapPass, FnMapToBoolMapPass, FnMergePass, PredicateFilterPass},
        storage::{FactStore, InMemoryIndex},
        test_utils::DummyTxData,
    };

    use crate::{
        change_identification::change_identification_map_pass_fn,
        coinjoin_detection::{CoinJoinRule, coinjoin_detection_filter_pass_fn},
        common_input::{MihRule, common_input_map_pass_fn},
    };

    // #[test]
    // fn test_filtered() {
    //     let src = vec![
    //         // Coinbase 1
    //         DummyTxData {
    //             id: TxId(0),
    //             outputs_amounts: vec![100, 200, 300],
    //             spent_coins: vec![],
    //         },
    //         // Coinbase 2
    //         DummyTxData {
    //             id: TxId(1),
    //             outputs_amounts: vec![100, 100],
    //             spent_coins: vec![],
    //         },
    //         // Non-coinjoin spending the two coinbases
    //         DummyTxData {
    //             id: TxId(2),
    //             // Creating a change output and a payment output
    //             outputs_amounts: vec![100, 200],
    //             // Spending the vout = 0 of the two coinbases
    //             spent_coins: vec![TxOutId::new(TxId(0), 0), TxOutId::new(TxId(1), 0)],
    //         },
    //         // Spending the change output
    //         DummyTxData {
    //             id: TxId(3),
    //             outputs_amounts: vec![100],
    //             // Spending the change output
    //             spent_coins: vec![TxOutId::new(TxId(2), 0)],
    //         },
    //         // Spending the payment output
    //         DummyTxData {
    //             id: TxId(4),
    //             outputs_amounts: vec![200],
    //             // Spending the payment output
    //             spent_coins: vec![TxOutId::new(TxId(2), 1)],
    //         },
    //     ];

    //     let mut index = InMemoryIndex::new();

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
    fn test_coinjoin_detection() {
        let mut engine = EngineBuilder::new()
            .add_rule(Box::new(MihRule))
            .add_rule(Box::new(CoinJoinRule))
            .build();

        // TODO: eliminate memstore, store.initialized.
        // TODO: repr data dependency with rust expressions.
        // TODO: datatype for the entire computation -> what the engine takes as input
        let mut store = tx_indexer_primitives::storage::MemStore::new(InMemoryIndex::new());
        store.initialize::<TxRel>();
        store.initialize::<IsCoinJoinRel>();
        store.initialize::<ClusterRel>();
        // Lets create some dummy txs
        let txs = vec![
            DummyTxData {
                id: TxId(0),
                outputs_amounts: vec![100, 200, 300],
                spent_coins: vec![],
            },
            DummyTxData {
                id: TxId(1),
                outputs_amounts: vec![100, 200, 300],
                spent_coins: vec![],
            },
        ];
        for tx in txs {
            store.insert::<TxRel>(tx);
        }
        engine.run_to_fixpoint(&mut store);

        let coinjoin_facts = store.read_range::<IsCoinJoinRel>(0, store.len::<IsCoinJoinRel>());
        assert_eq!(coinjoin_facts.len(), 2);
        assert_eq!(coinjoin_facts[0], (TxId(0), false));
        assert_eq!(coinjoin_facts[1], (TxId(1), false));

        let clustering_facts = store.read_range::<ClusterRel>(0, store.len::<ClusterRel>());
        // Two empty sets, one for each tx
        assert_eq!(clustering_facts.len(), 2);

        // Lets add more txs (this time with inputs) and re-run with deltas causing writes
        let txs = vec![DummyTxData {
            id: TxId(2),
            outputs_amounts: vec![100, 200],
            spent_coins: vec![TxOutId::new(TxId(0), 0), TxOutId::new(TxId(1), 0)],
        }];
        store.insert::<TxRel>(txs[0].clone());
        engine.run_to_fixpoint(&mut store);
        // Coinjoin results should be the same
        let res = store.read_range::<IsCoinJoinRel>(0, store.len::<IsCoinJoinRel>());
        assert_eq!(res.len(), 3);
        assert_eq!(res[0], (TxId(0), false));
        assert_eq!(res[1], (TxId(1), false));
        assert_eq!(res[2], (TxId(2), false));
        // new MIH clusters should be present
        let res = store.read_range::<ClusterRel>(0, store.len::<ClusterRel>());
        assert_eq!(res.len(), 3);

        // assert_eq!(res[0], (TxOutId::new(TxId(0), 0), TxOutId::new(TxId(1), 0)));
    }
}
