pub mod change_identification;
pub mod coinjoin_detection;
pub mod common_input;

/// AST-based heuristics for the pipeline DSL.
pub mod ast;

// #[cfg(test)]
// mod tests {
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
// }
