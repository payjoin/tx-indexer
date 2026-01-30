//! Integration tests for the AST-based pipeline DSL.

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use pipeline::Placeholder;
    use pipeline::context::PipelineContext;
    use pipeline::engine::Engine;
    use pipeline::ops::source::AllTxs;
    use pipeline::value::Clustering;
    use tx_indexer_primitives::disjoint_set::DisJointSet;
    use tx_indexer_primitives::loose::{TxId, TxOutId};
    use tx_indexer_primitives::storage::InMemoryIndex;
    use tx_indexer_primitives::test_utils::{DummyTxData, DummyTxOutData};

    use crate::ast::{
        ChangeClustering, ChangeIdentification, IsCoinJoin, IsUnilateral, MultiInputHeuristic,
    };

    /// Test fixture: two coinbase txs, a spending tx with two inputs and two outputs
    fn setup_test_fixture() -> (InMemoryIndex, DummyTxData, TxOutId, TxOutId) {
        let spk_hash = [1u8; 20];

        // Coinbase 1
        let coinbase1 = DummyTxData {
            id: TxId(0),
            outputs: vec![
                DummyTxOutData::new(100, spk_hash),
                DummyTxOutData::new(200, spk_hash),
            ],
            spent_coins: vec![],
        };

        // Coinbase 2
        let coinbase2 = DummyTxData {
            id: TxId(1),
            outputs: vec![DummyTxOutData::new(150, spk_hash)],
            spent_coins: vec![],
        };

        // Spending tx: spends from both coinbases, has payment and change outputs
        let spending_tx = DummyTxData {
            id: TxId(2),
            outputs: vec![
                DummyTxOutData::new(100, spk_hash), // payment
                DummyTxOutData::new(50, spk_hash),  // change (last output)
            ],
            spent_coins: vec![TxOutId::new(TxId(0), 0), TxOutId::new(TxId(1), 0)],
        };

        let payment_output = TxOutId::new(TxId(2), 0);
        let change_output = TxOutId::new(TxId(2), 1);

        let mut index = InMemoryIndex::new();
        index.add_tx(Arc::new(coinbase1));
        index.add_tx(Arc::new(coinbase2));
        index.add_tx(Arc::new(spending_tx.clone()));

        (index, spending_tx, payment_output, change_output)
    }

    #[test]
    fn test_coinjoin_detection() {
        let spk_hash = [1u8; 20];

        // Non-coinjoin tx
        let normal_tx = DummyTxData {
            id: TxId(0),
            outputs: vec![
                DummyTxOutData::new(100, spk_hash),
                DummyTxOutData::new(200, spk_hash),
            ],
            spent_coins: vec![],
        };

        // Coinjoin tx (3+ outputs with same value)
        let coinjoin_tx = DummyTxData {
            id: TxId(1),
            outputs: vec![
                DummyTxOutData::new(100, spk_hash),
                DummyTxOutData::new(100, spk_hash),
                DummyTxOutData::new(100, spk_hash),
            ],
            spent_coins: vec![],
        };

        let mut index = InMemoryIndex::new();
        index.add_tx(Arc::new(normal_tx));
        index.add_tx(Arc::new(coinjoin_tx));

        let ctx = Arc::new(PipelineContext::new());
        let mut engine = Engine::new(ctx.clone(), Arc::new(index));

        let all_txs = AllTxs::new(&ctx);
        let coinjoin_mask = IsCoinJoin::new(all_txs);

        let result = engine.eval(&coinjoin_mask);

        assert_eq!(result.get(&TxId(0)), Some(&false)); // Not a coinjoin
        assert_eq!(result.get(&TxId(1)), Some(&true)); // Is a coinjoin
    }

    #[test]
    fn test_multi_input_heuristic() {
        let (index, spending_tx, _payment, _change) = setup_test_fixture();

        let ctx = Arc::new(PipelineContext::new());
        let mut engine = Engine::new(ctx.clone(), Arc::new(index));

        let all_txs = AllTxs::new(&ctx);
        let coinjoin_mask = IsCoinJoin::new(all_txs.clone());
        let non_coinjoin = all_txs.filter_with_mask(coinjoin_mask.negate());
        let mih_clustering = MultiInputHeuristic::new(non_coinjoin);

        let result = engine.eval(&mih_clustering);

        // The two inputs of spending_tx should be in the same cluster
        let input1 = spending_tx.spent_coins[0];
        let input2 = spending_tx.spent_coins[1];
        assert_eq!(result.find(input1), result.find(input2));
    }

    #[test]
    fn test_change_identification() {
        let (index, _spending_tx, payment_output, change_output) = setup_test_fixture();

        let ctx = Arc::new(PipelineContext::new());
        let mut engine = Engine::new(ctx.clone(), Arc::new(index));

        let all_txs = AllTxs::new(&ctx);
        let change_mask = ChangeIdentification::new(all_txs);

        let result = engine.eval(&change_mask);

        // Payment output (vout=0) should not be change
        assert_eq!(result.get(&payment_output), Some(&false));
        // Change output (vout=1, last output) should be change
        assert_eq!(result.get(&change_output), Some(&true));
    }

    #[test]
    fn test_unilateral_detection() {
        let (index, _spending_tx, _payment_output, _change_output) = setup_test_fixture();

        let ctx = Arc::new(PipelineContext::new());
        let mut engine = Engine::new(ctx.clone(), Arc::new(index));

        let all_txs = AllTxs::new(&ctx);

        let mih_clustering = MultiInputHeuristic::new(all_txs.clone());
        let is_unilateral_mask = IsUnilateral::with_clustering(all_txs, mih_clustering);

        let result = engine.eval(&is_unilateral_mask);

        // First two coinbases are not unilateral
        assert_eq!(result.get(&TxId(0)), Some(&false));
        assert_eq!(result.get(&TxId(1)), Some(&false));
        // The spending tx is unilateral
        assert_eq!(result.get(&TxId(2)), Some(&true));
    }

    #[test]
    fn test_updated_unilateral_detection() {
        // Test that we get updated unilateral detection when the clustering changes
        let (index, spending_tx, _payment_output, _change_output) = setup_test_fixture();

        let ctx = Arc::new(PipelineContext::new());
        let mut engine = Engine::new(ctx.clone(), Arc::new(index));

        let all_txs = AllTxs::new(&ctx);
        let placeholder = Placeholder::<Clustering>::new(&ctx);
        let mih_clustering = MultiInputHeuristic::new(all_txs.clone());
        let is_unilateral_mask = IsUnilateral::with_clustering(all_txs, placeholder.as_expr());

        // Unify the placeholder with the MIH clustering
        placeholder.unify(mih_clustering);

        let result = engine.eval(&is_unilateral_mask);

        // The spending tx should be unilateral
        assert_eq!(result.get(&spending_tx.id), Some(&true));
    }

    #[test]
    fn test_full_pipeline() {
        let (index, spending_tx, payment_output, change_output) = setup_test_fixture();

        // Pre-cluster the inputs so IsUnilateral passes
        let input1 = spending_tx.spent_coins[0];
        let input2 = spending_tx.spent_coins[1];
        index.global_clustering.union(input1, input2);

        let ctx = Arc::new(PipelineContext::new());
        let mut engine = Engine::new(ctx.clone(), Arc::new(index));

        // Build the pipeline (similar to the user's example)
        let all_txs = AllTxs::new(&ctx);

        let coinjoin_mask = IsCoinJoin::new(all_txs.clone());
        let non_coinjoin = all_txs.filter_with_mask(coinjoin_mask.negate());
        let mih_clustering = MultiInputHeuristic::new(non_coinjoin.clone());

        let change_mask = ChangeIdentification::new(all_txs.clone());

        // For IsUnilateral, we use MIH clustering
        let unilateral_mask =
            IsUnilateral::with_clustering(non_coinjoin.clone(), mih_clustering.clone());

        // Get outputs that are marked as change
        let change_outputs = non_coinjoin.outputs().filter_with_mask(change_mask);
        let txs_with_change = change_outputs.txs();

        // Filter to unilateral txs with change
        let unilateral_with_change = txs_with_change.filter_with_mask(unilateral_mask);

        // Get change mask for these txs
        let filtered_change_mask = ChangeIdentification::new(unilateral_with_change.clone());
        let change_clustering = ChangeClustering::new(unilateral_with_change, filtered_change_mask);

        let combined = change_clustering.join(mih_clustering);

        // Evaluate
        let result = engine.eval(&combined);

        // The two inputs should be clustered together (MIH)
        assert_eq!(result.find(input1), result.find(input2));

        // Change output should be clustered with inputs (change clustering)
        assert_eq!(result.find(change_output), result.find(input1));

        // Payment output should NOT be in the same cluster
        assert_ne!(result.find(payment_output), result.find(input1));
    }

    #[test]
    fn test_pipeline_with_placeholder() {
        let (index, spending_tx, _payment_output, _change_output) = setup_test_fixture();

        let ctx = Arc::new(PipelineContext::new());
        let mut engine = Engine::new(ctx.clone(), Arc::new(index));

        let all_txs = AllTxs::new(&ctx);

        // Create a placeholder for global clustering
        let global_clustering = Placeholder::<Clustering>::new(&ctx);

        // MIH clustering
        let mih_clustering = MultiInputHeuristic::new(all_txs.clone());

        // Unify the placeholder with MIH clustering
        global_clustering.unify(mih_clustering);

        // Run to fixpoint
        engine.run_to_fixpoint();

        // Evaluate
        let result = engine.eval(&global_clustering.as_expr());

        // The two inputs should be clustered
        let input1 = spending_tx.spent_coins[0];
        let input2 = spending_tx.spent_coins[1];
        assert_eq!(result.find(input1), result.find(input2));
    }

    /// Test the full cyclic dependency pattern:
    ///
    /// global_clustering (placeholder)
    ///       ↓ (used by)
    /// IsUnilateral(txs, global_clustering)
    ///       ↓ (filters)
    /// change_clustering
    ///       ↓ (joins with)
    /// mih_clustering
    ///       ↓ (result)
    /// combined_clustering
    ///       ↓ (unified with)
    /// global_clustering (closes the cycle)
    ///
    /// The fixpoint iteration should:
    /// 1. Start with empty global_clustering
    /// 2. IsUnilateral returns false (no clustering yet)
    /// 3. change_clustering is empty
    /// 4. global_clustering = mih_clustering
    /// 5. Re-run: IsUnilateral now sees MIH clustering
    /// 6. change_clustering adds more clusters
    /// 7. global_clustering = mih.join(change)
    /// 8. Continue until stable
    #[test]
    fn test_cyclic_dependency_pattern() {
        let spk_hash = [1u8; 20];

        // Build a chain: coinbase -> tx1 -> tx2
        // tx1 has two outputs (payment + change)
        // tx2 spends the change output

        let coinbase = DummyTxData {
            id: TxId(0),
            outputs: vec![DummyTxOutData::new(1000, spk_hash)],
            spent_coins: vec![],
        };

        // tx1: spends coinbase, creates payment + change
        let tx1 = DummyTxData {
            id: TxId(1),
            outputs: vec![
                DummyTxOutData::new(700, spk_hash), // payment (vout=0)
                DummyTxOutData::new(300, spk_hash), // change (vout=1, last)
            ],
            spent_coins: vec![TxOutId::new(TxId(0), 0)],
        };

        // tx2: spends the change output of tx1 along with another input
        // This tests the recursive nature: once we know tx1's change is clustered
        // with coinbase, tx2's MIH should cluster its inputs
        let coinbase2 = DummyTxData {
            id: TxId(2),
            outputs: vec![DummyTxOutData::new(500, spk_hash)],
            spent_coins: vec![],
        };

        let tx2 = DummyTxData {
            id: TxId(3),
            outputs: vec![
                DummyTxOutData::new(400, spk_hash), // payment
                DummyTxOutData::new(100, spk_hash), // change
            ],
            spent_coins: vec![
                TxOutId::new(TxId(1), 1), // spends tx1's change
                TxOutId::new(TxId(2), 0), // spends coinbase2
            ],
        };

        let mut index = InMemoryIndex::new();
        index.add_tx(Arc::new(coinbase.clone()));
        index.add_tx(Arc::new(tx1.clone()));
        index.add_tx(Arc::new(coinbase2.clone()));
        index.add_tx(Arc::new(tx2.clone()));

        let ctx = Arc::new(PipelineContext::new());
        let mut engine = Engine::new(ctx.clone(), Arc::new(index));

        // === Build the cyclic pipeline ===

        let all_txs = AllTxs::new(&ctx);

        // Filter out coinjoins (none in this test, but for correctness)
        let coinjoin_mask = IsCoinJoin::new(all_txs.clone());
        let non_coinjoin = all_txs.filter_with_mask(coinjoin_mask.negate());

        // MIH clustering: all inputs of a tx are clustered
        let mih_clustering = MultiInputHeuristic::new(non_coinjoin.clone());

        // Create placeholder for global clustering -- the cyclic dependency
        let global_clustering = Placeholder::<Clustering>::new(&ctx);

        // IsUnilateral uses the placeholder (creates dependency on cycle)
        let unilateral_mask =
            IsUnilateral::with_clustering(non_coinjoin.clone(), global_clustering.as_expr());

        // Filter to txs that are unilateral (all inputs clustered)
        let unilateral_txs = non_coinjoin.filter_with_mask(unilateral_mask);

        // Get change mask for unilateral txs
        let unilateral_change_mask = ChangeIdentification::new(unilateral_txs.clone());

        // Change clustering: cluster change outputs with inputs
        let change_clustering = ChangeClustering::new(unilateral_txs, unilateral_change_mask);

        // Combine MIH and change clustering
        let combined = mih_clustering.join(change_clustering);

        // Close the cycle
        global_clustering.unify(combined);

        // === Run to fixpoint ===
        let iterations = engine.run_to_fixpoint();
        println!("Fixpoint reached in {} iterations", iterations);

        // === Verify results ===
        let result = engine.eval(&global_clustering.as_expr());

        // tx1 has single input, so MIH doesn't cluster anything for it
        // But tx1's change (vout=1) should be clustered with coinbase (vout=0)
        // because tx1 is unilateral (single input = trivially unilateral)
        let coinbase_out = TxOutId::new(TxId(0), 0);
        let tx1_change = TxOutId::new(TxId(1), 1);

        // tx1 is unilateral, so change clustering applies
        assert_eq!(
            result.find(coinbase_out),
            result.find(tx1_change),
            "tx1's change should be clustered with coinbase"
        );

        // tx2 has two inputs: tx1's change and coinbase2
        // MIH should cluster these together
        let tx1_change_as_input = TxOutId::new(TxId(1), 1);
        let coinbase2_out = TxOutId::new(TxId(2), 0);

        assert_eq!(
            result.find(tx1_change_as_input),
            result.find(coinbase2_out),
            "tx2's inputs should be clustered by MIH"
        );

        // By transitivity: coinbase, tx1_change, coinbase2 should all be clustered
        assert_eq!(
            result.find(coinbase_out),
            result.find(coinbase2_out),
            "All should be in same cluster by transitivity"
        );

        // tx2's change should also be clustered (if tx2 is unilateral)
        let tx2_change = TxOutId::new(TxId(3), 1);
        // After MIH runs, tx2's inputs are clustered, so it becomes unilateral
        // Then change clustering should cluster tx2's change with its inputs
        assert_eq!(
            result.find(tx2_change),
            result.find(coinbase2_out),
            "tx2's change should be clustered after fixpoint"
        );
    }

    /// Test that demonstrates the user's original example pattern
    // TODO: test is undeterministic
    #[test]
    fn test_user_example_pattern() {
        let (index, spending_tx, payment_output, change_output) = setup_test_fixture();

        let ctx = Arc::new(PipelineContext::new());
        let mut engine = Engine::new(ctx.clone(), Arc::new(index));

        let all_known_txs = AllTxs::new(&ctx);

        // Symbols of computation that will occur
        let is_coinjoin_mask = IsCoinJoin::new(all_known_txs.clone());
        let non_coinjoin = all_known_txs.filter_with_mask(is_coinjoin_mask.negate());
        let mih_clustering = MultiInputHeuristic::new(non_coinjoin.clone());

        // Placeholder for global clustering (the "anonymous variable" that will be unified)
        let global_clustering = Placeholder::<Clustering>::new(&ctx);

        // IsUnilateral depends on global_clustering placeholder
        let unilateral_txs_mask =
            IsUnilateral::with_clustering(non_coinjoin.clone(), global_clustering.as_expr());

        // Filter to get transactions that are unilateral and have change
        let txs_with_change_and_unilateral = non_coinjoin.filter_with_mask(unilateral_txs_mask);

        // Get change mask for these filtered txs
        let filtered_change_mask =
            ChangeIdentification::new(txs_with_change_and_unilateral.clone());

        // Build change clustering
        let change_clustering =
            ChangeClustering::new(txs_with_change_and_unilateral, filtered_change_mask);

        // Join change clustering with MIH
        let combined_clustering = change_clustering.join(mih_clustering);

        // Unify: global_clustering IS combined_clustering
        global_clustering.unify(combined_clustering);

        // Run to fixpoint
        let iterations = engine.run_to_fixpoint();
        println!("User example reached fixpoint in {} iterations", iterations);

        // Verify
        let result = engine.eval(&global_clustering.as_expr());

        let input1 = spending_tx.spent_coins[0];
        let input2 = spending_tx.spent_coins[1];

        // MIH should cluster the two inputs
        assert_eq!(result.find(input1), result.find(input2));

        // Change should be clustered with inputs (after MIH makes it unilateral)
        assert_eq!(result.find(change_output), result.find(input1));

        // Payment should NOT be clustered
        assert_ne!(result.find(payment_output), result.find(input1));
    }
}
