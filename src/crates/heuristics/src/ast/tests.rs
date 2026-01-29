//! Integration tests for the AST-based pipeline DSL.

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use pipeline::context::PipelineContext;
    use pipeline::engine::Engine;
    use pipeline::ops::source::AllTxs;
    use pipeline::Placeholder;
    use pipeline::value::Clustering;
    use tx_indexer_primitives::abstract_types::AbstractTxWrapper;
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
    fn test_full_pipeline() {
        let (mut index, spending_tx, payment_output, change_output) = setup_test_fixture();

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
        let unilateral_mask = IsUnilateral::with_clustering(non_coinjoin.clone(), mih_clustering.clone());

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
        let spk_hash = [1u8; 20];

        // Simple tx with two inputs and a change output
        let prev1 = DummyTxData {
            id: TxId(0),
            outputs: vec![DummyTxOutData::new(100, spk_hash)],
            spent_coins: vec![],
        };
        let prev2 = DummyTxData {
            id: TxId(1),
            outputs: vec![DummyTxOutData::new(100, spk_hash)],
            spent_coins: vec![],
        };
        let spending = DummyTxData {
            id: TxId(2),
            outputs: vec![
                DummyTxOutData::new(50, spk_hash),
                DummyTxOutData::new(50, spk_hash),
            ],
            spent_coins: vec![TxOutId::new(TxId(0), 0), TxOutId::new(TxId(1), 0)],
        };

        let mut index = InMemoryIndex::new();
        index.add_tx(Arc::new(prev1));
        index.add_tx(Arc::new(prev2));
        index.add_tx(Arc::new(spending));

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
        let input1 = TxOutId::new(TxId(0), 0);
        let input2 = TxOutId::new(TxId(1), 0);
        assert_eq!(result.find(input1), result.find(input2));
    }
}
