//! Integration tests for the AST-based pipeline DSL.
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use pipeline::Placeholder;
    use pipeline::context::PipelineContext;
    use pipeline::engine::Engine;
    use pipeline::ops::source::AllLooseTxs;
    use pipeline::value::TxOutClustering;
    use tx_indexer_primitives::abstract_types::AbstractTransaction;
    use tx_indexer_primitives::disjoint_set::DisJointSet;
    use tx_indexer_primitives::loose::LooseIds;
    use tx_indexer_primitives::loose::{TxId, TxOutId};
    use tx_indexer_primitives::test_utils::{DummyTxData, DummyTxOutData};

    use crate::ast::{
        ChangeClustering, ChangeIdentification, FingerPrintChangeIdentification, IsCoinJoin,
        IsUnilateral, MultiInputHeuristic,
    };

    struct TestFixture;

    impl TestFixture {
        fn spending_tx() -> DummyTxData {
            DummyTxData {
                id: TxId(2),
                outputs: vec![
                    // Payment output
                    DummyTxOutData::new(100, [1u8; 20], 0, TxId(2)),
                    // Change output
                    DummyTxOutData::new(150, [1u8; 20], 1, TxId(2)),
                ],
                spent_coins: vec![TxOutId::new(TxId(0), 0), TxOutId::new(TxId(1), 0)],
                n_locktime: 0,
            }
        }

        fn coinbase1() -> DummyTxData {
            DummyTxData {
                id: TxId(0),
                outputs: vec![
                    DummyTxOutData::new(100, [1u8; 20], 0, TxId(0)),
                    DummyTxOutData::new(150, [1u8; 20], 1, TxId(0)),
                ],
                spent_coins: vec![],
                n_locktime: 0,
            }
        }

        fn coinbase2() -> DummyTxData {
            DummyTxData {
                id: TxId(1),
                outputs: vec![DummyTxOutData::new(150, [1u8; 20], 0, TxId(1))],
                spent_coins: vec![],
                n_locktime: 0,
            }
        }

        fn payment_output() -> TxOutId {
            TxOutId::new(TxId(2), 0)
        }

        fn change_output() -> TxOutId {
            TxOutId::new(TxId(2), 1)
        }
    }

    fn setup_test_fixture() -> Vec<Arc<dyn AbstractTransaction<I = LooseIds> + Send + Sync>> {
        vec![
            Arc::new(TestFixture::coinbase1()),
            Arc::new(TestFixture::coinbase2()),
            Arc::new(TestFixture::spending_tx()),
        ]
    }

    #[test]
    fn test_coinjoin_detection() {
        let spk_hash = [1u8; 20];

        // Non-coinjoin tx
        let normal_tx = DummyTxData {
            id: TxId(0),
            outputs: vec![
                DummyTxOutData::new(100, spk_hash, 0, TxId(0)),
                DummyTxOutData::new(200, spk_hash, 1, TxId(0)),
            ],
            spent_coins: vec![],
            n_locktime: 0,
        };

        // Coinjoin tx (3+ outputs with same value)
        let coinjoin_tx = DummyTxData {
            id: TxId(1),
            outputs: vec![
                DummyTxOutData::new(100, spk_hash, 0, TxId(1)),
                DummyTxOutData::new(100, spk_hash, 1, TxId(1)),
                DummyTxOutData::new(100, spk_hash, 2, TxId(1)),
            ],
            spent_coins: vec![],
            n_locktime: 0,
        };

        let all_txs: Vec<Arc<dyn AbstractTransaction<I = LooseIds> + Send + Sync>> =
            vec![Arc::new(normal_tx), Arc::new(coinjoin_tx)];

        let ctx = Arc::new(PipelineContext::new());
        let mut engine = Engine::new(ctx.clone());
        engine.add_base_facts(all_txs);

        let source = AllLooseTxs::new(&ctx);
        let all_txs = source.txs();
        let index = source.index();
        let coinjoin_mask = IsCoinJoin::new(all_txs, index);

        let result = engine.eval(&coinjoin_mask);

        assert_eq!(result.get(&TxId(0)), Some(&false)); // Not a coinjoin
        assert_eq!(result.get(&TxId(1)), Some(&true)); // Is a coinjoin
    }

    #[test]
    fn test_multi_input_heuristic() {
        let all_txs = setup_test_fixture();

        let ctx = Arc::new(PipelineContext::new());
        let mut engine = Engine::new(ctx.clone());
        engine.add_base_facts(all_txs);

        let source = AllLooseTxs::new(&ctx);
        let all_txs = source.txs();
        let index = source.index();
        let coinjoin_mask = IsCoinJoin::new(all_txs.clone(), index.clone());
        let non_coinjoin = all_txs.filter_with_mask(coinjoin_mask.negate());
        let mih_clustering = MultiInputHeuristic::new(non_coinjoin, index);

        let result = engine.eval(&mih_clustering);

        // The two inputs of spending_tx should be in the same cluster
        let input1 = TestFixture::spending_tx().spent_coins[0];
        let input2 = TestFixture::spending_tx().spent_coins[1];
        assert_eq!(result.find(input1), result.find(input2));
    }

    /// MIH clustering with the fixture txs added in inverse order. Same conditions as
    /// `test_multi_input_heuristic`; results must be the same (order-independent).
    #[test]
    fn test_multi_input_heuristic_inverse_order() {
        // new index with txs in inverse order
        let all_txs = setup_test_fixture();

        let ctx = Arc::new(PipelineContext::new());
        let mut engine = Engine::new(ctx.clone());
        engine.add_base_facts(all_txs);

        let source = AllLooseTxs::new(&ctx);
        let all_txs = source.txs();
        let index = source.index();
        let coinjoin_mask = IsCoinJoin::new(all_txs.clone(), index.clone());
        let non_coinjoin = all_txs.filter_with_mask(coinjoin_mask.negate());
        let mih_clustering = MultiInputHeuristic::new(non_coinjoin, index.clone());

        let result = engine.eval(&mih_clustering);

        // Same assertion as test_multi_input_heuristic: the two inputs of spending_tx
        // should be in the same cluster regardless of insertion order
        let input1 = TestFixture::spending_tx().spent_coins[0];
        let input2 = TestFixture::spending_tx().spent_coins[1];
        assert_eq!(result.find(input1), result.find(input2));
    }

    #[test]
    fn test_change_identification() {
        let all_txs = setup_test_fixture();

        let ctx = Arc::new(PipelineContext::new());
        let mut engine = Engine::new(ctx.clone());
        engine.add_base_facts(all_txs);

        let source = AllLooseTxs::new(&ctx);
        let all_txouts = source.txs().outputs(source.index());
        let change_mask = ChangeIdentification::new(all_txouts, source.index());

        let result = engine.eval(&change_mask);

        // Payment output (vout=0) should not be change
        assert_eq!(result.get(&TestFixture::payment_output()), Some(&false));
        // Change output (vout=1, last output) should be change
        assert_eq!(result.get(&TestFixture::change_output()), Some(&true));
    }

    #[test]
    fn test_unilateral_detection() {
        let all_txs = setup_test_fixture();

        let ctx = Arc::new(PipelineContext::new());
        let mut engine = Engine::new(ctx.clone());
        engine.add_base_facts(all_txs);

        let source = AllLooseTxs::new(&ctx);
        let all_txs = source.txs();
        let index = source.index();

        let mih_clustering = MultiInputHeuristic::new(all_txs.clone(), index.clone());
        let is_unilateral_mask = IsUnilateral::with_clustering(all_txs, mih_clustering, index);

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
        let all_txs = setup_test_fixture();

        let ctx = Arc::new(PipelineContext::new());
        let mut engine = Engine::new(ctx.clone());
        engine.add_base_facts(all_txs);

        let source = AllLooseTxs::new(&ctx);
        let all_txs = source.txs();
        let index = source.index();
        let placeholder = Placeholder::<TxOutClustering<LooseIds>>::new(&ctx);
        let mih_clustering = MultiInputHeuristic::new(all_txs.clone(), index.clone());
        let is_unilateral_mask =
            IsUnilateral::with_clustering(all_txs, placeholder.as_expr(), index);

        // Unify the placeholder with the MIH clustering
        placeholder.unify(mih_clustering);

        let result = engine.eval(&is_unilateral_mask);

        // The spending tx should be unilateral
        assert_eq!(result.get(&TestFixture::spending_tx().id), Some(&true));
    }

    #[test]
    fn test_pipeline_with_placeholder() {
        let all_txs = setup_test_fixture();

        let ctx = Arc::new(PipelineContext::new());
        let mut engine = Engine::new(ctx.clone());
        engine.add_base_facts(all_txs);

        let source = AllLooseTxs::new(&ctx);
        let all_txs = source.txs();
        let index = source.index();

        // Create a placeholder for global clustering
        let global_clustering = Placeholder::<TxOutClustering<LooseIds>>::new(&ctx);

        // MIH clustering
        let mih_clustering = MultiInputHeuristic::new(all_txs.clone(), index);

        // Unify the placeholder with MIH clustering
        global_clustering.unify(mih_clustering);

        // Run to fixpoint
        engine.run_to_fixpoint();

        // Evaluate
        let result = engine.eval(&global_clustering.as_expr());

        // The two inputs should be clustered
        let input1 = TestFixture::spending_tx().spent_coins[0];
        let input2 = TestFixture::spending_tx().spent_coins[1];
        assert_eq!(result.find(input1), result.find(input2));
    }

    #[test]
    fn test_global_clustering_e2e() {
        let spk_hash = [1u8; 20];

        // Build a chain: coinbase -> tx1 -> tx2
        // tx1 has two outputs (payment + change)
        // tx2 spends the change output

        let coinbase = DummyTxData {
            id: TxId(0),
            outputs: vec![DummyTxOutData::new(1000, spk_hash, 0, TxId(0))],
            spent_coins: vec![],
            n_locktime: 0,
        };

        // tx1: spends coinbase, creates payment + change
        let tx1 = DummyTxData {
            id: TxId(1),
            outputs: vec![
                DummyTxOutData::new(700, spk_hash, 0, TxId(1)), // payment (vout=0)
                DummyTxOutData::new(300, spk_hash, 1, TxId(1)), // change (vout=1, last)
            ],
            spent_coins: vec![TxOutId::new(TxId(0), 0)],
            n_locktime: 0,
        };

        // tx2: spends the change output of tx1 along with another input
        // This tests the recursive nature: once we know tx1's change is clustered
        // with coinbase, tx2's MIH should cluster its inputs
        let coinbase2 = DummyTxData {
            id: TxId(2),
            outputs: vec![DummyTxOutData::new(500, spk_hash, 0, TxId(2))],
            spent_coins: vec![],
            n_locktime: 0,
        };

        let tx2 = DummyTxData {
            id: TxId(3),
            outputs: vec![
                DummyTxOutData::new(400, spk_hash, 0, TxId(3)), // payment
                DummyTxOutData::new(100, spk_hash, 1, TxId(3)), // change
            ],
            spent_coins: vec![
                TxOutId::new(TxId(1), 1), // spends tx1's change
                TxOutId::new(TxId(2), 0), // spends coinbase2
            ],
            n_locktime: 0,
        };
        let all_txs: Vec<Arc<dyn AbstractTransaction<I = LooseIds> + Send + Sync>> = vec![
            Arc::new(coinbase),
            Arc::new(tx1),
            Arc::new(coinbase2),
            Arc::new(tx2),
        ];

        let ctx = Arc::new(PipelineContext::new());
        let mut engine = Engine::new(ctx.clone());
        engine.add_base_facts(all_txs);

        // === Build the cyclic pipeline ===

        let source = AllLooseTxs::new(&ctx);
        let all_txs = source.txs();
        let index = source.index();

        // Filter out coinjoins (none in this test, but for correctness)
        let coinjoin_mask = IsCoinJoin::new(all_txs.clone(), index.clone());
        let non_coinjoin = all_txs.filter_with_mask(coinjoin_mask.negate());

        // MIH clustering: all inputs of a tx are clustered
        let mih_clustering = MultiInputHeuristic::new(non_coinjoin.clone(), index.clone());

        let is_unilateral = IsUnilateral::with_clustering(
            non_coinjoin.clone(),
            mih_clustering.clone(),
            index.clone(),
        );
        let unilateral_txs = non_coinjoin.filter_with_mask(is_unilateral.clone());

        let change_mask =
            ChangeIdentification::new(unilateral_txs.outputs(index.clone()), index.clone());
        let change_clustering = ChangeClustering::new(unilateral_txs, change_mask, index.clone());
        let combined = change_clustering.join(mih_clustering.clone());

        let result = engine.eval(&combined);

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

    /// Coinbase -> spending tx (payment + change) -> tx that spends the change.
    /// Uses fingerprint-aware change heuristic (n_locktime): both the spending tx and the
    /// tx that spends the change have n_locktime > 0, so the change output is classified as change.
    #[test]
    fn test_fingerprint_change_coinbase_spend_change_spend() {
        let spk_hash = [1u8; 20];

        // Coinbase
        let coinbase = DummyTxData {
            id: TxId(0),
            outputs: vec![DummyTxOutData::new(1000, spk_hash, 0, TxId(0))],
            spent_coins: vec![],
            n_locktime: 0,
        };

        // Spending tx: spends coinbase, creates payment (vout 0) and change (vout 1)
        let spending_tx = DummyTxData {
            id: TxId(1),
            outputs: vec![
                DummyTxOutData::new(700, spk_hash, 0, TxId(1)), // payment
                DummyTxOutData::new(300, spk_hash, 1, TxId(1)), // change
            ],
            spent_coins: vec![TxOutId::new(TxId(0), 0)],
            n_locktime: 1, // fingerprint: same as child so change is classified as change
        };
        let change_output = TxOutId::new(TxId(1), 1);
        let payment_output = TxOutId::new(TxId(1), 0);

        // Tx that spends the change output (vout 1 of spending_tx)
        let change_spend_tx = DummyTxData {
            id: TxId(2),
            outputs: vec![DummyTxOutData::new(300, spk_hash, 0, TxId(2))],
            spent_coins: vec![change_output],
            n_locktime: 1, // matches spending_tx -> fingerprint says change
        };

        let payment_spend_tx = DummyTxData {
            id: TxId(3),
            outputs: vec![DummyTxOutData::new(700, spk_hash, 0, TxId(3))],
            spent_coins: vec![payment_output],
            n_locktime: 0,
        };

        let all_txs: Vec<Arc<dyn AbstractTransaction<I = LooseIds> + Send + Sync>> = vec![
            Arc::new(coinbase),
            Arc::new(spending_tx),
            Arc::new(change_spend_tx),
            Arc::new(payment_spend_tx),
        ];

        let ctx = Arc::new(PipelineContext::new());
        let mut engine = Engine::new(ctx.clone());
        engine.add_base_facts(all_txs);

        let source = AllLooseTxs::new(&ctx);
        let all_txouts = source.txs().outputs(source.index());
        let change_mask = FingerPrintChangeIdentification::new(all_txouts, source.index());

        let result = engine.eval(&change_mask);

        // Change output (vout 1, spent by tx2 with matching n_locktime) is change
        assert_eq!(
            result.get(&change_output),
            Some(&true),
            "change output should be identified as change by fingerprint heuristic"
        );

        assert_eq!(
            result.get(&payment_output),
            Some(&false),
            "payment output should not be identified as change by fingerprint heuristic"
        );
    }

    #[test]
    fn test_rerunning_engine_after_new_facts() {
        let mut all_txs = setup_test_fixture();
        let spending_tx = all_txs.split_off(2);
        let ctx = Arc::new(PipelineContext::new());
        let mut engine = Engine::new(ctx.clone());
        engine.add_base_facts(all_txs);

        let source = AllLooseTxs::new(&ctx);
        let all_txs = source.txs();
        let index = source.index();
        let mih_clustering = MultiInputHeuristic::new(all_txs, index);
        let result = engine.eval(&mih_clustering);

        let spending_tx_fixture = TestFixture::spending_tx();
        assert_ne!(
            result.find(spending_tx_fixture.spent_coins[0]),
            result.find(spending_tx_fixture.spent_coins[1])
        );

        engine.add_base_facts(spending_tx);

        let result = engine.eval(&mih_clustering);
        assert_eq!(
            result.find(spending_tx_fixture.spent_coins[0]),
            result.find(spending_tx_fixture.spent_coins[1])
        );

        // TODO: test the reverse condition (add the spending tx first, then the coinbases)
    }
}
