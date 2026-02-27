//! Integration tests for the AST-based pipeline DSL.
#[cfg(test)]
pub(crate) mod tests {
    use std::sync::Arc;

    use tx_indexer_disjoint_set::DisJointSet;
    use tx_indexer_pipeline::{context::PipelineContext, engine::Engine, ops::source::AllLooseTxs};
    use tx_indexer_primitives::{
        UnifiedStorageBuilder,
        loose::LooseIndexBuilder,
        loose::{TxId, TxOutId},
        test_utils::{DummyTxData, DummyTxOutData},
        traits::abstract_types::AbstractTransaction,
        unified::{AnyOutId, AnyTxId},
    };

    use crate::ast::{
        ChangeClustering, ChangeIdentification, FingerPrintChangeIdentification, IsCoinJoin,
        IsUnilateral, MultiInputHeuristic,
    };

    pub struct TestFixture;

    impl TestFixture {
        pub fn spending_tx() -> DummyTxData {
            DummyTxData {
                outputs: vec![
                    // Payment output
                    DummyTxOutData::new(100, [1u8; 20], 0),
                    // Change output
                    DummyTxOutData::new(150, [1u8; 20], 1),
                ],
                spent_coins: vec![TxOutId::new(TxId(1), 0), TxOutId::new(TxId(2), 0)],
                n_locktime: 0,
            }
        }

        pub fn coinbase1() -> DummyTxData {
            DummyTxData {
                outputs: vec![
                    DummyTxOutData::new(100, [1u8; 20], 0),
                    DummyTxOutData::new(150, [1u8; 20], 1),
                ],
                spent_coins: vec![],
                n_locktime: 0,
            }
        }

        pub fn coinbase2() -> DummyTxData {
            DummyTxData {
                outputs: vec![DummyTxOutData::new(150, [1u8; 20], 0)],
                spent_coins: vec![],
                n_locktime: 0,
            }
        }

        pub fn payment_output() -> TxOutId {
            TxOutId::new(TxId(3), 0)
        }

        pub fn change_output() -> TxOutId {
            TxOutId::new(TxId(3), 1)
        }

        /// Single-input spending tx (spends coinbase2). Used for UIH2 single-input tests.
        pub fn single_input_spending_tx() -> DummyTxData {
            DummyTxData {
                outputs: vec![
                    DummyTxOutData::new_with_amount(200, 0),
                    DummyTxOutData::new_with_amount(300, 1),
                ],
                spent_coins: vec![TxOutId::new(TxId(2), 0)],
                n_locktime: 0,
            }
        }
    }

    fn engine_with_loose(
        ctx: Arc<PipelineContext>,
        txs: Vec<Arc<dyn AbstractTransaction + Send + Sync>>,
    ) -> Engine {
        let mut builder = LooseIndexBuilder::new();
        for tx in txs {
            builder.add_tx(tx);
        }
        let unified = UnifiedStorageBuilder::new()
            .with_loose(builder)
            .build()
            .expect("build unified storage");
        Engine::new(ctx, Arc::new(unified))
    }

    fn setup_test_fixture() -> Vec<Arc<dyn AbstractTransaction + Send + Sync>> {
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
            outputs: vec![
                DummyTxOutData::new(100, spk_hash, 0),
                DummyTxOutData::new(200, spk_hash, 1),
            ],
            spent_coins: vec![],
            n_locktime: 0,
        };

        // Coinjoin tx (3+ outputs with same value)
        let coinjoin_tx = DummyTxData {
            outputs: vec![
                DummyTxOutData::new(100, spk_hash, 0),
                DummyTxOutData::new(100, spk_hash, 1),
                DummyTxOutData::new(100, spk_hash, 2),
            ],
            spent_coins: vec![],
            n_locktime: 0,
        };

        let all_txs: Vec<Arc<dyn AbstractTransaction + Send + Sync>> =
            vec![Arc::new(normal_tx), Arc::new(coinjoin_tx)];

        let ctx = Arc::new(PipelineContext::new());
        let mut engine = engine_with_loose(ctx.clone(), all_txs);

        let source = AllLooseTxs::new(&ctx);
        let all_txs = source.txs();
        let coinjoin_mask = IsCoinJoin::new(all_txs);

        let result = engine.eval(&coinjoin_mask);

        assert_eq!(result.get(&AnyTxId::from(TxId(1))), Some(&false)); // Not a coinjoin
        assert_eq!(result.get(&AnyTxId::from(TxId(2))), Some(&true)); // Is a coinjoin
    }

    #[test]
    fn test_multi_input_heuristic() {
        let all_txs = setup_test_fixture();

        let ctx = Arc::new(PipelineContext::new());
        let mut engine = engine_with_loose(ctx.clone(), all_txs);

        let source = AllLooseTxs::new(&ctx);
        let all_txs = source.txs();
        let coinjoin_mask = IsCoinJoin::new(all_txs.clone());
        let non_coinjoin = all_txs.filter_with_mask(coinjoin_mask.negate());
        let mih_clustering = MultiInputHeuristic::new(non_coinjoin);

        let result = engine.eval(&mih_clustering);

        // The two inputs of spending_tx should be in the same cluster
        let input1 = AnyOutId::from(TestFixture::spending_tx().spent_coins[0]);
        let input2 = AnyOutId::from(TestFixture::spending_tx().spent_coins[1]);
        assert_eq!(result.find(input1), result.find(input2));
    }

    /// MIH clustering with the fixture txs added in inverse order. Same conditions as
    /// `test_multi_input_heuristic`; results must be the same (order-independent).
    #[test]
    fn test_multi_input_heuristic_inverse_order() {
        let all_txs = setup_test_fixture();

        let ctx = Arc::new(PipelineContext::new());
        let mut engine = engine_with_loose(ctx.clone(), all_txs);

        let source = AllLooseTxs::new(&ctx);
        let all_txs = source.txs();
        let coinjoin_mask = IsCoinJoin::new(all_txs.clone());
        let non_coinjoin = all_txs.filter_with_mask(coinjoin_mask.negate());
        let mih_clustering = MultiInputHeuristic::new(non_coinjoin);

        let result = engine.eval(&mih_clustering);

        let input1 = AnyOutId::from(TestFixture::spending_tx().spent_coins[0]);
        let input2 = AnyOutId::from(TestFixture::spending_tx().spent_coins[1]);
        assert_eq!(result.find(input1), result.find(input2));
    }

    #[test]
    fn test_change_identification() {
        let all_txs = setup_test_fixture();

        let ctx = Arc::new(PipelineContext::new());
        let mut engine = engine_with_loose(ctx.clone(), all_txs);

        let source = AllLooseTxs::new(&ctx);
        let all_txouts = source.txs().outputs();
        let change_mask = ChangeIdentification::new(all_txouts);

        let result = engine.eval(&change_mask);

        // Payment output (vout=0) should not be change
        assert_eq!(
            result.get(&AnyOutId::from(TestFixture::payment_output())),
            Some(&false)
        );
        // Change output (vout=1) should be change
        assert_eq!(
            result.get(&AnyOutId::from(TestFixture::change_output())),
            Some(&true)
        );
    }

    #[test]
    fn test_fingerprint_change_identification() {
        let all_txs = setup_test_fixture();

        let ctx = Arc::new(PipelineContext::new());
        let mut engine = engine_with_loose(ctx.clone(), all_txs);

        let source = AllLooseTxs::new(&ctx);
        let all_txouts = source.txs().outputs();
        let change_mask = FingerPrintChangeIdentification::new(all_txouts);

        let result = engine.eval(&change_mask);

        assert_eq!(
            result.get(&AnyOutId::from(TestFixture::payment_output())),
            Some(&false)
        );
        assert_eq!(
            result.get(&AnyOutId::from(TestFixture::change_output())),
            Some(&false)
        );
    }

    #[test]
    fn test_is_unilateral() {
        let all_txs = setup_test_fixture();

        let ctx = Arc::new(PipelineContext::new());
        let mut engine = engine_with_loose(ctx.clone(), all_txs);

        let source = AllLooseTxs::new(&ctx);
        let all_txs = source.txs();
        let mih_clustering = MultiInputHeuristic::new(all_txs.clone());
        let all_txouts = all_txs.clone().outputs();
        let change_mask = ChangeIdentification::new(all_txouts.clone());
        let change_clustering = ChangeClustering::new(all_txs.clone(), change_mask);
        let combined = change_clustering.join(mih_clustering);
        let is_unilateral = IsUnilateral::with_clustering(all_txs, combined);

        let result = engine.eval(&is_unilateral);

        assert_eq!(result.get(&AnyTxId::from(TxId(1))), Some(&false));
        assert_eq!(result.get(&AnyTxId::from(TxId(2))), Some(&false));
        assert_eq!(result.get(&AnyTxId::from(TxId(3))), Some(&true));
    }

    #[test]
    fn test_change_clustering() {
        let all_txs = setup_test_fixture();

        let ctx = Arc::new(PipelineContext::new());
        let mut engine = engine_with_loose(ctx.clone(), all_txs);

        let source = AllLooseTxs::new(&ctx);
        let all_txs = source.txs();
        let mih_clustering = MultiInputHeuristic::new(all_txs.clone());
        let all_txouts = all_txs.clone().outputs();
        let change_mask = ChangeIdentification::new(all_txouts);
        let change_clustering = ChangeClustering::new(all_txs, change_mask);
        let combined = change_clustering.join(mih_clustering);

        let result = engine.eval(&combined);

        let change_output = AnyOutId::from(TestFixture::change_output());
        let input0 = AnyOutId::from(TestFixture::spending_tx().spent_coins[0]);
        let input1 = AnyOutId::from(TestFixture::spending_tx().spent_coins[1]);

        assert_eq!(result.find(change_output), result.find(input0));
        assert_eq!(result.find(change_output), result.find(input1));
    }

    #[test]
    fn test_change_clustering_fixed_point() {
        let ctx = Arc::new(PipelineContext::new());

        let txs = vec![
            // Coinbase 0
            DummyTxData {
                outputs: vec![DummyTxOutData::new(1000, [1u8; 20], 0)],
                spent_coins: vec![],
                n_locktime: 0,
            },
            // tx1: spends coinbase 0, produces payment + change
            DummyTxData {
                outputs: vec![
                    DummyTxOutData::new(700, [1u8; 20], 0), // payment
                    DummyTxOutData::new(300, [1u8; 20], 1), // change
                ],
                spent_coins: vec![TxOutId::new(TxId(1), 0)],
                n_locktime: 0,
            },
            // coinbase 2
            DummyTxData {
                outputs: vec![DummyTxOutData::new(500, [1u8; 20], 0)],
                spent_coins: vec![],
                n_locktime: 0,
            },
            // tx3: spends tx1 change + coinbase2
            DummyTxData {
                outputs: vec![
                    DummyTxOutData::new(400, [1u8; 20], 0), // payment
                    DummyTxOutData::new(100, [1u8; 20], 1), // change
                ],
                spent_coins: vec![
                    TxOutId::new(TxId(2), 1), // spends tx1 change
                    TxOutId::new(TxId(3), 0), // spends coinbase2
                ],
                n_locktime: 0,
            },
        ];

        let mut builder = LooseIndexBuilder::new();
        for tx in txs {
            builder.add_tx(Arc::new(tx));
        }
        let unified = UnifiedStorageBuilder::new()
            .with_loose(builder)
            .build()
            .expect("build unified storage");
        let mut engine = Engine::new(ctx.clone(), Arc::new(unified));

        let source = AllLooseTxs::new(&ctx);
        let all_txs = source.txs();
        let all_txouts = all_txs.clone().outputs();

        let change_mask = ChangeIdentification::new(all_txouts);
        let change_clustering = ChangeClustering::new(all_txs, change_mask);

        let result = engine.eval(&change_clustering);

        let coinbase_out = AnyOutId::from(TxOutId::new(TxId(1), 0));
        let tx1_change = AnyOutId::from(TxOutId::new(TxId(2), 1));
        let tx3_change = AnyOutId::from(TxOutId::new(TxId(4), 1));

        assert_eq!(result.find(coinbase_out), result.find(tx1_change));
        assert_eq!(result.find(tx1_change), result.find(tx3_change));
    }

    #[test]
    fn test_change_clustering_no_cycles() {
        let ctx = Arc::new(PipelineContext::new());

        let txs = vec![
            DummyTxData {
                outputs: vec![DummyTxOutData::new(1000, [1u8; 20], 0)],
                spent_coins: vec![],
                n_locktime: 0,
            },
            DummyTxData {
                outputs: vec![
                    DummyTxOutData::new(700, [1u8; 20], 0), // payment
                    DummyTxOutData::new(300, [1u8; 20], 1), // change
                ],
                spent_coins: vec![TxOutId::new(TxId(1), 0)],
                n_locktime: 0,
            },
            DummyTxData {
                outputs: vec![DummyTxOutData::new(300, [1u8; 20], 0)],
                spent_coins: vec![],
                n_locktime: 0,
            },
            DummyTxData {
                outputs: vec![DummyTxOutData::new(700, [1u8; 20], 0)],
                spent_coins: vec![],
                n_locktime: 0,
            },
        ];

        let mut builder = LooseIndexBuilder::new();
        for tx in txs {
            builder.add_tx(Arc::new(tx));
        }
        let unified = UnifiedStorageBuilder::new()
            .with_loose(builder)
            .build()
            .expect("build unified storage");
        let mut engine = Engine::new(ctx.clone(), Arc::new(unified));

        let source = AllLooseTxs::new(&ctx);
        let all_txs = source.txs();
        let all_txouts = all_txs.clone().outputs();
        let change_mask = ChangeIdentification::new(all_txouts);
        let change_clustering = ChangeClustering::new(all_txs, change_mask);

        let result = engine.eval(&change_clustering);

        let change_output = AnyOutId::from(TxOutId::new(TxId(2), 1));
        let payment_output = AnyOutId::from(TxOutId::new(TxId(2), 0));

        assert_ne!(result.find(change_output), result.find(payment_output));
    }
}
