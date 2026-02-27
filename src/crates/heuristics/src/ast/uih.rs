//! Unnecessary Input Heuristics (UIH) from the PayJoin paper.
//!
//! BlockSci definitions:
//! - UIH1 (Optimal change): smallest output is likely change when min(out) < min(in).
//! - UIH2 (Unnecessary input): transaction could pay outputs without the smallest input.

use std::collections::HashSet;

use bitcoin::Amount;
use tx_indexer_pipeline::{
    engine::EvalContext,
    expr::Expr,
    node::{Node, NodeId},
    value::{TxMask, TxOutSet, TxSet},
};
use tx_indexer_primitives::{
    AbstractTxIn, AbstractTxOut,
    unified::{AnyOutId, AnyTxId},
};

/// Node that implements UIH1 (Optimal change heuristic).
///
/// For each transaction where min(output values) < min(input values), adds the
/// smallest output(s) by value to the result set (likely change).
pub struct UnnecessaryInputHeuristic1Node {
    input: Expr<TxSet>,
}

impl UnnecessaryInputHeuristic1Node {
    pub fn new(input: Expr<TxSet>) -> Self {
        Self { input }
    }
}

impl Node for UnnecessaryInputHeuristic1Node {
    type OutputValue = TxOutSet;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.input.id()]
    }

    fn evaluate(&self, ctx: &EvalContext) -> HashSet<AnyOutId> {
        let tx_ids = ctx.get_or_default(&self.input);

        let mut result = HashSet::new();

        for tx_id in &tx_ids {
            let tx = tx_id.with(ctx.unified_storage());

            let outputs: Vec<_> = tx.outputs().map(|o| (o.id(), o.value())).collect();
            if outputs.is_empty() {
                continue;
            }

            let input_values: Vec<Amount> = tx
                .inputs()
                .filter_map(|input| {
                    input
                        .prev_txout_id()
                        .map(|out_id| out_id.with(ctx.unified_storage()).value())
                })
                .collect();
            if input_values.is_empty() {
                continue;
            }

            let min_in = input_values
                .iter()
                .min()
                .copied()
                .expect("non-empty inputs");
            let min_out = outputs
                .iter()
                .map(|(_, v)| *v)
                .min()
                .expect("non-empty outputs");

            if min_out < min_in {
                for (out_id, v) in &outputs {
                    if *v == min_out {
                        result.insert(*out_id);
                    }
                }
            }
        }

        result
    }

    fn name(&self) -> &'static str {
        "UnnecessaryInputHeuristic1"
    }
}

/// Factory for creating a UIH1 expression.
pub struct UnnecessaryInputHeuristic1;

impl UnnecessaryInputHeuristic1 {
    /// Returns the set of outputs that are the smallest by value in each tx
    /// where min(out) < min(in) (BlockSci optimal change heuristic).
    pub fn new(input: Expr<TxSet>) -> Expr<TxOutSet> {
        let ctx = input.context().clone();
        ctx.register(UnnecessaryInputHeuristic1Node::new(input))
    }
}

/// Node that implements UIH2 (Unnecessary input heuristic).
///
/// Flags transactions where (sum_in - min_in) >= (sum_out - min_out), i.e. the
/// largest output could be paid without the smallest input. Fee is ignored.
pub struct UnnecessaryInputHeuristic2Node {
    input: Expr<TxSet>,
}

impl UnnecessaryInputHeuristic2Node {
    pub fn new(input: Expr<TxSet>) -> Self {
        Self { input }
    }
}

impl Node for UnnecessaryInputHeuristic2Node {
    type OutputValue = TxMask;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.input.id()]
    }

    fn evaluate(&self, ctx: &EvalContext) -> std::collections::HashMap<AnyTxId, bool> {
        let tx_ids = ctx.get_or_default(&self.input);

        let mut result = std::collections::HashMap::new();

        for tx_id in &tx_ids {
            let tx = tx_id.with(ctx.unified_storage());

            // TODO: internal method for collecting input values
            let input_values: Vec<Amount> = tx
                .inputs()
                .filter_map(|input| {
                    input
                        .prev_txout_id()
                        .map(|out_id| out_id.with(ctx.unified_storage()).value())
                })
                .collect();

            // TODO: internal method for collecting output values
            let output_values: Vec<Amount> = tx.outputs().map(|o| o.value()).collect();

            let is_uih2 = if input_values.len() < 2 || output_values.is_empty() {
                false
            } else {
                let sum_in = input_values.iter().fold(Amount::from_sat(0), |a, b| a + *b);
                let min_in = input_values.iter().min().copied().expect("len >= 2");
                let sum_out = output_values
                    .iter()
                    .fold(Amount::from_sat(0), |a, b| a + *b);
                let min_out = output_values.iter().min().copied().expect("non-empty");
                (sum_in - min_in) >= (sum_out - min_out)
            };

            result.insert(*tx_id, is_uih2);
        }

        result
    }

    fn name(&self) -> &'static str {
        "UnnecessaryInputHeuristic2"
    }
}

/// Factory for creating a UIH2 expression.
pub struct UnnecessaryInputHeuristic2;

impl UnnecessaryInputHeuristic2 {
    /// Returns a mask over transactions that exhibit unnecessary input
    /// (BlockSci UIH2: largest output could be paid without smallest input).
    pub fn new(input: Expr<TxSet>) -> Expr<TxMask> {
        let ctx = input.context().clone();
        ctx.register(UnnecessaryInputHeuristic2Node::new(input))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tx_indexer_pipeline::{context::PipelineContext, engine::Engine, ops::source::AllLooseTxs};
    use tx_indexer_primitives::{
        UnifiedStorageBuilder,
        loose::{LooseIndexBuilder, TxId, TxOutId},
        test_utils::{DummyTxData, DummyTxOutData},
        traits::abstract_types::AbstractTransaction,
        unified::{AnyOutId, AnyTxId},
    };

    use super::{UnnecessaryInputHeuristic1, UnnecessaryInputHeuristic2};
    use crate::ast::tests::tests::TestFixture;

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

    fn setup_uih1_qualifying_fixture() -> Vec<Arc<dyn AbstractTransaction + Send + Sync>> {
        vec![
            Arc::new(DummyTxData {
                outputs: vec![DummyTxOutData::new_with_amount(100, 0)],
                spent_coins: vec![],
                n_locktime: 0,
            }),
            Arc::new(DummyTxData {
                outputs: vec![DummyTxOutData::new_with_amount(200, 0)],
                spent_coins: vec![],
                n_locktime: 0,
            }),
            Arc::new(DummyTxData {
                outputs: vec![
                    DummyTxOutData::new_with_amount(50, 0),
                    DummyTxOutData::new_with_amount(250, 1),
                ],
                spent_coins: vec![TxOutId::new(TxId(1), 0), TxOutId::new(TxId(2), 0)],
                n_locktime: 0,
            }),
        ]
    }

    fn setup_uih1_no_candidate_fixture() -> Vec<Arc<dyn AbstractTransaction + Send + Sync>> {
        vec![
            Arc::new(DummyTxData {
                outputs: vec![DummyTxOutData::new_with_amount(50, 0)],
                spent_coins: vec![],
                n_locktime: 0,
            }),
            Arc::new(DummyTxData {
                outputs: vec![DummyTxOutData::new_with_amount(100, 0)],
                spent_coins: vec![],
                n_locktime: 0,
            }),
            Arc::new(DummyTxData {
                outputs: vec![
                    DummyTxOutData::new_with_amount(80, 0),
                    DummyTxOutData::new_with_amount(70, 1),
                ],
                spent_coins: vec![TxOutId::new(TxId(1), 0), TxOutId::new(TxId(2), 0)],
                n_locktime: 0,
            }),
        ]
    }

    fn setup_uih1_tie_fixture() -> Vec<Arc<dyn AbstractTransaction + Send + Sync>> {
        vec![
            Arc::new(DummyTxData {
                outputs: vec![DummyTxOutData::new_with_amount(100, 0)],
                spent_coins: vec![],
                n_locktime: 0,
            }),
            Arc::new(DummyTxData {
                outputs: vec![DummyTxOutData::new_with_amount(200, 0)],
                spent_coins: vec![],
                n_locktime: 0,
            }),
            Arc::new(DummyTxData {
                outputs: vec![
                    DummyTxOutData::new_with_amount(50, 0),
                    DummyTxOutData::new_with_amount(50, 1),
                ],
                spent_coins: vec![TxOutId::new(TxId(1), 0), TxOutId::new(TxId(2), 0)],
                n_locktime: 0,
            }),
        ]
    }

    fn setup_uih2_no_unnecessary_fixture() -> Vec<Arc<dyn AbstractTransaction + Send + Sync>> {
        vec![
            Arc::new(DummyTxData {
                outputs: vec![DummyTxOutData::new_with_amount(100, 0)],
                spent_coins: vec![],
                n_locktime: 0,
            }),
            Arc::new(DummyTxData {
                outputs: vec![DummyTxOutData::new_with_amount(200, 0)],
                spent_coins: vec![],
                n_locktime: 0,
            }),
            Arc::new(DummyTxData {
                outputs: vec![
                    DummyTxOutData::new_with_amount(250, 0),
                    DummyTxOutData::new_with_amount(40, 1),
                ],
                spent_coins: vec![TxOutId::new(TxId(1), 0), TxOutId::new(TxId(2), 0)],
                n_locktime: 0,
            }),
        ]
    }

    fn setup_uih2_boundary_fixture() -> Vec<Arc<dyn AbstractTransaction + Send + Sync>> {
        vec![
            Arc::new(DummyTxData {
                outputs: vec![DummyTxOutData::new_with_amount(100, 0)],
                spent_coins: vec![],
                n_locktime: 0,
            }),
            Arc::new(DummyTxData {
                outputs: vec![DummyTxOutData::new_with_amount(200, 0)],
                spent_coins: vec![],
                n_locktime: 0,
            }),
            Arc::new(DummyTxData {
                outputs: vec![
                    DummyTxOutData::new_with_amount(200, 0),
                    DummyTxOutData::new_with_amount(0, 1),
                ],
                spent_coins: vec![TxOutId::new(TxId(1), 0), TxOutId::new(TxId(2), 0)],
                n_locktime: 0,
            }),
        ]
    }

    fn setup_uih_mixed_fixture() -> Vec<Arc<dyn AbstractTransaction + Send + Sync>> {
        vec![
            Arc::new(DummyTxData {
                outputs: vec![DummyTxOutData::new_with_amount(100, 0)],
                spent_coins: vec![],
                n_locktime: 0,
            }),
            Arc::new(DummyTxData {
                outputs: vec![DummyTxOutData::new_with_amount(200, 0)],
                spent_coins: vec![],
                n_locktime: 0,
            }),
            Arc::new(DummyTxData {
                outputs: vec![DummyTxOutData::new_with_amount(50, 0)],
                spent_coins: vec![],
                n_locktime: 0,
            }),
            Arc::new(DummyTxData {
                outputs: vec![DummyTxOutData::new_with_amount(260, 0)],
                spent_coins: vec![],
                n_locktime: 0,
            }),
            Arc::new(DummyTxData {
                outputs: vec![
                    DummyTxOutData::new_with_amount(200, 0),
                    DummyTxOutData::new_with_amount(30, 1),
                ],
                spent_coins: vec![TxOutId::new(TxId(1), 0), TxOutId::new(TxId(2), 0)],
                n_locktime: 0,
            }),
            Arc::new(DummyTxData {
                outputs: vec![
                    DummyTxOutData::new_with_amount(270, 0),
                    DummyTxOutData::new_with_amount(10, 1),
                ],
                spent_coins: vec![TxOutId::new(TxId(3), 0), TxOutId::new(TxId(4), 0)],
                n_locktime: 0,
            }),
        ]
    }

    #[test]
    fn test_uih1_smallest_output_is_change() {
        let all_txs = setup_uih1_qualifying_fixture();
        let ctx = Arc::new(PipelineContext::new());
        let mut engine = engine_with_loose(ctx.clone(), all_txs);

        let source = AllLooseTxs::new(&ctx);
        let uih1 = UnnecessaryInputHeuristic1::new(source.txs());
        let result = engine.eval(&uih1);

        let smallest_out = AnyOutId::from(TxOutId::new(TxId(3), 0));
        assert!(
            result.contains(&smallest_out),
            "UIH1 should contain the smallest output (value 50)"
        );
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_uih1_no_change_candidate() {
        let all_txs = setup_uih1_no_candidate_fixture();
        let ctx = Arc::new(PipelineContext::new());
        let mut engine = engine_with_loose(ctx.clone(), all_txs);

        let source = AllLooseTxs::new(&ctx);
        let uih1 = UnnecessaryInputHeuristic1::new(source.txs());
        let result = engine.eval(&uih1);

        assert!(
            result.is_empty(),
            "UIH1 should be empty when min(out) >= min(in)"
        );
    }

    #[test]
    fn test_uih1_tie_smallest_outputs() {
        let all_txs = setup_uih1_tie_fixture();
        let ctx = Arc::new(PipelineContext::new());
        let mut engine = engine_with_loose(ctx.clone(), all_txs);

        let source = AllLooseTxs::new(&ctx);
        let uih1 = UnnecessaryInputHeuristic1::new(source.txs());
        let result = engine.eval(&uih1);

        assert!(result.contains(&AnyOutId::from(TxOutId::new(TxId(3), 0))));
        assert!(result.contains(&AnyOutId::from(TxOutId::new(TxId(3), 1))));
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_uih2_unnecessary_input() {
        let all_txs = setup_test_fixture();
        let ctx = Arc::new(PipelineContext::new());
        let mut engine = engine_with_loose(ctx.clone(), all_txs);

        let source = AllLooseTxs::new(&ctx);
        let uih2 = UnnecessaryInputHeuristic2::new(source.txs());
        let result = engine.eval(&uih2);

        assert_eq!(
            result.get(&AnyTxId::from(TxId(3))),
            Some(&true),
            "Tx with unnecessary input should be flagged UIH2"
        );
    }

    #[test]
    fn test_uih2_no_unnecessary_input() {
        let all_txs = setup_uih2_no_unnecessary_fixture();
        let ctx = Arc::new(PipelineContext::new());
        let mut engine = engine_with_loose(ctx.clone(), all_txs);

        let source = AllLooseTxs::new(&ctx);
        let uih2 = UnnecessaryInputHeuristic2::new(source.txs());
        let result = engine.eval(&uih2);

        assert_eq!(
            result.get(&AnyTxId::from(TxId(3))),
            Some(&false),
            "Tx where (sum_in - min_in) < (sum_out - min_out) should not be UIH2"
        );
    }

    #[test]
    fn test_uih2_single_input_false() {
        let all_txs: Vec<Arc<dyn AbstractTransaction + Send + Sync>> = vec![
            Arc::new(TestFixture::coinbase2()),
            Arc::new(TestFixture::single_input_spending_tx()),
        ];
        let ctx = Arc::new(PipelineContext::new());
        let mut engine = engine_with_loose(ctx.clone(), all_txs);

        let source = AllLooseTxs::new(&ctx);
        let uih2 = UnnecessaryInputHeuristic2::new(source.txs());
        let result = engine.eval(&uih2);

        // Loose index assigns ids by insertion order (1, 2, …); single-input tx is 2nd → TxId(2)
        assert_eq!(
            result.get(&AnyTxId::from(TxId(2))),
            Some(&false),
            "Single-input tx should not be flagged UIH2"
        );
    }

    #[test]
    fn test_uih2_boundary_case() {
        let all_txs = setup_uih2_boundary_fixture();
        let ctx = Arc::new(PipelineContext::new());
        let mut engine = engine_with_loose(ctx.clone(), all_txs);

        let source = AllLooseTxs::new(&ctx);
        let uih2 = UnnecessaryInputHeuristic2::new(source.txs());
        let result = engine.eval(&uih2);

        assert_eq!(
            result.get(&AnyTxId::from(TxId(3))),
            Some(&true),
            "Boundary case should be flagged UIH2 (>=)"
        );
    }

    #[test]
    fn test_uih_mixed_cases() {
        let all_txs = setup_uih_mixed_fixture();
        let ctx = Arc::new(PipelineContext::new());
        let mut engine = engine_with_loose(ctx.clone(), all_txs);

        let source = AllLooseTxs::new(&ctx);
        let uih1 = UnnecessaryInputHeuristic1::new(source.txs());
        let uih2 = UnnecessaryInputHeuristic2::new(source.txs());

        let uih1_result = engine.eval(&uih1);
        let uih2_result = engine.eval(&uih2);

        assert!(
            uih1_result.contains(&AnyOutId::from(TxOutId::new(TxId(5), 1))),
            "UIH1 should flag tx4's smallest output (vout=1)"
        );
        assert!(
            !uih1_result.contains(&AnyOutId::from(TxOutId::new(TxId(6), 0))),
            "UIH1 should not flag tx5 (min(out) >= min(in))"
        );

        // uih2: tx4 true, tx5 false
        assert_eq!(uih2_result.get(&AnyTxId::from(TxId(5))), Some(&true));
        assert_eq!(uih2_result.get(&AnyTxId::from(TxId(6))), Some(&false));
    }
}
