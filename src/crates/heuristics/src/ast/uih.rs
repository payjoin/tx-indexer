//! Unnecessary Input Heuristics (UIH) from the PayJoin paper.
//!
//! BlockSci definitions:
//! - UIH1 (Optimal change): smallest output is likely change when min(out) < min(in).
//! - UIH2 (Unnecessary input): transaction could pay outputs without the smallest input.

use std::collections::HashMap;

use tx_indexer_pipeline::{
    engine::EvalContext,
    expr::Expr,
    node::{Node, NodeId},
    value::{TxMask, TxOutMask, TxOutSet, TxSet},
};
use tx_indexer_primitives::{
    handle::SpendableTxConstituent,
    unified::{AnyOutId, AnyTxId},
};

use crate::uih::UnnecessaryInputHeuristic;

/// Node that implements UIH1 (Optimal change heuristic).
///
/// For each output, returns `true` if its value is less than the minimum input
/// value of its containing transaction.
pub struct UnnecessaryInputHeuristic1Node {
    input: Expr<TxOutSet>,
}

impl UnnecessaryInputHeuristic1Node {
    pub fn new(input: Expr<TxOutSet>) -> Self {
        Self { input }
    }
}

impl Node for UnnecessaryInputHeuristic1Node {
    type OutputValue = TxOutMask;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.input.id()]
    }

    fn evaluate(&self, ctx: &EvalContext) -> HashMap<AnyOutId, bool> {
        let txouts = ctx.get_or_default(&self.input);
        let mut result = HashMap::new();

        for output_id in txouts.iter() {
            let Ok(spendable) =
                SpendableTxConstituent::try_new(output_id.with(ctx.unified_storage()))
            else {
                result.insert(*output_id, false);
                continue;
            };
            result.insert(
                *output_id,
                UnnecessaryInputHeuristic::is_uih1_candidate(spendable),
            );
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
    /// Returns a mask over outputs where `true` indicates a UIH1 candidate
    /// (output value < min input value of its containing transaction).
    pub fn new(input: Expr<TxOutSet>) -> Expr<TxOutMask> {
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

        for tx_id in tx_ids.iter() {
            let tx = tx_id.with(ctx.unified_storage());

            result.insert(*tx_id, UnnecessaryInputHeuristic::is_uih2(&tx));
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
        UnifiedStorage,
        loose::{LooseIndexBuilder, TxId, TxOutId},
        test_utils::DummyTxData,
        traits::abstract_types::AbstractTransaction,
        unified::{AnyOutId, AnyTxId},
    };

    use super::{UnnecessaryInputHeuristic1, UnnecessaryInputHeuristic2};
    use crate::ast::tests::ast_tests::TestFixture;

    fn engine_with_loose(
        ctx: Arc<PipelineContext>,
        txs: Vec<Arc<dyn AbstractTransaction + Send + Sync>>,
    ) -> Engine {
        let mut builder = LooseIndexBuilder::new();
        for tx in txs {
            builder.add_tx(tx);
        }
        let unified = UnifiedStorage::from(builder);
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
            Arc::new(DummyTxData::new_with_amounts(vec![100])),
            Arc::new(DummyTxData::new_with_amounts(vec![200])),
            Arc::new(DummyTxData::new_with_spent(
                vec![50, 250],
                vec![TxOutId::new(TxId(1), 0), TxOutId::new(TxId(2), 0)],
            )),
        ]
    }

    fn setup_uih1_no_candidate_fixture() -> Vec<Arc<dyn AbstractTransaction + Send + Sync>> {
        vec![
            Arc::new(DummyTxData::new_with_amounts(vec![50])),
            Arc::new(DummyTxData::new_with_amounts(vec![100])),
            Arc::new(DummyTxData::new_with_spent(
                vec![80, 70],
                vec![TxOutId::new(TxId(1), 0), TxOutId::new(TxId(2), 0)],
            )),
        ]
    }

    fn setup_uih1_tie_fixture() -> Vec<Arc<dyn AbstractTransaction + Send + Sync>> {
        vec![
            Arc::new(DummyTxData::new_with_amounts(vec![100])),
            Arc::new(DummyTxData::new_with_amounts(vec![200])),
            Arc::new(DummyTxData::new_with_spent(
                vec![50, 50],
                vec![TxOutId::new(TxId(1), 0), TxOutId::new(TxId(2), 0)],
            )),
        ]
    }

    fn setup_uih2_no_unnecessary_fixture() -> Vec<Arc<dyn AbstractTransaction + Send + Sync>> {
        vec![
            Arc::new(DummyTxData::new_with_amounts(vec![100])),
            Arc::new(DummyTxData::new_with_amounts(vec![200])),
            Arc::new(DummyTxData::new_with_spent(
                vec![250, 40],
                vec![TxOutId::new(TxId(1), 0), TxOutId::new(TxId(2), 0)],
            )),
        ]
    }

    fn setup_uih2_boundary_fixture() -> Vec<Arc<dyn AbstractTransaction + Send + Sync>> {
        vec![
            Arc::new(DummyTxData::new_with_amounts(vec![100])),
            Arc::new(DummyTxData::new_with_amounts(vec![200])),
            Arc::new(DummyTxData::new_with_spent(
                vec![200, 0],
                vec![TxOutId::new(TxId(1), 0), TxOutId::new(TxId(2), 0)],
            )),
        ]
    }

    fn setup_uih_mixed_fixture() -> Vec<Arc<dyn AbstractTransaction + Send + Sync>> {
        vec![
            Arc::new(DummyTxData::new_with_amounts(vec![100])),
            Arc::new(DummyTxData::new_with_amounts(vec![200])),
            Arc::new(DummyTxData::new_with_amounts(vec![50])),
            Arc::new(DummyTxData::new_with_amounts(vec![260])),
            Arc::new(DummyTxData::new_with_spent(
                vec![200, 30],
                vec![TxOutId::new(TxId(1), 0), TxOutId::new(TxId(2), 0)],
            )),
            Arc::new(DummyTxData::new_with_spent(
                vec![270, 10],
                vec![TxOutId::new(TxId(3), 0), TxOutId::new(TxId(4), 0)],
            )),
        ]
    }

    #[test]
    fn test_uih1_smallest_output_is_change() {
        let all_txs = setup_uih1_qualifying_fixture();
        let ctx = Arc::new(PipelineContext::new());
        let mut engine = engine_with_loose(ctx.clone(), all_txs);

        let source = AllLooseTxs::new(&ctx);
        let uih1 = UnnecessaryInputHeuristic1::new(source.txs().outputs());
        let result = engine.eval(&uih1);

        let smallest_out = AnyOutId::from(TxOutId::new(TxId(3), 0));
        assert_eq!(
            result.get(&smallest_out),
            Some(&true),
            "UIH1 should flag the smallest output (value 50)"
        );
        assert_eq!(result.values().filter(|&&v| v).count(), 1);
    }

    #[test]
    fn test_uih1_no_change_candidate() {
        let all_txs = setup_uih1_no_candidate_fixture();
        let ctx = Arc::new(PipelineContext::new());
        let mut engine = engine_with_loose(ctx.clone(), all_txs);

        let source = AllLooseTxs::new(&ctx);
        let uih1 = UnnecessaryInputHeuristic1::new(source.txs().outputs());
        let result = engine.eval(&uih1);

        assert!(
            result.values().all(|&v| !v),
            "UIH1 should have no candidates when min(out) >= min(in)"
        );
    }

    #[test]
    fn test_uih1_tie_smallest_outputs() {
        let all_txs = setup_uih1_tie_fixture();
        let ctx = Arc::new(PipelineContext::new());
        let mut engine = engine_with_loose(ctx.clone(), all_txs);

        let source = AllLooseTxs::new(&ctx);
        let uih1 = UnnecessaryInputHeuristic1::new(source.txs().outputs());
        let result = engine.eval(&uih1);

        assert_eq!(
            result.get(&AnyOutId::from(TxOutId::new(TxId(3), 0))),
            Some(&true)
        );
        assert_eq!(
            result.get(&AnyOutId::from(TxOutId::new(TxId(3), 1))),
            Some(&true)
        );
        assert_eq!(result.values().filter(|&&v| v).count(), 2);
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
        let uih1 = UnnecessaryInputHeuristic1::new(source.txs().outputs());
        let uih2 = UnnecessaryInputHeuristic2::new(source.txs());

        // `.into_owned()` because we hold both results simultaneously below;
        // each `eval` borrows the engine, so we drop the borrows by cloning out.
        let uih1_result = engine.eval(&uih1).into_owned();
        let uih2_result = engine.eval(&uih2).into_owned();

        assert_eq!(
            uih1_result.get(&AnyOutId::from(TxOutId::new(TxId(5), 1))),
            Some(&true),
            "UIH1 should flag tx4's smallest output (vout=1)"
        );
        assert_ne!(
            uih1_result.get(&AnyOutId::from(TxOutId::new(TxId(6), 0))),
            Some(&true),
            "UIH1 should not flag tx5's vout=0 (min(out) >= min(in))"
        );

        // uih2: tx4 true, tx5 false
        assert_eq!(uih2_result.get(&AnyTxId::from(TxId(5))), Some(&true));
        assert_eq!(uih2_result.get(&AnyTxId::from(TxId(6))), Some(&false));
    }
}
