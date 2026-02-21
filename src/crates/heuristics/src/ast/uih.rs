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
    value::{Index, TxMask, TxOutSet, TxSet},
};
use tx_indexer_primitives::{
    abstract_types::{AbstractTxOut, IdFamily, IntoTxHandle, TxOutIdOps},
    graph_index::IndexedGraph,
};

/// Node that implements UIH1 (Optimal change heuristic).
///
/// For each transaction where min(output values) < min(input values), adds the
/// smallest output(s) by value to the result set (likely change).
pub struct UnnecessaryInputHeuristic1Node<I: IdFamily + 'static, G: IndexedGraph<I> + 'static> {
    input: Expr<TxSet<I>>,
    index: Expr<Index<G>>,
}

impl<I: IdFamily + 'static, G: IndexedGraph<I> + 'static> UnnecessaryInputHeuristic1Node<I, G> {
    pub fn new(input: Expr<TxSet<I>>, index: Expr<Index<G>>) -> Self {
        Self { input, index }
    }
}

impl<I: IdFamily + 'static, G: IndexedGraph<I> + 'static> Node
    for UnnecessaryInputHeuristic1Node<I, G>
{
    type OutputValue = TxOutSet<I>;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.input.id(), self.index.id()]
    }

    fn evaluate(&self, ctx: &EvalContext) -> HashSet<I::TxOutId> {
        let tx_ids = ctx.get_or_default(&self.input);
        let index_handle = ctx.get(&self.index);
        let index_guard = index_handle.as_arc().read().expect("lock poisoned");

        let mut result = HashSet::new();

        for tx_id in &tx_ids {
            let tx = tx_id.with_index(&*index_guard);

            let outputs: Vec<_> = tx.outputs().map(|o| (o.id(), o.value())).collect();
            if outputs.is_empty() {
                continue;
            }

            let input_values: Vec<Amount> = tx
                .inputs()
                .filter_map(|input| {
                    let prev_id = input.prev_txout_id();
                    let prev_tx = prev_id.containing_txid().with_index(&*index_guard);
                    prev_tx
                        .outputs()
                        .find(|o: &Box<dyn AbstractTxOut<I = I>>| o.id() == prev_id)
                        .map(|o| o.value())
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
    pub fn new<I: IdFamily + 'static, G: IndexedGraph<I> + 'static>(
        input: Expr<TxSet<I>>,
        index: Expr<Index<G>>,
    ) -> Expr<TxOutSet<I>> {
        let ctx = input.context().clone();
        ctx.register(UnnecessaryInputHeuristic1Node::new(input, index))
    }
}

/// Node that implements UIH2 (Unnecessary input heuristic).
///
/// Flags transactions where (sum_in - min_in) >= (sum_out - min_out), i.e. the
/// largest output could be paid without the smallest input. Fee is ignored.
pub struct UnnecessaryInputHeuristic2Node<I: IdFamily + 'static, G: IndexedGraph<I> + 'static> {
    input: Expr<TxSet<I>>,
    index: Expr<Index<G>>,
}

impl<I: IdFamily + 'static, G: IndexedGraph<I> + 'static> UnnecessaryInputHeuristic2Node<I, G> {
    pub fn new(input: Expr<TxSet<I>>, index: Expr<Index<G>>) -> Self {
        Self { input, index }
    }
}

impl<I: IdFamily + 'static, G: IndexedGraph<I> + 'static> Node
    for UnnecessaryInputHeuristic2Node<I, G>
{
    type OutputValue = TxMask<I>;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.input.id(), self.index.id()]
    }

    fn evaluate(&self, ctx: &EvalContext) -> std::collections::HashMap<I::TxId, bool> {
        let tx_ids = ctx.get_or_default(&self.input);
        let index_handle = ctx.get(&self.index);
        let index_guard = index_handle.as_arc().read().expect("lock poisoned");

        let mut result = std::collections::HashMap::new();

        for tx_id in &tx_ids {
            let tx = tx_id.with_index(&*index_guard);

            // TODO: internal method for collecting input values
            let input_values: Vec<Amount> = tx
                .inputs()
                .filter_map(|input| {
                    let prev_id = input.prev_txout_id();
                    let prev_tx = prev_id.containing_txid().with_index(&*index_guard);
                    prev_tx
                        .outputs()
                        .find(|o: &Box<dyn AbstractTxOut<I = I>>| o.id() == prev_id)
                        .map(|o| o.value())
                })
                .collect();

            // TODO: internal method for collecting output values
            let output_values: Vec<Amount> = tx
                .outputs()
                .map(|o: Box<dyn AbstractTxOut<I = I>>| o.value())
                .collect();

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
    pub fn new<I: IdFamily + 'static, G: IndexedGraph<I> + 'static>(
        input: Expr<TxSet<I>>,
        index: Expr<Index<G>>,
    ) -> Expr<TxMask<I>> {
        let ctx = input.context().clone();
        ctx.register(UnnecessaryInputHeuristic2Node::new(input, index))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tx_indexer_pipeline::{context::PipelineContext, engine::Engine, ops::source::AllLooseTxs};
    use tx_indexer_primitives::{
        abstract_types::AbstractTransaction,
        loose::{LooseIds, TxId, TxOutId},
        test_utils::{DummyTxData, DummyTxOutData},
    };

    use super::{UnnecessaryInputHeuristic1, UnnecessaryInputHeuristic2};
    use crate::ast::tests::tests::TestFixture;

    fn setup_test_fixture() -> Vec<Arc<dyn AbstractTransaction<I = LooseIds> + Send + Sync>> {
        vec![
            Arc::new(TestFixture::coinbase1()),
            Arc::new(TestFixture::coinbase2()),
            Arc::new(TestFixture::spending_tx()),
        ]
    }

    fn setup_uih1_qualifying_fixture()
    -> Vec<Arc<dyn AbstractTransaction<I = LooseIds> + Send + Sync>> {
        vec![
            Arc::new(DummyTxData {
                id: TxId(0),
                outputs: vec![DummyTxOutData::new_with_amount(100, 0, TxId(0))],
                spent_coins: vec![],
                n_locktime: 0,
            }),
            Arc::new(DummyTxData {
                id: TxId(1),
                outputs: vec![DummyTxOutData::new_with_amount(200, 0, TxId(1))],
                spent_coins: vec![],
                n_locktime: 0,
            }),
            Arc::new(DummyTxData {
                id: TxId(2),
                outputs: vec![
                    DummyTxOutData::new_with_amount(50, 0, TxId(2)),
                    DummyTxOutData::new_with_amount(250, 1, TxId(2)),
                ],
                spent_coins: vec![TxOutId::new(TxId(0), 0), TxOutId::new(TxId(1), 0)],
                n_locktime: 0,
            }),
        ]
    }

    fn setup_uih1_no_candidate_fixture()
    -> Vec<Arc<dyn AbstractTransaction<I = LooseIds> + Send + Sync>> {
        vec![
            Arc::new(DummyTxData {
                id: TxId(0),
                outputs: vec![DummyTxOutData::new_with_amount(50, 0, TxId(0))],
                spent_coins: vec![],
                n_locktime: 0,
            }),
            Arc::new(DummyTxData {
                id: TxId(1),
                outputs: vec![DummyTxOutData::new_with_amount(100, 0, TxId(1))],
                spent_coins: vec![],
                n_locktime: 0,
            }),
            Arc::new(DummyTxData {
                id: TxId(2),
                outputs: vec![
                    DummyTxOutData::new_with_amount(80, 0, TxId(2)),
                    DummyTxOutData::new_with_amount(70, 1, TxId(2)),
                ],
                spent_coins: vec![TxOutId::new(TxId(0), 0), TxOutId::new(TxId(1), 0)],
                n_locktime: 0,
            }),
        ]
    }

    fn setup_uih1_tie_fixture() -> Vec<Arc<dyn AbstractTransaction<I = LooseIds> + Send + Sync>> {
        vec![
            Arc::new(DummyTxData {
                id: TxId(0),
                outputs: vec![DummyTxOutData::new_with_amount(100, 0, TxId(0))],
                spent_coins: vec![],
                n_locktime: 0,
            }),
            Arc::new(DummyTxData {
                id: TxId(1),
                outputs: vec![DummyTxOutData::new_with_amount(200, 0, TxId(1))],
                spent_coins: vec![],
                n_locktime: 0,
            }),
            Arc::new(DummyTxData {
                id: TxId(2),
                outputs: vec![
                    DummyTxOutData::new_with_amount(50, 0, TxId(2)),
                    DummyTxOutData::new_with_amount(50, 1, TxId(2)),
                ],
                spent_coins: vec![TxOutId::new(TxId(0), 0), TxOutId::new(TxId(1), 0)],
                n_locktime: 0,
            }),
        ]
    }

    fn setup_uih2_no_unnecessary_fixture()
    -> Vec<Arc<dyn AbstractTransaction<I = LooseIds> + Send + Sync>> {
        vec![
            Arc::new(DummyTxData {
                id: TxId(0),
                outputs: vec![DummyTxOutData::new_with_amount(100, 0, TxId(0))],
                spent_coins: vec![],
                n_locktime: 0,
            }),
            Arc::new(DummyTxData {
                id: TxId(1),
                outputs: vec![DummyTxOutData::new_with_amount(200, 0, TxId(1))],
                spent_coins: vec![],
                n_locktime: 0,
            }),
            Arc::new(DummyTxData {
                id: TxId(2),
                outputs: vec![
                    DummyTxOutData::new_with_amount(250, 0, TxId(2)),
                    DummyTxOutData::new_with_amount(40, 1, TxId(2)),
                ],
                spent_coins: vec![TxOutId::new(TxId(0), 0), TxOutId::new(TxId(1), 0)],
                n_locktime: 0,
            }),
        ]
    }

    fn setup_uih2_boundary_fixture() -> Vec<Arc<dyn AbstractTransaction<I = LooseIds> + Send + Sync>>
    {
        vec![
            Arc::new(DummyTxData {
                id: TxId(0),
                outputs: vec![DummyTxOutData::new_with_amount(100, 0, TxId(0))],
                spent_coins: vec![],
                n_locktime: 0,
            }),
            Arc::new(DummyTxData {
                id: TxId(1),
                outputs: vec![DummyTxOutData::new_with_amount(200, 0, TxId(1))],
                spent_coins: vec![],
                n_locktime: 0,
            }),
            Arc::new(DummyTxData {
                id: TxId(2),
                outputs: vec![
                    DummyTxOutData::new_with_amount(200, 0, TxId(2)),
                    DummyTxOutData::new_with_amount(0, 1, TxId(2)),
                ],
                spent_coins: vec![TxOutId::new(TxId(0), 0), TxOutId::new(TxId(1), 0)],
                n_locktime: 0,
            }),
        ]
    }

    fn setup_uih_mixed_fixture() -> Vec<Arc<dyn AbstractTransaction<I = LooseIds> + Send + Sync>> {
        vec![
            Arc::new(DummyTxData {
                id: TxId(0),
                outputs: vec![DummyTxOutData::new_with_amount(100, 0, TxId(0))],
                spent_coins: vec![],
                n_locktime: 0,
            }),
            Arc::new(DummyTxData {
                id: TxId(1),
                outputs: vec![DummyTxOutData::new_with_amount(200, 0, TxId(1))],
                spent_coins: vec![],
                n_locktime: 0,
            }),
            Arc::new(DummyTxData {
                id: TxId(2),
                outputs: vec![DummyTxOutData::new_with_amount(50, 0, TxId(2))],
                spent_coins: vec![],
                n_locktime: 0,
            }),
            Arc::new(DummyTxData {
                id: TxId(3),
                outputs: vec![DummyTxOutData::new_with_amount(260, 0, TxId(3))],
                spent_coins: vec![],
                n_locktime: 0,
            }),
            Arc::new(DummyTxData {
                id: TxId(4),
                outputs: vec![
                    DummyTxOutData::new_with_amount(200, 0, TxId(4)),
                    DummyTxOutData::new_with_amount(30, 1, TxId(4)),
                ],
                spent_coins: vec![TxOutId::new(TxId(0), 0), TxOutId::new(TxId(1), 0)],
                n_locktime: 0,
            }),
            Arc::new(DummyTxData {
                id: TxId(5),
                outputs: vec![
                    DummyTxOutData::new_with_amount(270, 0, TxId(5)),
                    DummyTxOutData::new_with_amount(10, 1, TxId(5)),
                ],
                spent_coins: vec![TxOutId::new(TxId(2), 0), TxOutId::new(TxId(3), 0)],
                n_locktime: 0,
            }),
        ]
    }

    #[test]
    fn test_uih1_smallest_output_is_change() {
        let all_txs = setup_uih1_qualifying_fixture();
        let ctx = Arc::new(PipelineContext::new());
        let mut engine = Engine::new(ctx.clone());
        engine.add_base_facts(all_txs);

        let source = AllLooseTxs::new(&ctx);
        let uih1 = UnnecessaryInputHeuristic1::new(source.txs(), source.index());
        let result = engine.eval(&uih1);

        let smallest_out = TxOutId::new(TxId(2), 0);
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
        let mut engine = Engine::new(ctx.clone());
        engine.add_base_facts(all_txs);

        let source = AllLooseTxs::new(&ctx);
        let uih1 = UnnecessaryInputHeuristic1::new(source.txs(), source.index());
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
        let mut engine = Engine::new(ctx.clone());
        engine.add_base_facts(all_txs);

        let source = AllLooseTxs::new(&ctx);
        let uih1 = UnnecessaryInputHeuristic1::new(source.txs(), source.index());
        let result = engine.eval(&uih1);

        assert!(result.contains(&TxOutId::new(TxId(2), 0)));
        assert!(result.contains(&TxOutId::new(TxId(2), 1)));
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_uih2_unnecessary_input() {
        let all_txs = setup_test_fixture();
        let ctx = Arc::new(PipelineContext::new());
        let mut engine = Engine::new(ctx.clone());
        engine.add_base_facts(all_txs);

        let source = AllLooseTxs::new(&ctx);
        let uih2 = UnnecessaryInputHeuristic2::new(source.txs(), source.index());
        let result = engine.eval(&uih2);

        assert_eq!(
            result.get(&TxId(2)),
            Some(&true),
            "Tx with unnecessary input should be flagged UIH2"
        );
    }

    #[test]
    fn test_uih2_no_unnecessary_input() {
        let all_txs = setup_uih2_no_unnecessary_fixture();
        let ctx = Arc::new(PipelineContext::new());
        let mut engine = Engine::new(ctx.clone());
        engine.add_base_facts(all_txs);

        let source = AllLooseTxs::new(&ctx);
        let uih2 = UnnecessaryInputHeuristic2::new(source.txs(), source.index());
        let result = engine.eval(&uih2);

        assert_eq!(
            result.get(&TxId(2)),
            Some(&false),
            "Tx where (sum_in - min_in) < (sum_out - min_out) should not be UIH2"
        );
    }

    #[test]
    fn test_uih2_single_input_false() {
        let all_txs: Vec<Arc<dyn AbstractTransaction<I = LooseIds> + Send + Sync>> = vec![
            Arc::new(TestFixture::coinbase2()),
            Arc::new(TestFixture::single_input_spending_tx()),
        ];
        let ctx = Arc::new(PipelineContext::new());
        let mut engine = Engine::new(ctx.clone());
        engine.add_base_facts(all_txs);

        let source = AllLooseTxs::new(&ctx);
        let uih2 = UnnecessaryInputHeuristic2::new(source.txs(), source.index());
        let result = engine.eval(&uih2);

        assert_eq!(
            result.get(&TxId(2)),
            Some(&false),
            "Single-input tx should not be flagged UIH2"
        );
    }

    #[test]
    fn test_uih2_boundary_equality() {
        let all_txs = setup_uih2_boundary_fixture();
        let ctx = Arc::new(PipelineContext::new());
        let mut engine = Engine::new(ctx.clone());
        engine.add_base_facts(all_txs);

        let source = AllLooseTxs::new(&ctx);
        let uih2 = UnnecessaryInputHeuristic2::new(source.txs(), source.index());
        let result = engine.eval(&uih2);

        assert_eq!(
            result.get(&TxId(2)),
            Some(&true),
            "Boundary (sum_in - min_in) == (sum_out - min_out) should be UIH2"
        );
    }

    #[test]
    fn test_uih1_coinbase_skipped() {
        let all_txs: Vec<Arc<dyn AbstractTransaction<I = LooseIds> + Send + Sync>> =
            vec![Arc::new(TestFixture::coinbase1())];
        let ctx = Arc::new(PipelineContext::new());
        let mut engine = Engine::new(ctx.clone());
        engine.add_base_facts(all_txs);

        let source = AllLooseTxs::new(&ctx);
        let uih1 = UnnecessaryInputHeuristic1::new(source.txs(), source.index());
        let result = engine.eval(&uih1);

        assert!(
            result.is_empty(),
            "Coinbase (no inputs) should contribute no UIH1 outputs"
        );
    }

    #[test]
    fn test_uih1_and_uih2_mixed_transactions() {
        let all_txs = setup_uih_mixed_fixture();
        let ctx = Arc::new(PipelineContext::new());
        let mut engine = Engine::new(ctx.clone());
        engine.add_base_facts(all_txs);

        let source = AllLooseTxs::new(&ctx);

        let uih1 = UnnecessaryInputHeuristic1::new(source.txs(), source.index());
        let uih1_result = engine.eval(&uih1);
        assert!(
            uih1_result.contains(&TxOutId::new(TxId(4), 1)),
            "Tx4 smallest out in UIH1"
        );
        assert!(
            !uih1_result.contains(&TxOutId::new(TxId(5), 0)),
            "Tx5 min_out 10 not < min_in 50 so no UIH1"
        );

        let uih2 = UnnecessaryInputHeuristic2::new(source.txs(), source.index());
        let uih2_result = engine.eval(&uih2);
        assert_eq!(uih2_result.get(&TxId(4)), Some(&true));
        assert_eq!(uih2_result.get(&TxId(5)), Some(&false));
    }
}
