use std::collections::HashMap;

use tx_indexer_pipeline::{
    engine::EvalContext,
    expr::Expr,
    node::{Node, NodeId},
    value::{Index, Mask, TxSet},
};
use tx_indexer_primitives::{
    abstract_types::{IdFamily, IntoTxHandle},
    graph_index::IndexedGraph,
};

use crate::coinjoin_detection::NaiveCoinjoinDetection;

/// Node that detects CoinJoin transactions.
///
/// Uses a naive heuristic: if there are >= 3 outputs of the same value,
/// the transaction is classified as a CoinJoin.
pub struct IsCoinJoinNode<I: IdFamily + 'static, G: IndexedGraph<I> + 'static> {
    input: Expr<TxSet<I>>,
    index: Expr<Index<G>>,
}

impl<I: IdFamily + 'static, G: IndexedGraph<I> + 'static> IsCoinJoinNode<I, G> {
    pub fn new(input: Expr<TxSet<I>>, index: Expr<Index<G>>) -> Self {
        Self { input, index }
    }
}

impl<I: IdFamily + 'static, G: IndexedGraph<I> + 'static> Node for IsCoinJoinNode<I, G> {
    type OutputValue = Mask<I::TxId>;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.input.id(), self.index.id()]
    }

    fn evaluate(&self, ctx: &EvalContext) -> HashMap<I::TxId, bool> {
        let tx_ids = ctx.get(&self.input);
        let index_handle = ctx.get(&self.index);
        let index_guard = index_handle.as_arc().read().expect("lock poisoned");
        tx_ids
            .iter()
            .map(|tx_id| {
                let tx = tx_id.with_index(&*index_guard);
                (*tx_id, NaiveCoinjoinDetection::is_coinjoin(&tx))
            })
            .collect()
    }

    fn name(&self) -> &'static str {
        "IsCoinJoin"
    }
}

pub struct IsCoinJoin;

impl IsCoinJoin {
    pub fn new<I: IdFamily + 'static, G: IndexedGraph<I> + 'static>(
        input: Expr<TxSet<I>>,
        index: Expr<Index<G>>,
    ) -> Expr<Mask<I::TxId>> {
        let ctx = input.context().clone();
        ctx.register(IsCoinJoinNode::new(input, index))
    }
}
