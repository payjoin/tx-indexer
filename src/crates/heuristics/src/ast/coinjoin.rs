use std::collections::HashMap;

use pipeline::engine::EvalContext;
use pipeline::expr::Expr;
use pipeline::node::{Node, NodeId};
use pipeline::value::{Backend, Index, Mask, TxSet};
use tx_indexer_primitives::graph_index::{IndexHandleFor, WithIndex};

use crate::coinjoin_detection::NaiveCoinjoinDetection;

/// Node that detects CoinJoin transactions.
pub struct IsCoinJoinNode<I: Backend> {
    input: Expr<TxSet<I::TxId>>,
    index: Expr<Index<I>>,
}

impl<I: Backend> IsCoinJoinNode<I>
where
    I::TxId: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
{
    pub fn new(input: Expr<TxSet<I::TxId>>, index: Expr<Index<I>>) -> Self {
        Self { input, index }
    }
}

impl<I: Backend> Node for IsCoinJoinNode<I>
where
    I::TxId: Eq + std::hash::Hash + Clone + Send + Sync + 'static + WithIndex<I>,
{
    type OutputValue = Mask<I::TxId>;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.input.id(), self.index.id()]
    }

    fn evaluate(&self, ctx: &EvalContext) -> HashMap<I::TxId, bool> {
        let tx_ids = ctx.get(&self.input);
        let index_handle = ctx.get(&self.index);

        index_handle.with_graph(|graph| {
            tx_ids
                .iter()
                .map(|tx_id| {
                    let tx = tx_id.with_index(graph);
                    (tx.id(), NaiveCoinjoinDetection::is_coinjoin(&tx))
                })
                .collect()
        })
    }

    fn name(&self) -> &'static str {
        "IsCoinJoin"
    }
}

pub struct IsCoinJoin;

impl IsCoinJoin {
    pub fn new<I: Backend>(
        input: Expr<TxSet<I::TxId>>,
        index: Expr<Index<I>>,
    ) -> Expr<Mask<I::TxId>>
    where
        I::TxId: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
    {
        let ctx = input.context().clone();
        ctx.register(IsCoinJoinNode::new(input, index))
    }
}
