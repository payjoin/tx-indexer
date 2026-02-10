use pipeline::engine::EvalContext;
use pipeline::expr::Expr;
use pipeline::node::{Node, NodeId};
use pipeline::value::{Backend, Clustering, Index, TxSet};
use tx_indexer_primitives::disjoint_set::{DisJointSet, SparseDisjointSet};
use tx_indexer_primitives::graph_index::{IndexHandleFor, TxHandleLike, WithIndex};

/// Node that implements the Multi-Input Heuristic.
///
/// The MIH assumes that all inputs to a transaction are controlled by the same entity.
/// This node creates a clustering where all spent outputs (inputs) of each transaction
/// are in the same cluster.
pub struct MultiInputHeuristicNode<I: Backend> {
    input: Expr<TxSet<I::TxId>>,
    index: Expr<Index<I>>,
}

impl<I: Backend> MultiInputHeuristicNode<I>
where
    I::TxId: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
    I::TxOutId: Eq + std::hash::Hash + Copy + Send + Sync + 'static,
{
    pub fn new(input: Expr<TxSet<I::TxId>>, index: Expr<Index<I>>) -> Self {
        Self { input, index }
    }
}

impl<I: Backend> Node for MultiInputHeuristicNode<I>
where
    I::TxId: Eq + std::hash::Hash + Clone + Send + Sync + 'static + WithIndex<I>,
    I::TxOutId: Eq + std::hash::Hash + Copy + Send + Sync + 'static,
{
    type OutputValue = Clustering<I::TxOutId>;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.input.id(), self.index.id()]
    }

    fn evaluate(&self, ctx: &EvalContext) -> SparseDisjointSet<I::TxOutId> {
        let tx_ids = ctx.get_or_default(&self.input);
        let index_handle = ctx.get(&self.index);

        index_handle.with_graph(|graph| {
            let mut clustering = SparseDisjointSet::new();

            for tx_id in &tx_ids {
                let tx = tx_id.with_index(graph);
                let coins: Vec<I::TxOutId> = tx.spent_coins().collect();
                if coins.len() > 1 {
                    let set = SparseDisjointSet::new();
                    for i in 1..coins.len() {
                        set.union(coins[0], coins[i]);
                    }
                    clustering = clustering.join(&set);
                }
            }

            clustering
        })
    }

    fn name(&self) -> &'static str {
        "MultiInputHeuristic"
    }
}

/// Factory for creating a Multi-Input Heuristic expression.
pub struct MultiInputHeuristic;

impl MultiInputHeuristic {
    /// Apply the Multi-Input Heuristic to the given transactions.
    ///
    /// Returns a clustering where all inputs of each transaction are in the same cluster.
    pub fn new<I: Backend>(
        input: Expr<TxSet<I::TxId>>,
        index: Expr<Index<I>>,
    ) -> Expr<Clustering<I::TxOutId>>
    where
        I::TxId: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
        I::TxOutId: Eq + std::hash::Hash + Copy + Send + Sync + 'static,
    {
        let ctx = input.context().clone();
        ctx.register(MultiInputHeuristicNode::new(input, index))
    }
}
