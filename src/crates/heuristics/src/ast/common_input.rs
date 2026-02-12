use pipeline::engine::EvalContext;
use pipeline::expr::Expr;
use pipeline::node::{Node, NodeId};
use pipeline::value::{Index, TxOutClustering, TxSet};
use tx_indexer_primitives::abstract_types::{IdFamily, IntoTxHandle};
use tx_indexer_primitives::disjoint_set::{DisJointSet, SparseDisjointSet};
use tx_indexer_primitives::graph_index::IndexedGraph;

/// Node that implements the Multi-Input Heuristic.
///
/// The MIH assumes that all inputs to a transaction are controlled by the same entity.
/// This node creates a clustering where all spent outputs (inputs) of each transaction
/// are in the same cluster.
pub struct MultiInputHeuristicNode<I: IdFamily + 'static, G: IndexedGraph<I> + 'static> {
    input: Expr<TxSet<I>>,
    index: Expr<Index<G>>,
}

impl<I: IdFamily + 'static, G: IndexedGraph<I> + 'static> MultiInputHeuristicNode<I, G> {
    pub fn new(input: Expr<TxSet<I>>, index: Expr<Index<G>>) -> Self {
        Self { input, index }
    }
}

impl<I: IdFamily + 'static, G: IndexedGraph<I> + 'static> Node for MultiInputHeuristicNode<I, G> {
    type OutputValue = TxOutClustering<I>;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.input.id(), self.index.id()]
    }

    fn evaluate(&self, ctx: &EvalContext) -> SparseDisjointSet<I::TxOutId> {
        // Use get_or_default since input may not be ready yet in cyclic pipelines
        let tx_ids = ctx.get_or_default(&self.input);
        let index_handle = ctx.get(&self.index);
        let index_guard = (&**index_handle.as_arc()).read().expect("lock poisoned");

        let mut clustering = SparseDisjointSet::new();

        for tx_id in &tx_ids {
            let tx = tx_id.with_index(&*index_guard);
            let coins: Vec<I::TxOutId> = tx.inputs().map(|input| input.prev_txout_id()).collect();
            if coins.len() > 1 {
                let set = SparseDisjointSet::new();
                for i in 1..coins.len() {
                    set.union(coins[0], coins[i]);
                }
                clustering = clustering.join(&set);
            }
        }

        clustering
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
    pub fn new<I: IdFamily + 'static, G: IndexedGraph<I> + 'static>(
        input: Expr<TxSet<I>>,
        index: Expr<Index<G>>,
    ) -> Expr<TxOutClustering<I>> {
        let ctx = input.context().clone();
        ctx.register(MultiInputHeuristicNode::new(input, index))
    }
}
