use pipeline::engine::EvalContext;
use pipeline::expr::Expr;
use pipeline::node::{Node, NodeId};
use pipeline::value::{Clustering, TxSet};
use tx_indexer_primitives::disjoint_set::SparseDisjointSet;
use tx_indexer_primitives::loose::TxOutId;

use crate::common_input::MultiInputHeuristic as MIHImpl;

/// Node that implements the Multi-Input Heuristic.
///
/// The MIH assumes that all inputs to a transaction are controlled by the same entity.
/// This node creates a clustering where all spent outputs (inputs) of each transaction
/// are in the same cluster.
pub struct MultiInputHeuristicNode {
    input: Expr<TxSet>,
}

impl MultiInputHeuristicNode {
    pub fn new(input: Expr<TxSet>) -> Self {
        Self { input }
    }
}

impl Node for MultiInputHeuristicNode {
    type OutputValue = Clustering;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.input.id()]
    }

    fn evaluate(&self, ctx: &EvalContext) -> SparseDisjointSet<TxOutId> {
        // Use get_or_default since input may not be ready yet in cyclic pipelines
        let tx_ids = ctx.get_or_default(&self.input);
        let index = ctx.index();
        let heuristic = MIHImpl;

        let mut clustering = SparseDisjointSet::new();

        for tx_id in &tx_ids {
            let tx = tx_id.with(index);
            let tx_clustering = heuristic.merge_prevouts(&tx);
            clustering = clustering.join(&tx_clustering);
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
    pub fn new(input: Expr<TxSet>) -> Expr<Clustering> {
        let ctx = input.context().clone();
        ctx.register(MultiInputHeuristicNode::new(input))
    }
}
