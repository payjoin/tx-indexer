use tx_indexer_disjoint_set::SparseDisjointSet;
use tx_indexer_pipeline::{
    engine::EvalContext,
    expr::Expr,
    node::{Node, NodeId},
    value::{TxOutClustering, TxSet},
};
use tx_indexer_primitives::unified::id::AnyOutId;

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
    type OutputValue = TxOutClustering;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.input.id()]
    }

    fn evaluate(&self, ctx: &EvalContext) -> SparseDisjointSet<AnyOutId> {
        // Use get_or_default since input may not be ready yet in cyclic pipelines
        let tx_ids = ctx.get_or_default(&self.input);

        let mut clustering = SparseDisjointSet::new();

        for tx_id in &tx_ids {
            let tx = ctx.unified_storage().tx(*tx_id);
            let set = crate::common_input::MultiInputHeuristic::merge_prevouts(&tx);
            clustering = clustering.join(&set);
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
    pub fn new(input: Expr<TxSet>) -> Expr<TxOutClustering> {
        let ctx = input.context().clone();
        ctx.register(MultiInputHeuristicNode::new(input))
    }
}
