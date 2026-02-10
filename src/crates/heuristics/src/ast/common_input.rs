use pipeline::engine::EvalContext;
use pipeline::expr::Expr;
use pipeline::node::{Node, NodeId};
use pipeline::value::{Clustering, Set};
use tx_indexer_primitives::abstract_id::AbstractTxOutId;
use tx_indexer_primitives::abstract_types::EnumerateSpentTxOuts;
use tx_indexer_primitives::disjoint_set::{DisJointSet, SparseDisjointSet};

/// Node that implements the Multi-Input Heuristic.
///
/// The MIH assumes that all inputs to a transaction are controlled by the same entity.
/// This node creates a clustering where all spent outputs (inputs) of each transaction
/// are in the same cluster.
pub struct MultiInputHeuristicNode {
    input: Expr<Set>,
}

impl MultiInputHeuristicNode {
    pub fn new(input: Expr<Set>) -> Self {
        Self { input }
    }
}

impl Node for MultiInputHeuristicNode {
    type OutputValue = Clustering;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.input.id()]
    }

    fn evaluate(&self, ctx: &EvalContext) -> SparseDisjointSet<AbstractTxOutId> {
        // Use get_or_default since input may not be ready yet in cyclic pipelines
        let tx_ids = ctx.get_or_default(&self.input);
        let index = ctx.index();

        let mut clustering = SparseDisjointSet::new();

        for tx_id in &tx_ids {
            if let Some(concrete_id) = tx_id.try_as_loose() {
                let tx = concrete_id.with(index);
                let coins: Vec<AbstractTxOutId> =
                    tx.spent_coins().map(AbstractTxOutId::from).collect();
                if coins.len() > 1 {
                    let set = SparseDisjointSet::new();
                    for i in 1..coins.len() {
                        set.union(coins[0], coins[i]);
                    }
                    clustering = clustering.join(&set);
                }
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
    pub fn new(input: Expr<Set>) -> Expr<Clustering> {
        let ctx = input.context().clone();
        ctx.register(MultiInputHeuristicNode::new(input))
    }
}
