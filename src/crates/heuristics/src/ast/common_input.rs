//! Multi-Input Heuristic (Common Input Ownership) for the pipeline DSL.

use pipeline::engine::EvalContext;
use pipeline::expr::Expr;
use pipeline::node::{Node, NodeId};
use pipeline::value::{Clustering, TxSet};
use tx_indexer_primitives::disjoint_set::{DisJointSet, SparseDisjointSet};
use tx_indexer_primitives::loose::TxOutId;

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
    type Value = Clustering;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.input.id()]
    }

    fn evaluate(&self, ctx: &EvalContext) -> SparseDisjointSet<TxOutId> {
        let tx_ids = ctx.get(&self.input);
        let index = ctx.index();

        let clustering = SparseDisjointSet::new();

        for &tx_id in tx_ids {
            if let Some(tx) = index.txs.get(&tx_id) {
                // Get all inputs (spent outputs)
                let inputs: Vec<_> = tx
                    .inputs()
                    .map(|input| TxOutId::new(input.prev_txid(), input.prev_vout()))
                    .collect();

                // Union all inputs together
                if inputs.len() >= 2 {
                    for window in inputs.windows(2) {
                        clustering.union(window[0], window[1]);
                    }
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
    pub fn new(input: Expr<TxSet>) -> Expr<Clustering> {
        let ctx = input.context().clone();
        ctx.register(MultiInputHeuristicNode::new(input))
    }
}
