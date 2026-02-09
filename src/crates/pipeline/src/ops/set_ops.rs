//! Set operations for the pipeline DSL.
//!
//! - `outputs`: TxSet -> TxOutSet (get all outputs of transactions)
//! - `txs`: TxOutSet -> TxSet (get transactions containing outputs)
//! - `join`: Clustering x Clustering -> Clustering (merge clusterings)

use std::collections::HashSet;

use tx_indexer_primitives::disjoint_set::SparseDisjointSet;
use tx_indexer_primitives::loose::{TxId, TxOutId};

use crate::engine::EvalContext;
use crate::expr::Expr;
use crate::node::{Node, NodeId};
use crate::value::{Clustering, TxOutSet, TxSet};

/// Node that extracts all outputs from a set of transactions.
pub struct OutputsNode {
    input: Expr<TxSet>,
}

impl OutputsNode {
    pub fn new(input: Expr<TxSet>) -> Self {
        Self { input }
    }
}

impl Node for OutputsNode {
    type OutputValue = TxOutSet;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.input.id()]
    }

    fn evaluate(&self, ctx: &EvalContext) -> HashSet<TxOutId> {
        let tx_ids = ctx.get(&self.input);
        let index = ctx.index();

        let mut outputs = HashSet::new();
        for &tx_id in tx_ids {
            if let Some(tx) = index.txs.get(&tx_id) {
                let output_count = tx.output_len();
                for vout in 0..output_count {
                    outputs.insert(TxOutId::new(tx_id, vout as u32));
                }
            }
        }
        outputs
    }

    fn name(&self) -> &'static str {
        "Outputs"
    }
}

/// Node that extracts the containing transactions from a set of outputs.
pub struct TxsNode {
    input: Expr<TxOutSet>,
}

impl TxsNode {
    pub fn new(input: Expr<TxOutSet>) -> Self {
        Self { input }
    }
}

impl Node for TxsNode {
    type OutputValue = TxSet;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.input.id()]
    }

    fn evaluate(&self, ctx: &EvalContext) -> HashSet<TxId> {
        let outputs = ctx.get(&self.input);
        outputs.iter().map(|out| out.txid()).collect()
    }

    fn name(&self) -> &'static str {
        "Txs"
    }
}

/// Node that joins (merges) two clusterings.
///
/// The result is the coarsest partition that is a refinement of both input
/// clusterings. In other words, if two items are in the same cluster in
/// either input, they will be in the same cluster in the output.
pub struct JoinClusteringNode {
    left: Expr<Clustering>,
    right: Expr<Clustering>,
}

impl JoinClusteringNode {
    pub fn new(left: Expr<Clustering>, right: Expr<Clustering>) -> Self {
        Self { left, right }
    }
}

impl Node for JoinClusteringNode {
    type OutputValue = Clustering;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.left.id(), self.right.id()]
    }

    fn evaluate(&self, ctx: &EvalContext) -> SparseDisjointSet<TxOutId> {
        // Use get_or_default since either side might be part of a cycle
        let left = ctx.get_or_default(&self.left);
        let right = ctx.get_or_default(&self.right);
        left.join(&right)
    }

    fn name(&self) -> &'static str {
        "JoinClustering"
    }
}

// Extension methods on Expr<TxSet>
impl Expr<TxSet> {
    /// Get all outputs of the transactions in this set.
    pub fn outputs(&self) -> Expr<TxOutSet> {
        self.ctx.register(OutputsNode::new(self.clone()))
    }
}

// Extension methods on Expr<TxOutSet>
impl Expr<TxOutSet> {
    /// Get the transactions containing these outputs.
    pub fn txs(&self) -> Expr<TxSet> {
        self.ctx.register(TxsNode::new(self.clone()))
    }
}

// Extension methods on Expr<Clustering>
impl Expr<Clustering> {
    /// Join (merge) this clustering with another.
    ///
    /// The result contains all equivalences from both clusterings.
    pub fn join(&self, other: Expr<Clustering>) -> Expr<Clustering> {
        self.ctx
            .register(JoinClusteringNode::new(self.clone(), other))
    }
}
