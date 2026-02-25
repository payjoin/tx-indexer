//! Set operations for the pipeline DSL.
//!
//! - `outputs`: TxSet -> TxOutSet (get all outputs of transactions)
//! - `txs`: TxOutSet -> TxSet (get transactions containing outputs)
//! - `join`: Clustering x Clustering -> Clustering (merge clusterings)

use std::{collections::HashSet, hash::Hash};

use tx_indexer_disjoint_set::SparseDisjointSet;
use tx_indexer_primitives::unified::{AnyOutId, AnyTxId};

use crate::{
    engine::EvalContext,
    expr::Expr,
    node::{Node, NodeId},
    value::{Clustering, TxOutSet, TxSet},
};

/// Node that extracts all TxOut ids from a set of transactions.
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

    fn evaluate(&self, ctx: &EvalContext) -> HashSet<AnyOutId> {
        let tx_ids = ctx.get_or_default(&self.input);
        let mut outputs = HashSet::new();
        for id in tx_ids {
            let tx_outputs = ctx.unified_storage().tx_out_ids(id);
            outputs.extend(tx_outputs);
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

    fn evaluate(&self, ctx: &EvalContext) -> HashSet<AnyTxId> {
        let outputs = ctx.get(&self.input);
        outputs
            .iter()
            .map(|out| ctx.unified_storage().txid_for_out(*out))
            .collect()
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
pub struct JoinClusteringNode<K: Eq + Hash + Copy + Clone + Send + Sync + 'static> {
    left: Expr<Clustering<K>>,
    right: Expr<Clustering<K>>,
}

impl<K: Eq + Hash + Copy + Clone + Send + Sync + 'static> JoinClusteringNode<K> {
    pub fn new(left: Expr<Clustering<K>>, right: Expr<Clustering<K>>) -> Self {
        Self { left, right }
    }
}

impl<K: Eq + Hash + Copy + Clone + Send + Sync + 'static> Node for JoinClusteringNode<K> {
    type OutputValue = Clustering<K>;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.left.id(), self.right.id()]
    }

    fn evaluate(&self, ctx: &EvalContext) -> SparseDisjointSet<K> {
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
impl<K: Eq + Hash + Copy + Clone + Send + Sync + 'static> Expr<Clustering<K>> {
    /// Join (merge) this clustering with another.
    ///
    /// The result contains all equivalences from both clusterings.
    pub fn join(&self, other: Expr<Clustering<K>>) -> Expr<Clustering<K>> {
        self.ctx
            .register(JoinClusteringNode::new(self.clone(), other))
    }
}
