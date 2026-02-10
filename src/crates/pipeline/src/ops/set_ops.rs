//! Set operations for the pipeline DSL.
//!
//! - `outputs`: TxSet x Index -> TxOutSet (get all outputs of transactions)
//! - `txs`: TxOutSet -> TxSet (get transactions containing outputs)
//! - `join`: Clustering x Clustering -> Clustering (merge clusterings)

use std::collections::HashSet;

use tx_indexer_primitives::disjoint_set::SparseDisjointSet;
use tx_indexer_primitives::loose::{TxId, TxOutId};

use crate::engine::EvalContext;
use crate::expr::Expr;
use crate::node::{Node, NodeId};
use crate::value::{Clustering, LooseIndex, TxOutSet, TxSet};

/// Node that extracts all outputs from a set of transactions.
/// Requires the index as a dependency (e.g. from AllTxs output).
pub struct OutputsNode {
    input: Expr<TxSet<TxId>>,
    index: Expr<LooseIndex>,
}

impl OutputsNode {
    pub fn new(input: Expr<TxSet<TxId>>, index: Expr<LooseIndex>) -> Self {
        Self { input, index }
    }
}

impl Node for OutputsNode {
    type OutputValue = TxOutSet<TxOutId>;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.input.id(), self.index.id()]
    }

    fn evaluate(&self, ctx: &EvalContext) -> HashSet<TxOutId> {
        let tx_ids = ctx.get(&self.input);
        let index_handle = ctx.get(&self.index);
        let index = index_handle.0.read().expect("lock poisoned");

        let mut outputs = HashSet::new();
        for id in tx_ids.iter() {
            if let Some(tx) = index.txs.get(id) {
                let output_count = tx.output_len();
                for vout in 0..output_count {
                    outputs.insert((*id).txout_id(vout as u32));
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
pub struct TxsNode<K: Eq + std::hash::Hash + Clone + Send + Sync + 'static> {
    input: Expr<TxOutSet<K>>,
}

impl<K: Eq + std::hash::Hash + Clone + Send + Sync + 'static> TxsNode<K> {
    pub fn new(input: Expr<TxOutSet<K>>) -> Self {
        Self { input }
    }
}

/// TxsNode for loose backend: TxOutSet<TxOutId> -> TxSet<TxId>
impl Node for TxsNode<TxOutId> {
    type OutputValue = TxSet<TxId>;

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
pub struct JoinClusteringNode<K: Eq + std::hash::Hash + Copy + Send + Sync + 'static> {
    left: Expr<Clustering<K>>,
    right: Expr<Clustering<K>>,
}

impl<K: Eq + std::hash::Hash + Copy + Send + Sync + 'static> JoinClusteringNode<K> {
    pub fn new(left: Expr<Clustering<K>>, right: Expr<Clustering<K>>) -> Self {
        Self { left, right }
    }
}

impl<K: Eq + std::hash::Hash + Copy + Send + Sync + 'static> Node for JoinClusteringNode<K> {
    type OutputValue = Clustering<K>;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.left.id(), self.right.id()]
    }

    fn evaluate(&self, ctx: &EvalContext) -> SparseDisjointSet<K> {
        let left = ctx.get_or_default(&self.left);
        let right = ctx.get_or_default(&self.right);
        left.join(&right)
    }

    fn name(&self) -> &'static str {
        "JoinClustering"
    }
}

// Extension methods: Expr<TxSet<TxId>>::outputs(index) and Expr<TxOutSet<TxOutId>>::txs()
impl Expr<TxSet<TxId>> {
    /// Get all outputs of the transactions in this set.
    /// Requires the index (e.g. from `all_txs.index()`).
    pub fn outputs(&self, index: Expr<LooseIndex>) -> Expr<TxOutSet<TxOutId>> {
        self.ctx.register(OutputsNode::new(self.clone(), index))
    }
}

impl Expr<TxOutSet<TxOutId>> {
    /// Get the transactions containing these outputs.
    pub fn txs(&self) -> Expr<TxSet<TxId>> {
        self.ctx.register(TxsNode::new(self.clone()))
    }
}

// Generic Expr<Clustering<K>>::join
impl<K: Eq + std::hash::Hash + Copy + Send + Sync + 'static> Expr<Clustering<K>> {
    /// Join (merge) this clustering with another.
    ///
    /// The result contains all equivalences from both clusterings.
    pub fn join(
        &self,
        other: Expr<Clustering<K>>,
    ) -> Expr<Clustering<K>> {
        self.ctx
            .register(JoinClusteringNode::new(self.clone(), other))
    }
}
