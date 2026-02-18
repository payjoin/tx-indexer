//! Set operations for the pipeline DSL.
//!
//! - `outputs`: TxSet -> TxOutSet (get all outputs of transactions)
//! - `txs`: TxOutSet -> TxSet (get transactions containing outputs)
//! - `join`: Clustering x Clustering -> Clustering (merge clusterings)

use std::collections::HashSet;
use std::hash::Hash;

use tx_indexer_primitives::abstract_types::{IdFamily, IntoTxHandle, TxOutIdOps};
use tx_indexer_primitives::disjoint_set::SparseDisjointSet;
use tx_indexer_primitives::graph_index::IndexedGraph;

use crate::engine::EvalContext;
use crate::expr::Expr;
use crate::node::{Node, NodeId};
use crate::value::{Clustering, Index, TxOutSet, TxSet};

/// Node that extracts all TxOut ids from a set of transactions.
pub struct OutputsNode<I: IdFamily + 'static, G: IndexedGraph<I> + 'static> {
    input: Expr<TxSet<I>>,
    index: Expr<Index<G>>,
}

impl<I: IdFamily + 'static, G: IndexedGraph<I> + 'static> OutputsNode<I, G> {
    pub fn new(input: Expr<TxSet<I>>, index: Expr<Index<G>>) -> Self {
        Self { input, index }
    }
}

impl<I: IdFamily + 'static, G: IndexedGraph<I> + 'static> Node for OutputsNode<I, G> {
    type OutputValue = TxOutSet<I>;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.input.id(), self.index.id()]
    }

    fn evaluate(&self, ctx: &EvalContext) -> HashSet<I::TxOutId> {
        let tx_ids = ctx.get_or_default(&self.input);
        let index_handle = ctx.get(&self.index);
        let index_guard = index_handle.as_arc().read().expect("lock poisoned");

        let mut outputs = HashSet::new();
        for id in tx_ids {
            let tx = id.with_index(&*index_guard);
            for output in tx.outputs() {
                outputs.insert(output.id());
            }
        }
        outputs
    }

    fn name(&self) -> &'static str {
        "Outputs"
    }
}

/// Node that extracts the containing transactions from a set of outputs.
pub struct TxsNode<I: IdFamily + 'static> {
    input: Expr<TxOutSet<I>>,
}

impl<I: IdFamily + 'static> TxsNode<I> {
    pub fn new(input: Expr<TxOutSet<I>>) -> Self {
        Self { input }
    }
}

impl<I: IdFamily + 'static> Node for TxsNode<I> {
    type OutputValue = TxSet<I>;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.input.id()]
    }

    fn evaluate(&self, ctx: &EvalContext) -> HashSet<I::TxId> {
        let outputs = ctx.get(&self.input);
        outputs.iter().map(|out| out.containing_txid()).collect()
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

// Extension methods on Expr<TxSet> (IdFamily-parameterized so I is constrained)
impl<I: IdFamily + 'static> Expr<TxSet<I>> {
    /// Get all outputs of the transactions in this set.
    pub fn outputs<G: IndexedGraph<I> + 'static>(
        &self,
        index: Expr<Index<G>>,
    ) -> Expr<TxOutSet<I>> {
        self.ctx
            .register(OutputsNode::<I, G>::new(self.clone(), index))
    }
}

// Extension methods on Expr<TxOutSet>
impl<I: IdFamily + 'static> Expr<TxOutSet<I>> {
    /// Get the transactions containing these outputs.
    pub fn txs(&self) -> Expr<TxSet<I>> {
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
