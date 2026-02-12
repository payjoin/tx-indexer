use std::collections::HashMap;

use pipeline::engine::EvalContext;
use pipeline::expr::Expr;
use pipeline::node::{Node, NodeId};
use pipeline::value::{Index, TxMask, TxOutClustering, TxOutMask, TxOutSet, TxSet};
use tx_indexer_primitives::abstract_types::{IdFamily, IntoTxHandle, TxInIdOps, TxOutIdOps};
use tx_indexer_primitives::disjoint_set::{DisJointSet, SparseDisjointSet};
use tx_indexer_primitives::graph_index::IndexedGraph;

/// Node that identifies change outputs in transactions.
///
/// Uses a naive heuristic: the last output of a transaction is assumed to be change.
pub struct ChangeIdentificationNode<I: IdFamily + 'static, G: IndexedGraph<I> + 'static> {
    input: Expr<TxOutSet<I>>,
    index: Expr<Index<G>>,
}

impl<I: IdFamily + 'static, G: IndexedGraph<I> + 'static> ChangeIdentificationNode<I, G> {
    pub fn new(input: Expr<TxOutSet<I>>, index: Expr<Index<G>>) -> Self {
        Self { input, index }
    }
}

impl<I: IdFamily + 'static, G: IndexedGraph<I> + 'static> Node for ChangeIdentificationNode<I, G> {
    type OutputValue = TxOutMask<I>;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.input.id(), self.index.id()]
    }

    fn evaluate(&self, ctx: &EvalContext) -> HashMap<I::TxOutId, bool> {
        // Use get_or_default since input might be part of a cycle
        let txouts = ctx.get_or_default(&self.input);
        let index_handle = ctx.get(&self.index);
        let index_guard = index_handle.as_arc().read().expect("lock poisoned");

        let mut result = HashMap::new();

        for output_id in txouts.iter() {
            let tx_id = output_id.containing_txid();
            let Some(tx) = index_guard.tx(&tx_id) else {
                continue;
            };
            let output_count = tx.output_len();
            if output_count == 0 {
                continue;
            }

            let last_output = tx_id
                .with_index(&*index_guard)
                .outputs()
                .last()
                .expect("Tx should have at least one output")
                .id();
            result.insert(*output_id, *output_id == last_output);
        }

        result
    }

    fn name(&self) -> &'static str {
        "ChangeIdentification"
    }
}

/// Factory for creating a change identification expression.
pub struct ChangeIdentification;

impl ChangeIdentification {
    /// Identify change outputs in the given transactions.
    ///
    /// Returns a mask over outputs where `true` indicates the output is likely change.
    pub fn new<I: IdFamily + 'static, G: IndexedGraph<I> + 'static>(
        input: Expr<TxOutSet<I>>,
        index: Expr<Index<G>>,
    ) -> Expr<TxOutMask<I>> {
        let ctx = input.context().clone();
        ctx.register(ChangeIdentificationNode::new(input, index))
    }
}

/// Factory for creating a fingerprint-aware change identification expression.
///
/// Uses spending-tx fingerprints (e.g. n_locktime) to classify outputs as change or not:
/// when both the containing tx and the spending tx share a fingerprint (e.g. n_locktime > 0),
/// the output is classified as change.
pub struct FingerPrintChangeIdentification;

impl FingerPrintChangeIdentification {
    pub fn new<I: IdFamily + 'static, G: IndexedGraph<I> + 'static>(
        input: Expr<TxOutSet<I>>,
        index: Expr<Index<G>>,
    ) -> Expr<TxOutMask<I>> {
        let ctx = input.context().clone();
        ctx.register(FingerPrintChangeIdentificationNode::new(input, index))
    }
}

pub struct FingerPrintChangeIdentificationNode<I: IdFamily + 'static, G: IndexedGraph<I> + 'static>
{
    input: Expr<TxOutSet<I>>,
    index: Expr<Index<G>>,
}

impl<I: IdFamily + 'static, G: IndexedGraph<I> + 'static>
    FingerPrintChangeIdentificationNode<I, G>
{
    pub fn new(input: Expr<TxOutSet<I>>, index: Expr<Index<G>>) -> Self {
        Self { input, index }
    }
}

impl<I: IdFamily + 'static, G: IndexedGraph<I> + 'static> Node
    for FingerPrintChangeIdentificationNode<I, G>
{
    type OutputValue = TxOutMask<I>;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.input.id(), self.index.id()]
    }

    fn evaluate(&self, ctx: &EvalContext) -> HashMap<I::TxOutId, bool> {
        // Use get_or_default since input might be part of a cycle
        let txouts = ctx.get_or_default(&self.input);
        let index_handle = ctx.get(&self.index);
        let index_guard = index_handle.as_arc().read().expect("lock poisoned");

        let mut result = HashMap::new();

        for output_id in txouts.iter() {
            let tx_id = output_id.containing_txid();
            let Some(containing_tx) = index_guard.tx(&tx_id) else {
                continue;
            };
            let Some(spending_txin) = index_guard.spending_txin(output_id) else {
                continue;
            };
            let spending_txid = spending_txin.containing_txid();
            let Some(spending_tx) = index_guard.tx(&spending_txid) else {
                continue;
            };

            let is_change = if containing_tx.locktime() == 0 && spending_tx.locktime() == 0 {
                false
            } else {
                containing_tx.locktime() > 0 && spending_tx.locktime() > 0
            };

            result.insert(*output_id, is_change);
        }

        result
    }

    fn name(&self) -> &'static str {
        "FingerPrintChangeIdentification"
    }
}

/// Node that checks if a transaction's inputs are all in the same cluster.
///
/// This is used to gate change clustering - we only cluster change with inputs
/// if we're confident all inputs belong to the same entity.
pub struct IsUnilateralNode<I: IdFamily + 'static, G: IndexedGraph<I> + 'static> {
    txs: Expr<TxSet<I>>,
    clustering: Expr<TxOutClustering<I>>,
    index: Expr<Index<G>>,
}

impl<I: IdFamily + 'static, G: IndexedGraph<I> + 'static> IsUnilateralNode<I, G> {
    pub fn new(
        txs: Expr<TxSet<I>>,
        clustering: Expr<TxOutClustering<I>>,
        index: Expr<Index<G>>,
    ) -> Self {
        Self {
            txs,
            clustering,
            index,
        }
    }
}

impl<I: IdFamily + 'static, G: IndexedGraph<I> + 'static> Node for IsUnilateralNode<I, G> {
    type OutputValue = TxMask<I>;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.txs.id(), self.clustering.id(), self.index.id()]
    }

    fn evaluate(&self, ctx: &EvalContext) -> HashMap<I::TxId, bool> {
        // Use get_or_default for both; txs or clustering may not be ready yet in cyclic pipelines
        let tx_ids = ctx.get_or_default(&self.txs);
        let clustering = ctx.get_or_default(&self.clustering);
        let index_handle = ctx.get(&self.index);
        let index_guard = index_handle.as_arc().read().expect("lock poisoned");

        let mut result = HashMap::new();

        for tx_id in &tx_ids {
            let Some(tx) = index_guard.tx(tx_id) else {
                continue;
            };
            let inputs: Vec<I::TxOutId> = tx.inputs().map(|input| input.prev_txout_id()).collect();

            let is_unilateral = if inputs.is_empty() {
                false // Coinbase - no inputs to cluster
            } else if inputs.len() == 1 {
                true // Single input is trivially unilateral
            } else {
                let first_root = clustering.find(inputs[0]);
                inputs
                    .iter()
                    .all(|input| clustering.find(*input) == first_root)
            };

            result.insert(*tx_id, is_unilateral);
        }

        result
    }

    fn name(&self) -> &'static str {
        "IsUnilateral"
    }
}

/// Factory for creating an IsUnilateral expression.
pub struct IsUnilateral;

impl IsUnilateral {
    /// Check if transactions have all inputs in the same cluster.
    ///
    /// Takes a set of transactions and a clustering, returns a mask where `true`
    /// indicates all inputs of that transaction are in the same cluster.
    pub fn with_clustering<I: IdFamily + 'static, G: IndexedGraph<I> + 'static>(
        txs: Expr<TxSet<I>>,
        clustering: Expr<TxOutClustering<I>>,
        index: Expr<Index<G>>,
    ) -> Expr<TxMask<I>> {
        let ctx = txs.context().clone();
        ctx.register(IsUnilateralNode::new(txs, clustering, index))
    }
}

/// Node that clusters change outputs with their transaction's inputs.
///
/// For each transaction, if inputs are unilateral (all in same cluster) and has change outputs,
/// cluster the change outputs with the inputs.
pub struct ChangeClusteringNode<I: IdFamily + 'static, G: IndexedGraph<I> + 'static> {
    txs: Expr<TxSet<I>>,
    change_mask: Expr<TxOutMask<I>>,
    index: Expr<Index<G>>,
}

impl<I: IdFamily + 'static, G: IndexedGraph<I> + 'static> ChangeClusteringNode<I, G> {
    pub fn new(
        txs: Expr<TxSet<I>>,
        change_mask: Expr<TxOutMask<I>>,
        index: Expr<Index<G>>,
    ) -> Self {
        Self {
            txs,
            change_mask,
            index,
        }
    }
}

impl<I: IdFamily + 'static, G: IndexedGraph<I> + 'static> Node for ChangeClusteringNode<I, G> {
    type OutputValue = TxOutClustering<I>;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.txs.id(), self.change_mask.id(), self.index.id()]
    }

    fn evaluate(&self, ctx: &EvalContext) -> SparseDisjointSet<I::TxOutId> {
        // Use get_or_default since txs and change_mask might be part of a cycle
        let tx_ids = ctx.get_or_default(&self.txs);
        let change_mask = ctx.get_or_default(&self.change_mask);
        let index_handle = ctx.get(&self.index);
        let index_guard = index_handle.as_arc().read().expect("lock poisoned");

        let clustering = SparseDisjointSet::new();

        for tx_id in &tx_ids {
            let Some(tx) = index_guard.tx(tx_id) else {
                continue;
            };
            let first_input: Option<I::TxOutId> =
                tx.inputs().next().map(|input| input.prev_txout_id());

            let Some(root_input) = first_input else {
                continue; // Coinbase
            };

            for output in tx.outputs() {
                let txout_id = output.id();
                if change_mask.get(&txout_id).copied().unwrap_or(false) {
                    clustering.union(txout_id, root_input);
                }
            }
        }

        clustering
    }

    fn name(&self) -> &'static str {
        "ChangeClustering"
    }
}

/// Factory for creating a change clustering expression.
pub struct ChangeClustering;

impl ChangeClustering {
    /// Cluster change outputs with their transaction's inputs.
    ///
    /// Takes a set of transactions and a mask identifying change outputs.
    /// Returns a clustering where change outputs are in the same cluster as inputs.
    pub fn new<I: IdFamily + 'static, G: IndexedGraph<I> + 'static>(
        txs: Expr<TxSet<I>>,
        change_mask: Expr<TxOutMask<I>>,
        index: Expr<Index<G>>,
    ) -> Expr<TxOutClustering<I>> {
        let ctx = txs.context().clone();
        ctx.register(ChangeClusteringNode::new(txs, change_mask, index))
    }
}
