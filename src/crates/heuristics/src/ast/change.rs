//! Change identification and clustering heuristics for the pipeline DSL.

use std::collections::HashMap;

use pipeline::engine::EvalContext;
use pipeline::expr::Expr;
use pipeline::node::{Node, NodeId};
use pipeline::value::{Clustering, Mask, TxSet};
use tx_indexer_primitives::disjoint_set::{DisJointSet, SparseDisjointSet};
use tx_indexer_primitives::loose::{TxId, TxOutId};

use crate::change_identification::NaiveChangeIdentificationHueristic;

/// Node that identifies change outputs in transactions.
///
/// Uses a naive heuristic: the last output of a transaction is assumed to be change.
pub struct ChangeIdentificationNode {
    input: Expr<TxSet>,
}

impl ChangeIdentificationNode {
    pub fn new(input: Expr<TxSet>) -> Self {
        Self { input }
    }
}

impl Node for ChangeIdentificationNode {
    type OutputValue = Mask<TxOutId>;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.input.id()]
    }

    fn evaluate(&self, ctx: &EvalContext) -> HashMap<TxOutId, bool> {
        // Use get_or_default since input might be part of a cycle
        let tx_ids = ctx.get_or_default(&self.input);
        let index = ctx.index();

        let mut result = HashMap::new();

        for &tx_id in &tx_ids {
            if let Some(tx) = index.txs.get(&tx_id) {
                let output_count = tx.output_len();
                if output_count == 0 {
                    continue;
                }

                // Mark each output using the naive change identification heuristic
                for vout in 0..output_count {
                    let txout_id = TxOutId::new(tx_id, vout as u32);
                    let is_change = NaiveChangeIdentificationHueristic::is_change_vout(tx, vout);
                    result.insert(txout_id, is_change);
                }
            }
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
    pub fn new(input: Expr<TxSet>) -> Expr<Mask<TxOutId>> {
        let ctx = input.context().clone();
        ctx.register(ChangeIdentificationNode::new(input))
    }
}

/// Node that checks if a transaction's inputs are all in the same cluster.
///
/// This is used to gate change clustering - we only cluster change with inputs
/// if we're confident all inputs belong to the same entity.
pub struct IsUnilateralNode {
    txs: Expr<TxSet>,
    clustering: Expr<Clustering>,
}

impl IsUnilateralNode {
    pub fn new(txs: Expr<TxSet>, clustering: Expr<Clustering>) -> Self {
        Self { txs, clustering }
    }
}

impl Node for IsUnilateralNode {
    type OutputValue = Mask<TxId>;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.txs.id(), self.clustering.id()]
    }

    fn evaluate(&self, ctx: &EvalContext) -> HashMap<TxId, bool> {
        let tx_ids = ctx.get(&self.txs);
        // Use get_or_default for clustering since it might be part of a cycle
        // During initial fixpoint iteration, this will return an empty clustering
        let clustering = ctx.get_or_default(&self.clustering);
        let index = ctx.index();

        let mut result = HashMap::new();

        for &tx_id in tx_ids {
            if let Some(tx) = index.txs.get(&tx_id) {
                let inputs: Vec<_> = tx
                    .inputs()
                    .map(|input| TxOutId::new(input.prev_txid(), input.prev_vout()))
                    .collect();

                let is_unilateral = if inputs.is_empty() {
                    false // Coinbase - no inputs to cluster
                } else if inputs.len() == 1 {
                    true // Single input is trivially unilateral
                } else {
                    // TODO: update to use the util on tx handle
                    // Check if all inputs are in the same cluster
                    let first_root = clustering.find(inputs[0]);
                    inputs
                        .iter()
                        .all(|&input| clustering.find(input) == first_root)
                };

                result.insert(tx_id, is_unilateral);
            }
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
    pub fn with_clustering(txs: Expr<TxSet>, clustering: Expr<Clustering>) -> Expr<Mask<TxId>> {
        let ctx = txs.context().clone();
        ctx.register(IsUnilateralNode::new(txs, clustering))
    }
}

/// Node that clusters change outputs with their transaction's inputs.
///
/// For each transaction, if inputs are unilateral (all in same cluster) and has change outputs,
/// cluster the change outputs with the inputs.
pub struct ChangeClusteringNode {
    txs: Expr<TxSet>,
    change_mask: Expr<Mask<TxOutId>>,
}

impl ChangeClusteringNode {
    pub fn new(txs: Expr<TxSet>, change_mask: Expr<Mask<TxOutId>>) -> Self {
        Self { txs, change_mask }
    }
}

impl Node for ChangeClusteringNode {
    type OutputValue = Clustering;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.txs.id(), self.change_mask.id()]
    }

    fn evaluate(&self, ctx: &EvalContext) -> SparseDisjointSet<TxOutId> {
        // Use get_or_default since txs and change_mask might be part of a cycle
        let tx_ids = ctx.get_or_default(&self.txs);
        let change_mask = ctx.get_or_default(&self.change_mask);
        let index = ctx.index();

        let clustering = SparseDisjointSet::new();

        for &tx_id in &tx_ids {
            if let Some(tx) = index.txs.get(&tx_id) {
                // Get first input (if any)
                let first_input: Option<TxOutId> = tx
                    .inputs()
                    .next()
                    .map(|input| TxOutId::new(input.prev_txid(), input.prev_vout()));

                let Some(root_input) = first_input else {
                    continue; // Coinbase
                };

                // Find change outputs for this transaction
                let output_count = tx.output_len();
                for vout in 0..output_count {
                    let txout_id = TxOutId::new(tx_id, vout as u32);
                    if change_mask.get(&txout_id).copied().unwrap_or(false) {
                        // This is a change output - cluster it with inputs
                        clustering.union(txout_id, root_input);
                    }
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
    pub fn new(txs: Expr<TxSet>, change_mask: Expr<Mask<TxOutId>>) -> Expr<Clustering> {
        let ctx = txs.context().clone();
        ctx.register(ChangeClusteringNode::new(txs, change_mask))
    }
}
