use std::collections::HashMap;

use tx_indexer_disjoint_set::{DisJointSet, SparseDisjointSet};
use tx_indexer_pipeline::{
    engine::EvalContext,
    expr::Expr,
    node::{Node, NodeId},
    value::{TxMask, TxOutClustering, TxOutMask, TxOutSet, TxSet},
};
use tx_indexer_primitives::unified::{AnyOutId, AnyTxId};

/// Node that identifies change outputs in transactions.
///
/// Uses a naive heuristic: the last output of a transaction is assumed to be change.
pub struct ChangeIdentificationNode {
    input: Expr<TxOutSet>,
}

impl ChangeIdentificationNode {
    pub fn new(input: Expr<TxOutSet>) -> Self {
        Self { input }
    }
}

impl Node for ChangeIdentificationNode {
    type OutputValue = TxOutMask;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.input.id()]
    }

    fn evaluate(&self, ctx: &EvalContext) -> HashMap<AnyOutId, bool> {
        // Use get_or_default since input might be part of a cycle
        let txouts = ctx.get_or_default(&self.input);

        let mut result = HashMap::new();

        for output_id in txouts.iter() {
            let tx_id = ctx.unified_storage().txid_for_out(*output_id);
            let tx = ctx.unified_storage().tx(tx_id);
            let output_count = tx.output_len();
            if output_count == 0 {
                continue;
            }

            let last_output = tx
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
    pub fn new(input: Expr<TxOutSet>) -> Expr<TxOutMask> {
        let ctx = input.context().clone();
        ctx.register(ChangeIdentificationNode::new(input))
    }
}

/// Factory for creating a fingerprint-aware change identification expression.
///
/// Uses spending-tx fingerprints (e.g. n_locktime) to classify outputs as change or not:
/// when both the containing tx and the spending tx share a fingerprint (e.g. n_locktime > 0),
/// the output is classified as change.
pub struct FingerPrintChangeIdentification;

impl FingerPrintChangeIdentification {
    pub fn new(input: Expr<TxOutSet>) -> Expr<TxOutMask> {
        let ctx = input.context().clone();
        ctx.register(FingerPrintChangeIdentificationNode::new(input))
    }
}

pub struct FingerPrintChangeIdentificationNode {
    input: Expr<TxOutSet>,
}

impl FingerPrintChangeIdentificationNode {
    pub fn new(input: Expr<TxOutSet>) -> Self {
        Self { input }
    }
}

impl Node for FingerPrintChangeIdentificationNode {
    type OutputValue = TxOutMask;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.input.id()]
    }

    fn evaluate(&self, ctx: &EvalContext) -> HashMap<AnyOutId, bool> {
        // Use get_or_default since input might be part of a cycle
        let txouts = ctx.get_or_default(&self.input);

        let mut result = HashMap::new();

        for output_id in txouts.iter() {
            let tx_id = ctx.unified_storage().txid_for_out(*output_id);
            let containing_tx = ctx.unified_storage().tx(tx_id);

            let is_change = match ctx.unified_storage().spender_for_out(*output_id) {
                Some(spending_txin) => {
                    let spending_txid = ctx.unified_storage().txid_for_in(spending_txin);
                    let spending_tx = ctx.unified_storage().tx(spending_txid);
                    if containing_tx.locktime() == 0 && spending_tx.locktime() == 0 {
                        false
                    } else {
                        containing_tx.locktime() > 0 && spending_tx.locktime() > 0
                    }
                }
                None => false, // Unspent output: not change by fingerprint
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
pub struct IsUnilateralNode {
    txs: Expr<TxSet>,
    clustering: Expr<TxOutClustering>,
}

impl IsUnilateralNode {
    pub fn new(txs: Expr<TxSet>, clustering: Expr<TxOutClustering>) -> Self {
        Self { txs, clustering }
    }
}

impl Node for IsUnilateralNode {
    type OutputValue = TxMask;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.txs.id(), self.clustering.id()]
    }

    fn evaluate(&self, ctx: &EvalContext) -> HashMap<AnyTxId, bool> {
        // Use get_or_default for both; txs or clustering may not be ready yet in cyclic pipelines
        let tx_ids = ctx.get_or_default(&self.txs);
        let clustering = ctx.get_or_default(&self.clustering);

        let mut result = HashMap::new();

        for tx_id in &tx_ids {
            let tx = ctx.unified_storage().tx(*tx_id);
            let inputs: Vec<AnyOutId> =
                tx.inputs().filter_map(|input| input.prev_txout_id()).collect();

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
    pub fn with_clustering(txs: Expr<TxSet>, clustering: Expr<TxOutClustering>) -> Expr<TxMask> {
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
    change_mask: Expr<TxOutMask>,
}

impl ChangeClusteringNode {
    pub fn new(txs: Expr<TxSet>, change_mask: Expr<TxOutMask>) -> Self {
        Self { txs, change_mask }
    }
}

impl Node for ChangeClusteringNode {
    type OutputValue = TxOutClustering;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.txs.id(), self.change_mask.id()]
    }

    fn evaluate(&self, ctx: &EvalContext) -> SparseDisjointSet<AnyOutId> {
        // Use get_or_default since txs and change_mask might be part of a cycle
        let tx_ids = ctx.get_or_default(&self.txs);
        let change_mask = ctx.get_or_default(&self.change_mask);

        let clustering = SparseDisjointSet::new();

        for tx_id in &tx_ids {
            let tx = ctx.unified_storage().tx(*tx_id);

            let first_input: Option<AnyOutId> =
                tx.inputs().next().and_then(|input| input.prev_txout_id());

            if let Some(root_input) = first_input {
                for output in tx.outputs() {
                    let txout_id = output.id();
                    if change_mask.get(&txout_id).copied().unwrap_or(false) {
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
    pub fn new(txs: Expr<TxSet>, change_mask: Expr<TxOutMask>) -> Expr<TxOutClustering> {
        let ctx = txs.context().clone();
        ctx.register(ChangeClusteringNode::new(txs, change_mask))
    }
}
