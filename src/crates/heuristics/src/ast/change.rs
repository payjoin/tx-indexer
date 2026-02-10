use std::collections::HashMap;

use pipeline::engine::EvalContext;
use pipeline::expr::Expr;
use pipeline::node::{Node, NodeId};
use pipeline::value::{Backend, Clustering, Index, Mask, TxOutSet, TxSet};
use tx_indexer_primitives::abstract_types::TxConstituent;
use tx_indexer_primitives::disjoint_set::{DisJointSet, SparseDisjointSet};
use tx_indexer_primitives::graph_index::{
    IndexHandleFor, TxHandleLike, TxIdIndexOps, TxOutHandleLike, TxOutIdWithIndex, WithIndex,
};

use crate::change_identification::{
    NLockTimeChangeIdentification, NaiveChangeIdentificationHueristic, TxOutChangeAnnotation,
};

/// Adapter so that we can pass `dyn TxOutHandleLike` to change detection (which expects TxConstituent).
struct TxOutConstituentAdapter<'a, I: tx_indexer_primitives::abstract_id::AbstractId>(
    &'a dyn TxOutHandleLike<I>,
);

impl<I: tx_indexer_primitives::abstract_id::AbstractId> TxConstituent
    for TxOutConstituentAdapter<'_, I>
{
    type Handle<'a> = Box<dyn TxHandleLike<I> + 'a>;

    fn containing_tx(&self) -> Self::Handle<'_> {
        self.0.tx()
    }

    fn vout(&self) -> usize {
        self.0.vout() as usize
    }
}

/// Node that identifies change outputs in transactions.
pub struct ChangeIdentificationNode<I: Backend> {
    input: Expr<TxOutSet<I::TxOutId>>,
    index: Expr<Index<I>>,
}

impl<I: Backend> ChangeIdentificationNode<I>
where
    I::TxOutId: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
{
    pub fn new(input: Expr<TxOutSet<I::TxOutId>>, index: Expr<Index<I>>) -> Self {
        Self { input, index }
    }
}

impl<I: Backend> Node for ChangeIdentificationNode<I>
where
    I::TxOutId: Eq + std::hash::Hash + Clone + Send + Sync + 'static + TxOutIdWithIndex<I>,
{
    type OutputValue = Mask<I::TxOutId>;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.input.id(), self.index.id()]
    }

    fn evaluate(&self, ctx: &EvalContext) -> HashMap<I::TxOutId, bool> {
        let txouts = ctx.get_or_default(&self.input);
        let index_handle = ctx.get(&self.index);

        index_handle.with_graph(|graph| {
            let mut result = HashMap::new();

            for output_id in txouts.iter() {
                output_id.with_index_apply(graph, |output| {
                    let tx = output.tx();
                    let output_count = tx.output_count();
                    if output_count == 0 {
                        return;
                    }

                    let adapter = TxOutConstituentAdapter(&output as &dyn TxOutHandleLike<I>);
                    let is_change =
                        NaiveChangeIdentificationHueristic::is_change(adapter) == TxOutChangeAnnotation::Change;
                    result.insert(output_id.clone(), is_change);
                });
            }

            result
        })
    }

    fn name(&self) -> &'static str {
        "ChangeIdentification"
    }
}

/// Factory for creating a change identification expression.
pub struct ChangeIdentification;

impl ChangeIdentification {
    pub fn new<I: Backend>(
        input: Expr<TxOutSet<I::TxOutId>>,
        index: Expr<Index<I>>,
    ) -> Expr<Mask<I::TxOutId>>
    where
        I::TxOutId: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
    {
        let ctx = input.context().clone();
        ctx.register(ChangeIdentificationNode::new(input, index))
    }
}

/// Fingerprint-aware change identification.
pub struct FingerPrintChangeIdentification;

impl FingerPrintChangeIdentification {
    pub fn new<I: Backend>(
        input: Expr<TxOutSet<I::TxOutId>>,
        index: Expr<Index<I>>,
    ) -> Expr<Mask<I::TxOutId>>
    where
        I::TxOutId: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
    {
        let ctx = input.context().clone();
        ctx.register(FingerPrintChangeIdentificationNode::new(input, index))
    }
}

pub struct FingerPrintChangeIdentificationNode<I: Backend> {
    input: Expr<TxOutSet<I::TxOutId>>,
    index: Expr<Index<I>>,
}

impl<I: Backend> FingerPrintChangeIdentificationNode<I>
where
    I::TxOutId: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
{
    pub fn new(input: Expr<TxOutSet<I::TxOutId>>, index: Expr<Index<I>>) -> Self {
        Self { input, index }
    }
}

impl<I: Backend> Node for FingerPrintChangeIdentificationNode<I>
where
    I::TxOutId: Eq + std::hash::Hash + Clone + Send + Sync + 'static + TxOutIdWithIndex<I>,
{
    type OutputValue = Mask<I::TxOutId>;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.input.id(), self.index.id()]
    }

    fn evaluate(&self, ctx: &EvalContext) -> HashMap<I::TxOutId, bool> {
        let txouts = ctx.get_or_default(&self.input);
        let index_handle = ctx.get(&self.index);

        index_handle.with_graph(|graph| {
            let mut result = HashMap::new();

            for output_id in txouts.iter() {
                output_id.with_index_apply(graph, |output| {
                    if let Some(spending_txin) = output.spent_by() {
                        let spending_tx_handle = spending_txin.tx();
                        let adapter = TxOutConstituentAdapter(&output as &dyn TxOutHandleLike<I>);
                        let is_change = NLockTimeChangeIdentification::is_change(
                            adapter,
                            spending_tx_handle.as_ref(),
                        ) == TxOutChangeAnnotation::Change;
                        result.insert(output_id.clone(), is_change);
                    }
                });
            }

            result
        })
    }

    fn name(&self) -> &'static str {
        "FingerPrintChangeIdentification"
    }
}

/// Node that checks if a transaction's inputs are all in the same cluster.
pub struct IsUnilateralNode<I: Backend> {
    txs: Expr<TxSet<I::TxId>>,
    clustering: Expr<Clustering<I::TxOutId>>,
    index: Expr<Index<I>>,
}

impl<I: Backend> IsUnilateralNode<I>
where
    I::TxId: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
    I::TxOutId: Eq + std::hash::Hash + Copy + Send + Sync + 'static,
{
    pub fn new(
        txs: Expr<TxSet<I::TxId>>,
        clustering: Expr<Clustering<I::TxOutId>>,
        index: Expr<Index<I>>,
    ) -> Self {
        Self {
            txs,
            clustering,
            index,
        }
    }
}

impl<I: Backend> Node for IsUnilateralNode<I>
where
    I::TxId: Eq + std::hash::Hash + Clone + Send + Sync + 'static + WithIndex<I>,
    I::TxOutId: Eq + std::hash::Hash + Copy + Send + Sync + 'static,
{
    type OutputValue = Mask<I::TxId>;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.txs.id(), self.clustering.id(), self.index.id()]
    }

    fn evaluate(&self, ctx: &EvalContext) -> HashMap<I::TxId, bool> {
        let tx_ids = ctx.get_or_default(&self.txs);
        let clustering = ctx.get_or_default(&self.clustering);
        let index_handle = ctx.get(&self.index);

        index_handle.with_graph(|graph| {
            let mut result = HashMap::new();

            for tx_id in &tx_ids {
                let tx = tx_id.with_index(graph);
                let inputs: Vec<I::TxOutId> = tx.spent_coins().collect();

                let is_unilateral = if inputs.is_empty() {
                    false
                } else if inputs.len() == 1 {
                    true
                } else {
                    let first_root = clustering.find(inputs[0]);
                    inputs
                        .iter()
                        .all(|input| clustering.find(*input) == first_root)
                };

                result.insert(tx_id.clone(), is_unilateral);
            }

            result
        })
    }

    fn name(&self) -> &'static str {
        "IsUnilateral"
    }
}

/// Factory for creating an IsUnilateral expression.
pub struct IsUnilateral;

impl IsUnilateral {
    pub fn with_clustering<I: Backend>(
        txs: Expr<TxSet<I::TxId>>,
        clustering: Expr<Clustering<I::TxOutId>>,
        index: Expr<Index<I>>,
    ) -> Expr<Mask<I::TxId>>
    where
        I::TxId: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
        I::TxOutId: Eq + std::hash::Hash + Copy + Send + Sync + 'static,
    {
        let ctx = txs.context().clone();
        ctx.register(IsUnilateralNode::new(txs, clustering, index))
    }
}

/// Node that clusters change outputs with their transaction's inputs.
pub struct ChangeClusteringNode<I: Backend> {
    txs: Expr<TxSet<I::TxId>>,
    change_mask: Expr<Mask<I::TxOutId>>,
    index: Expr<Index<I>>,
}

impl<I: Backend> ChangeClusteringNode<I>
where
    I::TxId: Eq + std::hash::Hash + Clone + Send + Sync + 'static + TxIdIndexOps<I>,
    I::TxOutId: Eq + std::hash::Hash + Copy + Send + Sync + 'static,
{
    pub fn new(
        txs: Expr<TxSet<I::TxId>>,
        change_mask: Expr<Mask<I::TxOutId>>,
        index: Expr<Index<I>>,
    ) -> Self {
        Self {
            txs,
            change_mask,
            index,
        }
    }
}

impl<I: Backend> Node for ChangeClusteringNode<I>
where
    I::TxId: Eq + std::hash::Hash + Clone + Send + Sync + 'static + TxIdIndexOps<I> + WithIndex<I>,
    I::TxOutId: Eq + std::hash::Hash + Copy + Send + Sync + 'static,
{
    type OutputValue = Clustering<I::TxOutId>;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.txs.id(), self.change_mask.id(), self.index.id()]
    }

    fn evaluate(&self, ctx: &EvalContext) -> SparseDisjointSet<I::TxOutId> {
        let tx_ids = ctx.get_or_default(&self.txs);
        let change_mask = ctx.get_or_default(&self.change_mask);
        let index_handle = ctx.get(&self.index);

        index_handle.with_graph(|graph| {
            let mut clustering = SparseDisjointSet::new();

            for tx_id in &tx_ids {
                let tx = tx_id.with_index(graph);
                let first_input: Option<I::TxOutId> = tx.spent_coins().next();

                let Some(root_input) = first_input else {
                    continue;
                };

                let output_count = tx.output_count();
                for vout in 0..output_count {
                    let txout_id = tx_id.clone().txout_id(vout as u32);
                    if change_mask.get(&txout_id).copied().unwrap_or(false) {
                        clustering.union(txout_id, root_input);
                    }
                }
            }

            clustering
        })
    }

    fn name(&self) -> &'static str {
        "ChangeClustering"
    }
}

/// Factory for creating a change clustering expression.
pub struct ChangeClustering;

impl ChangeClustering {
    pub fn new<I: Backend>(
        txs: Expr<TxSet<I::TxId>>,
        change_mask: Expr<Mask<I::TxOutId>>,
        index: Expr<Index<I>>,
    ) -> Expr<Clustering<I::TxOutId>>
    where
        I::TxId: Eq + std::hash::Hash + Clone + Send + Sync + 'static + TxIdIndexOps<I>,
        I::TxOutId: Eq + std::hash::Hash + Copy + Send + Sync + 'static,
    {
        let ctx = txs.context().clone();
        ctx.register(ChangeClusteringNode::new(txs, change_mask, index))
    }
}
