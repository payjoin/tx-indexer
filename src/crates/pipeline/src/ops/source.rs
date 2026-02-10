//! Source operations for the pipeline DSL.
//!
//! Source nodes produce values from external data. The AllTxs source creates
//! the index and returns both the set of transaction IDs and the index handle;
//! use `.txs()` and `.index()` to obtain expressions for each.

use std::collections::HashSet;
use std::sync::{Arc, RwLock};

use tx_indexer_primitives::abstract_id::LooseIds;
use tx_indexer_primitives::loose::{storage::InMemoryIndex};

use crate::context::PipelineContext;
use crate::engine::SourceNodeEvalContext;
use crate::expr::Expr;
use crate::node::{Node, NodeId, SourceNode};
use crate::value::{AllTxsOutput, Index, LooseIndexHandle, TxSet};

/// Node that returns all known transaction IDs and creates the index.
/// Produces (TxSet, index handle); dependents get the index via a projection.
pub struct AllTxsNode;

impl SourceNode for AllTxsNode {
    type OutputValue = AllTxsOutput<LooseIds>;

    fn evaluate(
        &self,
        ctx: &mut SourceNodeEvalContext<'_>,
    ) -> <Self::OutputValue as crate::value::ExprValue>::Output {
        let base_tx_facts = ctx.take_base_facts();
        if let Some(base_tx_facts) = base_tx_facts {
            let mut index = InMemoryIndex::new();
            let mut res = HashSet::new();
            for tx in base_tx_facts {
                let id = tx.id();
                res.insert(id);
                index.add_tx(tx);
            }
            (res, LooseIndexHandle(Arc::new(RwLock::new(index))))
        } else {
            (
                HashSet::new(),
                LooseIndexHandle(Arc::new(RwLock::new(InMemoryIndex::new()))),
            )
        }
    }

    fn name(&self) -> &'static str {
        "AllTxs"
    }
}

/// Factory for creating an AllTxs expression.
pub struct AllTxs;

impl AllTxs {
    /// Create a new expression that evaluates to (tx set, index handle).
    /// Use `.txs()` and `.index()` on the result to get the component expressions.
    pub fn new(ctx: &Arc<PipelineContext>) -> Expr<AllTxsOutput<LooseIds>> {
        ctx.register_source(AllTxsNode)
    }
}

// --- Projection nodes: AllTxsOutput -> TxSet or Index ---

use crate::engine::EvalContext;
use crate::value::{Backend, ExprValue};

/// Projects the transaction set from an AllTxsOutput.
pub struct ProjectTxSetNode<I: Backend + Send + Sync + 'static>
where
    I::TxId: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
{
    input: Expr<AllTxsOutput<I>>,
}

impl<I: Backend + Send + Sync + 'static> ProjectTxSetNode<I>
where
    I::TxId: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
{
    pub fn new(input: Expr<AllTxsOutput<I>>) -> Self {
        Self { input }
    }
}

impl<I: Backend + Send + Sync + 'static> Node for ProjectTxSetNode<I>
where
    I::TxId: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
{
    type OutputValue = TxSet<I::TxId>;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.input.id()]
    }

    fn evaluate(&self, ctx: &EvalContext) -> <TxSet<I::TxId> as ExprValue>::Output {
        let pair = ctx.get(&self.input);
        pair.0.clone()
    }

    fn name(&self) -> &'static str {
        "ProjectTxSet"
    }
}

/// Projects the index handle from an AllTxsOutput.
pub struct ProjectIndexNode<I: Backend + Send + Sync + 'static>
where
    I::TxId: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
{
    input: Expr<AllTxsOutput<I>>,
}

impl<I: Backend + Send + Sync + 'static> ProjectIndexNode<I>
where
    I::TxId: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
{
    pub fn new(input: Expr<AllTxsOutput<I>>) -> Self {
        Self { input }
    }
}

impl<I: Backend + Send + Sync + 'static> Node for ProjectIndexNode<I>
where
    I::TxId: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
{
    type OutputValue = Index<I>;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.input.id()]
    }

    fn evaluate(&self, ctx: &EvalContext) -> <Index<I> as ExprValue>::Output {
        let pair = ctx.get(&self.input);
        pair.1.clone()
    }

    fn name(&self) -> &'static str {
        "ProjectIndex"
    }
}

// Extension methods on Expr<AllTxsOutput<I>>
impl<I: Backend + Send + Sync + 'static> Expr<AllTxsOutput<I>>
where
    I::TxId: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
{
    /// Project the transaction set from the AllTxs output.
    pub fn txs(&self) -> Expr<TxSet<I::TxId>> {
        self.ctx.register(ProjectTxSetNode::new(self.clone()))
    }

    /// Project the index handle from the AllTxs output.
    pub fn index(&self) -> Expr<Index<I>> {
        self.ctx.register(ProjectIndexNode::new(self.clone()))
    }
}
