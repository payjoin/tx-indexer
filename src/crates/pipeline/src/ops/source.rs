//! Source operations for the pipeline DSL.
//!
//! Source nodes produce values from external data rather than transforming
//! other expressions.

use std::collections::HashSet;
use std::sync::Arc;

use tx_indexer_primitives::unified::AnyTxId;

use crate::context::PipelineContext;
use crate::engine::SourceNodeEvalContext;
use crate::expr::Expr;
use crate::node::{Node, NodeId, SourceNode};
use crate::value::TxSet;

/// Node that returns all newly observed loose transaction IDs.
pub struct AllLooseTxsNode {
    _marker: Arc<()>,
}

impl AllLooseTxsNode {
    pub fn new(marker: Arc<()>) -> Self {
        Self { _marker: marker }
    }
}

impl SourceNode for AllLooseTxsNode {
    type OutputValue = TxSet;

    fn evaluate(&self, ctx: &mut SourceNodeEvalContext<'_>) -> HashSet<AnyTxId> {
        let start = ctx.processed_loose_len();
        ctx.unified_storage
            .loose_txids_from(start)
            .into_iter()
            .collect()
    }

    fn name(&self) -> &'static str {
        "AllTxs"
    }
}

/// Node that returns all newly observed dense transaction IDs.
pub struct AllDenseTxsNode {
    _marker: Arc<()>,
}

impl AllDenseTxsNode {
    pub fn new(marker: Arc<()>) -> Self {
        Self { _marker: marker }
    }
}

impl SourceNode for AllDenseTxsNode {
    type OutputValue = TxSet;

    fn evaluate(&self, ctx: &mut SourceNodeEvalContext<'_>) -> HashSet<AnyTxId> {
        let start = ctx.processed_dense_len();
        ctx.unified_storage
            .dense_txids_from(start)
            .into_iter()
            .collect()
    }

    fn name(&self) -> &'static str {
        "AllDenseTxs"
    }
}

pub struct SourceTxsNode {
    source: Expr<TxSet>,
}

impl SourceTxsNode {
    pub fn new(source: Expr<TxSet>) -> Self {
        Self { source }
    }
}

impl Node for SourceTxsNode {
    type OutputValue = TxSet;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.source.id()]
    }

    fn evaluate(&self, ctx: &crate::engine::EvalContext) -> HashSet<AnyTxId> {
        ctx.get(&self.source).clone()
    }

    fn name(&self) -> &'static str {
        "SourceTxs"
    }
}

/// Factory for creating source expressions.
pub struct AllLooseTxs {
    txs: Expr<TxSet>,
}

impl AllLooseTxs {
    /// Create source expressions for all known transaction IDs.
    pub fn new(ctx: &Arc<PipelineContext>) -> Self {
        let source = ctx.register_source(AllLooseTxsNode::new(Arc::new(())));
        let txs = ctx.register(SourceTxsNode::new(source.clone()));
        Self { txs }
    }

    pub fn txs(&self) -> Expr<TxSet> {
        self.txs.clone()
    }
}

/// Factory for creating dense source expressions.
pub struct AllDenseTxs {
    txs: Expr<TxSet>,
}

impl AllDenseTxs {
    /// Create source expressions for all known confirmed transaction IDs.
    pub fn new(ctx: &Arc<PipelineContext>) -> Self {
        let source = ctx.register_source(AllDenseTxsNode::new(Arc::new(())));
        let txs = ctx.register(SourceTxsNode::new(source.clone()));
        Self { txs }
    }

    pub fn txs(&self) -> Expr<TxSet> {
        self.txs.clone()
    }
}
