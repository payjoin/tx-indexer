//! Source operations for the pipeline DSL.
//!
//! Source nodes produce values from external data rather than transforming
//! other expressions.

use std::sync::Arc;

use tx_indexer_primitives::unified::AnyTxId;

use crate::context::PipelineContext;
use crate::engine::SourceNodeEvalContext;
use crate::expr::Expr;
use crate::node::SourceNode;
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

    fn evaluate(&self, ctx: &mut SourceNodeEvalContext<'_>) -> Vec<AnyTxId> {
        let start = ctx.processed_loose_len();
        ctx.unified_storage.loose_txids_from(start).collect()
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

    fn evaluate(&self, ctx: &mut SourceNodeEvalContext<'_>) -> Vec<AnyTxId> {
        let start = ctx.processed_dense_len();
        ctx.unified_storage.dense_txids_from(start).collect()
    }

    fn name(&self) -> &'static str {
        "AllDenseTxs"
    }
}

/// Factory for creating source expressions.
pub struct AllLooseTxs {
    txs: Expr<TxSet>,
}

impl AllLooseTxs {
    /// Create source expressions for all known transaction IDs.
    pub fn new(ctx: &Arc<PipelineContext>) -> Self {
        let txs = ctx.register_source(AllLooseTxsNode::new(Arc::new(())));
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
        let txs = ctx.register_source(AllDenseTxsNode::new(Arc::new(())));
        Self { txs }
    }

    pub fn txs(&self) -> Expr<TxSet> {
        self.txs.clone()
    }
}
