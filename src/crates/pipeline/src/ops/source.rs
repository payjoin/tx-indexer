//! Source operations for the pipeline DSL.
//!
//! Source nodes produce values from external data (the index) rather than
//! transforming other expressions.

use std::collections::HashSet;
use std::sync::Arc;

use tx_indexer_primitives::loose::TxId;

use crate::context::PipelineContext;
use crate::engine::EvalContext;
use crate::expr::Expr;
use crate::node::{Node, NodeId};
use crate::value::TxSet;

/// Node that returns all known transaction IDs from the index.
// TODO: this is mostly scaffolding for tests. In practice, we will likely have different types of sources.
pub struct AllTxsNode;

impl Node for AllTxsNode {
    type Value = TxSet;

    fn dependencies(&self) -> Vec<NodeId> {
        // Source node - no dependencies
        vec![]
    }

    fn evaluate(&self, ctx: &EvalContext) -> HashSet<TxId> {
        ctx.index().txs.keys().copied().collect()
    }

    fn name(&self) -> &'static str {
        "AllTxs"
    }
}

/// Factory for creating an AllTxs expression.
pub struct AllTxs;

impl AllTxs {
    /// Create a new expression that evaluates to all known transaction IDs.
    pub fn new(ctx: &Arc<PipelineContext>) -> Expr<TxSet> {
        ctx.register(AllTxsNode)
    }
}
