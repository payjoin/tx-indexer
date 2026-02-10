//! Source operations for the pipeline DSL.
//!
//! Source nodes produce values from external data (the index) rather than
//! transforming other expressions.

use std::collections::HashSet;
use std::sync::Arc;

use tx_indexer_primitives::abstract_id::AbstractTxId;

use crate::context::PipelineContext;
use crate::engine::SourceNodeEvalContext;
use crate::expr::Expr;
use crate::node::SourceNode;
use crate::value::TxSet;

/// Node that returns all known transaction IDs from the index.
/// Produces abstract IDs; conversion to/from concrete IDs happens at the index boundary.
pub struct AllTxsNode;

impl SourceNode for AllTxsNode {
    type OutputValue = TxSet;

    fn evaluate(&self, ctx: &mut SourceNodeEvalContext<'_>) -> HashSet<AbstractTxId> {
        let base_tx_facts = ctx.take_base_facts();
        if let Some(base_tx_facts) = base_tx_facts {
            let mut res = HashSet::new();
            ctx.with_index_mut(|index| {
                for tx in base_tx_facts {
                    res.insert(tx.id().into());
                    index.add_tx(tx);
                }
            });
            res
        } else {
            Default::default()
        }
    }

    fn name(&self) -> &'static str {
        "AllTxs"
    }
}

/// Factory for creating an AllTxs expression.
pub struct AllTxs;

impl AllTxs {
    /// Create a new expression that evaluates to all known transaction IDs (as abstract IDs).
    pub fn new(ctx: &Arc<PipelineContext>) -> Expr<TxSet> {
        ctx.register_source(AllTxsNode)
    }
}
