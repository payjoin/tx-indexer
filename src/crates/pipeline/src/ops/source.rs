//! Source operations for the pipeline DSL.
//!
//! Source nodes produce values from external data (the index) rather than
//! transforming other expressions.

use std::collections::HashSet;
use std::sync::Arc;

use tx_indexer_primitives::loose;

use crate::context::PipelineContext;
use crate::engine::SourceNodeEvalContext;
use crate::expr::Expr;
use crate::node::SourceNode;
use crate::value::Set;

/// Node that returns all known transaction IDs from the index.
/// Produces abstract IDs; conversion to/from concrete IDs happens at the index boundary.
pub struct AllLooseTxsNode;

impl SourceNode for AllLooseTxsNode {
    type OutputValue = Set<loose::TxId>;

    fn evaluate(&self, ctx: &mut SourceNodeEvalContext<'_>) -> HashSet<loose::TxId> {
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
pub struct AllLooseTxs;

impl AllLooseTxs {
    /// Create a new expression that evaluates to all known transaction IDs (as abstract IDs).
    pub fn new(ctx: &Arc<PipelineContext>) -> Expr<Set<loose::TxId>> {
        ctx.register_source(AllLooseTxsNode)
    }
}
