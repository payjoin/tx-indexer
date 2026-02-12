//! Source operations for the pipeline DSL.
//!
//! Source nodes produce values from external data (the index) rather than
//! transforming other expressions.

use std::collections::HashSet;
use std::sync::{Arc, RwLock};

use tx_indexer_primitives::loose;
use tx_indexer_primitives::loose::LooseIds;
use tx_indexer_primitives::loose::storage::InMemoryIndex;

use crate::context::PipelineContext;
use crate::engine::SourceNodeEvalContext;
use crate::expr::Expr;
use crate::node::{Node, NodeId, SourceNode};
use crate::value::{Index, IndexHandle, SourceOutput, SourceOutputData, TxSet};

/// Node that returns the source output (index + all known transaction IDs).
pub struct AllLooseTxsNode {
    index: Arc<RwLock<InMemoryIndex>>,
}

impl AllLooseTxsNode {
    pub fn new(index: Arc<RwLock<InMemoryIndex>>) -> Self {
        Self { index }
    }
}

impl SourceNode for AllLooseTxsNode {
    type OutputValue = SourceOutput<LooseIds, InMemoryIndex>;

    fn evaluate(
        &self,
        ctx: &mut SourceNodeEvalContext<'_>,
    ) -> SourceOutputData<LooseIds, InMemoryIndex> {
        let base_tx_facts = ctx.take_base_facts();
        let txs = if let Some(base_tx_facts) = base_tx_facts {
            let mut res = HashSet::new();
            let mut index = self.index.write().expect("lock poisoned");
            for tx in base_tx_facts {
                res.insert(tx.id());
                index.add_tx(tx);
            }
            res
        } else {
            HashSet::new()
        };

        SourceOutputData {
            index: IndexHandle::new(Arc::clone(&self.index)),
            txs,
        }
    }

    fn name(&self) -> &'static str {
        "AllTxs"
    }
}

pub struct SourceIndexNode {
    source: Expr<SourceOutput<LooseIds, InMemoryIndex>>,
}

impl SourceIndexNode {
    pub fn new(source: Expr<SourceOutput<LooseIds, InMemoryIndex>>) -> Self {
        Self { source }
    }
}

impl Node for SourceIndexNode {
    type OutputValue = Index<InMemoryIndex>;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.source.id()]
    }

    fn evaluate(&self, ctx: &crate::engine::EvalContext) -> IndexHandle<InMemoryIndex> {
        ctx.get(&self.source).index.clone()
    }

    fn name(&self) -> &'static str {
        "SourceIndex"
    }
}

pub struct SourceTxsNode {
    source: Expr<SourceOutput<LooseIds, InMemoryIndex>>,
}

impl SourceTxsNode {
    pub fn new(source: Expr<SourceOutput<LooseIds, InMemoryIndex>>) -> Self {
        Self { source }
    }
}

impl Node for SourceTxsNode {
    type OutputValue = TxSet<LooseIds>;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.source.id()]
    }

    fn evaluate(&self, ctx: &crate::engine::EvalContext) -> HashSet<loose::TxId> {
        ctx.get(&self.source).txs.clone()
    }

    fn name(&self) -> &'static str {
        "SourceTxs"
    }
}

/// Factory for creating source expressions.
pub struct AllLooseTxs {
    txs: Expr<TxSet<LooseIds>>,
    index: Expr<Index<InMemoryIndex>>,
}

impl AllLooseTxs {
    /// Create source expressions for the index and all known transaction IDs.
    pub fn new(ctx: &Arc<PipelineContext>) -> Self {
        let source = ctx.register_source(AllLooseTxsNode::new(Arc::new(RwLock::new(
            InMemoryIndex::new(),
        ))));
        let txs = ctx.register(SourceTxsNode::new(source.clone()));
        let index = ctx.register(SourceIndexNode::new(source));
        Self { txs, index }
    }

    pub fn txs(&self) -> Expr<TxSet<LooseIds>> {
        self.txs.clone()
    }

    pub fn index(&self) -> Expr<Index<InMemoryIndex>> {
        self.index.clone()
    }
}
