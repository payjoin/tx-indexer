use std::collections::HashMap;

use tx_indexer_pipeline::{
    engine::EvalContext,
    expr::Expr,
    node::{Node, NodeId},
    value::{Mask, TxSet},
};
use tx_indexer_primitives::unified::id::AnyTxId;

use crate::coinjoin_detection::NaiveCoinjoinDetection;

/// Node that detects CoinJoin transactions.
///
/// Uses a naive heuristic: if there are >= 3 outputs of the same value,
/// the transaction is classified as a CoinJoin.
pub struct IsCoinJoinNode {
    input: Expr<TxSet>,
}

impl IsCoinJoinNode {
    pub fn new(input: Expr<TxSet>) -> Self {
        Self { input }
    }
}

impl Node for IsCoinJoinNode {
    type OutputValue = Mask<AnyTxId>;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.input.id()]
    }

    fn evaluate(&self, ctx: &EvalContext) -> HashMap<AnyTxId, bool> {
        let tx_ids = ctx.get(&self.input);
        tx_ids
            .iter()
            .map(|tx_id| {
                let tx = ctx.unified_storage().tx(*tx_id);
                (*tx_id, NaiveCoinjoinDetection::is_coinjoin(&tx))
            })
            .collect()
    }

    fn name(&self) -> &'static str {
        "IsCoinJoin"
    }
}

pub struct IsCoinJoin;

impl IsCoinJoin {
    pub fn new(input: Expr<TxSet>) -> Expr<Mask<AnyTxId>> {
        let ctx = input.context().clone();
        ctx.register(IsCoinJoinNode::new(input))
    }
}
