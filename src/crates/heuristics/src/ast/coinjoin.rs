use std::collections::HashMap;

use pipeline::engine::EvalContext;
use pipeline::expr::Expr;
use pipeline::node::{Node, NodeId};
use pipeline::value::{Mask, TxSet};
use tx_indexer_primitives::abstract_id::AbstractTxId;

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
    type OutputValue = Mask<AbstractTxId>;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.input.id()]
    }

    fn evaluate(&self, ctx: &EvalContext) -> HashMap<AbstractTxId, bool> {
        let tx_ids = ctx.get(&self.input);
        let index = ctx.index();
        tx_ids
            .iter()
            .filter_map(|tx_id| {
                tx_id.try_as_loose().map(|concrete_id| {
                    let tx = concrete_id.with(index);
                    (
                        AbstractTxId::from(tx.id()),
                        NaiveCoinjoinDetection::is_coinjoin(&tx),
                    )
                })
            })
            .collect()
    }

    fn name(&self) -> &'static str {
        "IsCoinJoin"
    }
}

pub struct IsCoinJoin;

impl IsCoinJoin {
    pub fn new(input: Expr<TxSet>) -> Expr<Mask<AbstractTxId>> {
        let ctx = input.context().clone();
        ctx.register(IsCoinJoinNode::new(input))
    }
}
