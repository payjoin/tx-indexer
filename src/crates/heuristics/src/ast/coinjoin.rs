//! CoinJoin detection heuristic for the pipeline DSL.

use std::collections::HashMap;

use pipeline::engine::EvalContext;
use pipeline::expr::Expr;
use pipeline::node::{Node, NodeId};
use pipeline::value::{Mask, TxSet};
use tx_indexer_primitives::loose::TxId;

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
    type Value = Mask<TxId>;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.input.id()]
    }

    fn evaluate(&self, ctx: &EvalContext) -> HashMap<TxId, bool> {
        let tx_ids = ctx.get(&self.input);
        let index = ctx.index();

        let mut result = HashMap::new();
        for &tx_id in tx_ids {
            let is_coinjoin = if let Some(tx) = index.txs.get(&tx_id) {
                // Count output values
                let mut value_counts: HashMap<u64, usize> = HashMap::new();
                for output in tx.outputs() {
                    let value = output.value().to_sat();
                    *value_counts.entry(value).or_insert(0) += 1;
                }
                // If any value appears >= 3 times, it's a CoinJoin
                value_counts.values().any(|&count| count >= 3)
            } else {
                false
            };
            result.insert(tx_id, is_coinjoin);
        }
        result
    }

    fn name(&self) -> &'static str {
        "IsCoinJoin"
    }
}

/// Factory for creating an IsCoinJoin expression.
pub struct IsCoinJoin;

impl IsCoinJoin {
    /// Create a new CoinJoin detection mask over the given transactions.
    ///
    /// Returns a mask where `true` indicates the transaction is a CoinJoin.
    pub fn new(input: Expr<TxSet>) -> Expr<Mask<TxId>> {
        let ctx = input.context().clone();
        ctx.register(IsCoinJoinNode::new(input))
    }
}
