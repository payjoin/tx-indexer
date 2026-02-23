use tx_indexer_pipeline::{
    engine::EvalContext,
    expr::Expr,
    node::{Node, NodeId},
    value::{TxOutClustering, TxSet},
};

use tx_indexer_disjoint_set::{DisJointSet, SparseDisjointSet};
use tx_indexer_primitives::unified::id::AnyOutId;

pub struct SameAddressClusteringNode {
    txs: Expr<TxSet>,
}

impl SameAddressClusteringNode {
    pub fn new(txs: Expr<TxSet>) -> Self {
        Self { txs }
    }
}

impl Node for SameAddressClusteringNode {
    type OutputValue = TxOutClustering;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.txs.id()]
    }

    fn evaluate(&self, ctx: &EvalContext) -> SparseDisjointSet<AnyOutId> {
        let txs = ctx.get(&self.txs);
        let clustering = SparseDisjointSet::new();

        for tx_id in txs.iter() {
            let tx = ctx.unified_storage().tx(*tx_id);
            for output in tx.outputs() {
                let txout_id = output.id();
                if let Some(first_txout) = ctx
                    .unified_storage()
                    .script_pubkey_to_txout_id(&output.script_pubkey_hash())
                {
                    clustering.union(txout_id, first_txout);
                }
            }
        }

        clustering
    }
}

#[allow(dead_code)]
pub struct SameAddressClustering;

impl SameAddressClustering {
    #[allow(dead_code)]
    pub fn new(txs: Expr<TxSet>) -> Expr<TxOutClustering> {
        let ctx = txs.context().clone();
        ctx.register(SameAddressClusteringNode::new(txs))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use tx_indexer_pipeline::{Engine, PipelineContext, ops::AllLooseTxs};
    use tx_indexer_primitives::{
        abstract_types::AbstractTransaction,
        loose::storage::LooseIndexBuilder,
        loose::{TxId, TxOutId},
        test_utils::{DummyTxData, DummyTxOutData},
        unified::id::AnyOutId,
        unified::storage::UnifiedStorageBuilder,
    };

    fn setup_test_fixture() -> Vec<Arc<dyn AbstractTransaction + Send + Sync>> {
        // Fixture: two coinbase txs, spent by two txs; two outputs each, both change outputs share same spk
        let shared_spk = [42u8; 20];
        let unique_spk1 = [1u8; 20];
        let unique_spk2 = [2u8; 20];

        // Coinbase 1
        let coinbase1 = DummyTxData {
            id: TxId(1),
            outputs: vec![DummyTxOutData::new(10_000, unique_spk1, 0, TxId(1))],
            spent_coins: vec![],
            n_locktime: 0,
        };

        // Coinbase 2
        let coinbase2 = DummyTxData {
            id: TxId(2),
            outputs: vec![DummyTxOutData::new(10_000, unique_spk2, 0, TxId(2))],
            spent_coins: vec![],
            n_locktime: 0,
        };

        // Spend coinbase 1, make payment + change (change spk is shared)
        let spend1 = DummyTxData {
            id: TxId(3),
            spent_coins: vec![TxOutId::new(TxId(1), 0)],
            outputs: vec![
                DummyTxOutData::new(4_000, unique_spk1, 0, TxId(3)), // payment
                DummyTxOutData::new(5_000, shared_spk, 1, TxId(3)),  // change (shared with spend2)
            ],
            n_locktime: 0,
        };

        // Spend coinbase 2, make payment + change (change spk is shared)
        let spend2 = DummyTxData {
            id: TxId(4),
            spent_coins: vec![TxOutId::new(TxId(2), 0)],
            outputs: vec![
                DummyTxOutData::new(4_000, unique_spk2, 0, TxId(4)), // payment
                DummyTxOutData::new(5_000, shared_spk, 1, TxId(4)),  // change (shared with spend1)
            ],
            n_locktime: 0,
        };

        vec![
            Arc::new(coinbase1),
            Arc::new(coinbase2),
            Arc::new(spend1),
            Arc::new(spend2),
        ]
    }

    fn engine_with_loose(
        ctx: Arc<PipelineContext>,
        txs: Vec<Arc<dyn AbstractTransaction + Send + Sync>>,
    ) -> Engine {
        let mut builder = LooseIndexBuilder::new();
        for tx in txs {
            builder.add_tx(tx);
        }
        let unified = UnifiedStorageBuilder::new()
            .with_loose(builder)
            .build()
            .expect("build unified storage")
            .storage;
        Engine::new(ctx, Arc::new(unified))
    }

    #[test]
    fn test_same_address_clustering() {
        let all_txs = setup_test_fixture();
        let ctx = Arc::new(PipelineContext::new());
        let mut engine = engine_with_loose(ctx.clone(), all_txs);

        let source = AllLooseTxs::new(&ctx);
        let all_txs = source.txs();
        let clustering = SameAddressClustering::new(all_txs);
        engine.run_to_fixpoint();
        let result = engine.eval(&clustering);

        // Same spk should be clustered together
        assert_eq!(
            result.find(AnyOutId::from(TxOutId::new(TxId(3), 1))),
            result.find(AnyOutId::from(TxOutId::new(TxId(4), 1)))
        );

        // Other outputs should not be clustered together
        assert_ne!(
            result.find(AnyOutId::from(TxOutId::new(TxId(3), 0))),
            result.find(AnyOutId::from(TxOutId::new(TxId(4), 0)))
        );
    }
}
