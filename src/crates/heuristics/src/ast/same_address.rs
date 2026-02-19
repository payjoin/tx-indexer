use tx_indexer_pipeline::{
    engine::EvalContext,
    expr::Expr,
    node::{Node, NodeId},
    value::{Index, TxOutClustering, TxSet},
};

use tx_indexer_primitives::{
    abstract_types::{IdFamily, IntoTxHandle},
    disjoint_set::{DisJointSet, SparseDisjointSet},
    graph_index::IndexedGraph,
};

pub struct SameAddressClusteringNode<I: IdFamily + 'static, G: IndexedGraph<I> + 'static> {
    txs: Expr<TxSet<I>>,
    index: Expr<Index<G>>,
}

impl<I: IdFamily + 'static, G: IndexedGraph<I> + 'static> SameAddressClusteringNode<I, G> {
    pub fn new(txs: Expr<TxSet<I>>, index: Expr<Index<G>>) -> Self {
        Self { txs, index }
    }
}

impl<I: IdFamily + 'static, G: IndexedGraph<I> + 'static> Node for SameAddressClusteringNode<I, G> {
    type OutputValue = TxOutClustering<I>;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.txs.id(), self.index.id()]
    }

    fn evaluate(&self, ctx: &EvalContext) -> SparseDisjointSet<I::TxOutId> {
        let txs = ctx.get(&self.txs);
        let index_handle = ctx.get(&self.index);
        let index_guard = index_handle.as_arc().read().expect("lock poisoned");
        let clustering = SparseDisjointSet::new();

        for tx_id in txs.iter() {
            let tx = tx_id.with_index(&*index_guard);
            for output in tx.outputs() {
                let txout_id = output.id();
                if let Some(first_txout) =
                    index_guard.script_pubkey_to_txout_id(&output.script_pubkey_hash())
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
    pub fn new<I: IdFamily + 'static, G: IndexedGraph<I> + 'static>(
        txs: Expr<TxSet<I>>,
        index: Expr<Index<G>>,
    ) -> Expr<TxOutClustering<I>> {
        let ctx = txs.context().clone();
        ctx.register(SameAddressClusteringNode::new(txs, index))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use tx_indexer_pipeline::{Engine, PipelineContext, ops::AllLooseTxs};
    use tx_indexer_primitives::{
        abstract_types::AbstractTransaction,
        loose::{LooseIds, TxId, TxOutId},
        test_utils::{DummyTxData, DummyTxOutData},
    };

    fn setup_test_fixture() -> Vec<Arc<dyn AbstractTransaction<I = LooseIds> + Send + Sync>> {
        // Fixture: two coinbase txs, spent by two txs; two outputs each, both change outputs share same spk
        let shared_spk = [42u8; 20];
        let unique_spk1 = [1u8; 20];
        let unique_spk2 = [2u8; 20];

        // Coinbase 1
        let coinbase1 = DummyTxData {
            id: TxId(0),
            outputs: vec![DummyTxOutData::new(10_000, unique_spk1, 0, TxId(0))],
            spent_coins: vec![],
            n_locktime: 0,
        };

        // Coinbase 2
        let coinbase2 = DummyTxData {
            id: TxId(1),
            outputs: vec![DummyTxOutData::new(10_000, unique_spk2, 0, TxId(1))],
            spent_coins: vec![],
            n_locktime: 0,
        };

        // Spend coinbase 1, make payment + change (change spk is shared)
        let spend1 = DummyTxData {
            id: TxId(2),
            spent_coins: vec![TxOutId::new(TxId(0), 0)],
            outputs: vec![
                DummyTxOutData::new(4_000, unique_spk1, 0, TxId(2)), // payment
                DummyTxOutData::new(5_000, shared_spk, 1, TxId(2)),  // change (shared with spend2)
            ],
            n_locktime: 0,
        };

        // Spend coinbase 2, make payment + change (change spk is shared)
        let spend2 = DummyTxData {
            id: TxId(3),
            spent_coins: vec![TxOutId::new(TxId(1), 0)],
            outputs: vec![
                DummyTxOutData::new(4_000, unique_spk2, 0, TxId(3)), // payment
                DummyTxOutData::new(5_000, shared_spk, 1, TxId(3)),  // change (shared with spend1)
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

    #[test]
    fn test_same_address_clustering() {
        let all_txs = setup_test_fixture();
        let ctx = Arc::new(PipelineContext::new());
        let mut engine = Engine::new(ctx.clone());
        engine.add_base_facts(all_txs);

        let source = AllLooseTxs::new(&ctx);
        let all_txs = source.txs();
        let index = source.index();
        let clustering = SameAddressClustering::new(all_txs, index);
        engine.run_to_fixpoint();
        let result = engine.eval(&clustering);

        // Same spk should be clustered together
        assert_eq!(
            result.find(TxOutId::new(TxId(2), 1)),
            result.find(TxOutId::new(TxId(3), 1))
        );

        // Other outputs should not be clustered together
        assert_ne!(
            result.find(TxOutId::new(TxId(2), 0)),
            result.find(TxOutId::new(TxId(3), 0))
        );
    }
}
