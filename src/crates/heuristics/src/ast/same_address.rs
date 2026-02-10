use pipeline::expr::Expr;
use pipeline::node::Node;
use pipeline::value::{Backend, Clustering, Index, TxSet};
use tx_indexer_primitives::disjoint_set::{DisJointSet, SparseDisjointSet};
use tx_indexer_primitives::graph_index::{IndexHandleFor, ScriptPubkeyIndex, WithIndex};

pub struct SameAddressClusteringNode<I: Backend> {
    txs: Expr<TxSet<I::TxId>>,
    index: Expr<Index<I>>,
}

impl<I: Backend> SameAddressClusteringNode<I>
where
    I::TxId: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
    I::TxOutId: Eq + std::hash::Hash + Copy + Send + Sync + 'static,
{
    pub fn new(txs: Expr<TxSet<I::TxId>>, index: Expr<Index<I>>) -> Self {
        Self { txs, index }
    }
}

impl<I: Backend> Node for SameAddressClusteringNode<I>
where
    I::TxId: Eq + std::hash::Hash + Clone + Send + Sync + 'static + WithIndex<I>,
    I::TxOutId: Eq + std::hash::Hash + Copy + Send + Sync + 'static,
{
    type OutputValue = Clustering<I::TxOutId>;

    fn dependencies(&self) -> Vec<pipeline::NodeId> {
        vec![self.txs.id(), self.index.id()]
    }

    fn evaluate(&self, ctx: &pipeline::EvalContext) -> SparseDisjointSet<I::TxOutId> {
        let txs = ctx.get(&self.txs);
        let index_handle = ctx.get(&self.index);

        index_handle.with_graph(|graph| {
            let mut clustering = SparseDisjointSet::new();

            for tx_id in txs.iter() {
                let tx = tx_id.with_index(graph);
                for output in tx.output_handles() {
                    let txout_id = output.id();
                    if let Some(first_txout) =
                        graph.script_pubkey_to_txout_id(&output.script_pubkey_hash())
                    {
                        clustering.union(txout_id, first_txout);
                    }
                }
            }

            clustering
        })
    }
}

#[allow(dead_code)]
pub struct SameAddressClustering;

impl SameAddressClustering {
    #[allow(dead_code)]
    pub fn new<I: Backend>(
        txs: Expr<TxSet<I::TxId>>,
        index: Expr<Index<I>>,
    ) -> Expr<Clustering<I::TxOutId>>
    where
        I::TxId: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
        I::TxOutId: Eq + std::hash::Hash + Copy + Send + Sync + 'static,
    {
        let ctx = txs.context().clone();
        ctx.register(SameAddressClusteringNode::new(txs, index))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use pipeline::ops::AllTxs;
    use pipeline::{Engine, PipelineContext};
    use tx_indexer_primitives::abstract_id::LooseIds;
    use tx_indexer_primitives::abstract_types::AbstractTransaction;
    use tx_indexer_primitives::loose::{TxId, TxOutId};
    use tx_indexer_primitives::test_utils::{DummyTxData, DummyTxOutData};

    use super::*;

    fn setup_test_fixture() -> Vec<
        Arc<dyn AbstractTransaction<Id = LooseIds> + Send + Sync>,
    > {
        let shared_spk = [42u8; 20];
        let unique_spk1 = [1u8; 20];
        let unique_spk2 = [2u8; 20];

        let coinbase1 = DummyTxData {
            id: TxId(0),
            outputs: vec![DummyTxOutData::new(10_000, unique_spk1)],
            spent_coins: vec![],
            n_locktime: 0,
        };

        let coinbase2 = DummyTxData {
            id: TxId(1),
            outputs: vec![DummyTxOutData::new(10_000, unique_spk2)],
            spent_coins: vec![],
            n_locktime: 0,
        };

        let spend1 = DummyTxData {
            id: TxId(2),
            spent_coins: vec![TxOutId::new(TxId(0), 0)],
            outputs: vec![
                DummyTxOutData::new(4_000, unique_spk1),
                DummyTxOutData::new(5_000, shared_spk),
            ],
            n_locktime: 0,
        };

        let spend2 = DummyTxData {
            id: TxId(3),
            spent_coins: vec![TxOutId::new(TxId(1), 0)],
            outputs: vec![
                DummyTxOutData::new(4_000, unique_spk2),
                DummyTxOutData::new(5_000, shared_spk),
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

        let all_txs_expr = AllTxs::new(&ctx);
        let txs = all_txs_expr.txs();
        let index = all_txs_expr.index();
        let clustering = SameAddressClustering::new(txs, index);
        engine.run_to_fixpoint();
        let result = engine.eval(&clustering);

        assert_eq!(
            result.find(TxOutId::new(TxId(2), 1)),
            result.find(TxOutId::new(TxId(3), 1))
        );

        assert_ne!(
            result.find(TxOutId::new(TxId(2), 0)),
            result.find(TxOutId::new(TxId(3), 0))
        );
    }
}
