use pipeline::Clustering;
use pipeline::expr::Expr;
use pipeline::node::Node;
use pipeline::value::TxSet;
use tx_indexer_primitives::abstract_id::AbstractTxOutId;
use tx_indexer_primitives::disjoint_set::{DisJointSet, SparseDisjointSet};
use tx_indexer_primitives::graph_index::ScriptPubkeyIndex;

pub struct SameAddressClusteringNode {
    txs: Expr<TxSet>,
}

impl SameAddressClusteringNode {
    pub fn new(txs: Expr<TxSet>) -> Self {
        Self { txs }
    }
}

impl Node for SameAddressClusteringNode {
    type OutputValue = Clustering;

    fn dependencies(&self) -> Vec<pipeline::NodeId> {
        vec![self.txs.id()]
    }

    fn evaluate(&self, ctx: &pipeline::EvalContext) -> SparseDisjointSet<AbstractTxOutId> {
        let txs = ctx.get(&self.txs);
        let index = ctx.index();
        let clustering = SparseDisjointSet::new();

        for tx_id in txs.iter() {
            if let Some(concrete_id) = tx_id.try_as_loose() {
                for output in concrete_id.with(index).outputs() {
                    let txout_id = AbstractTxOutId::from(output.id());
                    if let Some(first_txout) =
                        index.script_pubkey_to_txout_id(&output.script_pubkey_hash())
                    {
                        clustering.union(txout_id, first_txout.into());
                    }
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
    pub fn new(txs: Expr<TxSet>) -> Expr<Clustering> {
        let ctx = txs.context().clone();
        ctx.register(SameAddressClusteringNode::new(txs))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, RwLock};

    use pipeline::ops::AllTxs;
    use pipeline::{Engine, PipelineContext};
    use tx_indexer_primitives::abstract_types::AbstractTransaction;
    use tx_indexer_primitives::loose::storage::InMemoryIndex;
    use tx_indexer_primitives::loose::{TxId, TxInId, TxOutId};
    use tx_indexer_primitives::test_utils::{DummyTxData, DummyTxOutData};

    use super::*;

    fn setup_test_fixture() -> Vec<
        Arc<dyn AbstractTransaction<TxId = TxId, TxOutId = TxOutId, TxInId = TxInId> + Send + Sync>,
    > {
        // Fixture: two coinbase txs, spent by two txs; two outputs each, both change outputs share same spk
        let shared_spk = [42u8; 20];
        let unique_spk1 = [1u8; 20];
        let unique_spk2 = [2u8; 20];

        // Coinbase 1
        let coinbase1 = DummyTxData {
            id: TxId(0),
            outputs: vec![DummyTxOutData::new(10_000, unique_spk1)],
            spent_coins: vec![],
            n_locktime: 0,
        };

        // Coinbase 2
        let coinbase2 = DummyTxData {
            id: TxId(1),
            outputs: vec![DummyTxOutData::new(10_000, unique_spk2)],
            spent_coins: vec![],
            n_locktime: 0,
        };

        // Spend coinbase 1, make payment + change (change spk is shared)
        let spend1 = DummyTxData {
            id: TxId(2),
            spent_coins: vec![TxOutId::new(TxId(0), 0)],
            outputs: vec![
                DummyTxOutData::new(4_000, unique_spk1), // payment
                DummyTxOutData::new(5_000, shared_spk),  // change (shared with spend2)
            ],
            n_locktime: 0,
        };

        // Spend coinbase 2, make payment + change (change spk is shared)
        let spend2 = DummyTxData {
            id: TxId(3),
            spent_coins: vec![TxOutId::new(TxId(1), 0)],
            outputs: vec![
                DummyTxOutData::new(4_000, unique_spk2), // payment
                DummyTxOutData::new(5_000, shared_spk),  // change (shared with spend1)
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
        let mut engine = Engine::new(ctx.clone(), Arc::new(RwLock::new(InMemoryIndex::new())));
        engine.add_base_facts(all_txs);

        let all_txs = AllTxs::new(&ctx);
        let clustering = SameAddressClustering::new(all_txs);
        engine.run_to_fixpoint();
        let result = engine.eval(&clustering);

        // Same spk should be clustered together
        assert_eq!(
            result.find(AbstractTxOutId::from(TxOutId::new(TxId(2), 1))),
            result.find(AbstractTxOutId::from(TxOutId::new(TxId(3), 1)))
        );

        // Other outputs should not be clustered together
        assert_ne!(
            result.find(AbstractTxOutId::from(TxOutId::new(TxId(2), 0))),
            result.find(AbstractTxOutId::from(TxOutId::new(TxId(3), 0)))
        );
    }
}
