use tx_indexer_pipeline::{
    engine::EvalContext,
    expr::Expr,
    node::{Node, NodeId},
    value::{TxOutClustering, TxSet},
};

use tx_indexer_disjoint_set::{DisJointSet, SparseDisjointSet};
use tx_indexer_primitives::{AbstractTxOut, unified::AnyOutId};

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
            let tx = tx_id.with(ctx.unified_storage());
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
    use bitcoin_test_data::blocks::mainnet_702861;
    use std::fs;
    use tx_indexer_pipeline::{
        Engine, PipelineContext,
        ops::{AllDenseTxs, AllLooseTxs},
    };
    use tx_indexer_primitives::UnifiedStorageBuilder;
    use tx_indexer_primitives::dense::IndexPaths;
    use tx_indexer_primitives::integration::NodeHarness;
    use tx_indexer_primitives::loose::LooseIndexBuilder;
    use tx_indexer_primitives::sled::db::SledDBFactory;
    use tx_indexer_primitives::{
        loose::{TxId, TxOutId},
        test_utils::{DummyTxData, DummyTxOutData, temp_dir, write_single_block_file},
        traits::abstract_types::AbstractTransaction,
        unified::AnyOutId,
    };

    fn setup_test_fixture() -> Vec<Arc<dyn AbstractTransaction + Send + Sync>> {
        // Fixture: two coinbase txs, spent by two txs; two outputs each, both change outputs share same spk
        let shared_spk = [42u8; 20];
        let unique_spk1 = [1u8; 20];
        let unique_spk2 = [2u8; 20];

        // Coinbase 1
        let coinbase1 = DummyTxData {
            outputs: vec![DummyTxOutData::new(10_000, unique_spk1, 0)],
            spent_coins: vec![],
            n_locktime: 0,
        };

        // Coinbase 2
        let coinbase2 = DummyTxData {
            outputs: vec![DummyTxOutData::new(10_000, unique_spk2, 0)],
            spent_coins: vec![],
            n_locktime: 0,
        };

        // Spend coinbase 1, make payment + change (change spk is shared)
        let spend1 = DummyTxData {
            spent_coins: vec![TxOutId::new(TxId(1), 0)],
            outputs: vec![
                DummyTxOutData::new(4_000, unique_spk1, 0), // payment
                DummyTxOutData::new(5_000, shared_spk, 1),  // change (shared with spend2)
            ],
            n_locktime: 0,
        };

        // Spend coinbase 2, make payment + change (change spk is shared)
        let spend2 = DummyTxData {
            spent_coins: vec![TxOutId::new(TxId(2), 0)],
            outputs: vec![
                DummyTxOutData::new(4_000, unique_spk2, 0), // payment
                DummyTxOutData::new(5_000, shared_spk, 1),  // change (shared with spend1)
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
            .expect("build unified storage");
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

    #[test]
    fn test_dense_same_address_mainnet_block() -> anyhow::Result<()> {
        let harness = NodeHarness::new(None)?;
        let block_bytes = mainnet_702861();
        let blocks_dir = harness.blocks_dir.join("mainnet_702861");
        fs::create_dir_all(&blocks_dir)?;
        write_single_block_file(&blocks_dir, block_bytes)?;

        let index_dir = temp_dir("tx_indexer_dense_mainnet_idx");
        let paths = IndexPaths {
            txptr: index_dir.join("txptr.idx"),
            block_tx: index_dir.join("block_tx.idx"),
            in_prevout: index_dir.join("in_prevout.idx"),
            out_spent: index_dir.join("out_spent.idx"),
        };
        let spk_db = SledDBFactory::open(temp_dir("tx_indexer_dense_mainnet_spk"))?.spk_db()?;
        let unified = UnifiedStorageBuilder::new()
            .with_dense(tx_indexer_primitives::unified::DenseBuildSpec {
                blocks_dir: blocks_dir.clone(),
                range: 0..1,
                paths,
                spk_db,
            })
            .build()?;

        let ctx = Arc::new(PipelineContext::new());
        let mut engine = Engine::new(ctx.clone(), Arc::new(unified));

        let source = AllDenseTxs::new(&ctx);
        let clustering = SameAddressClustering::new(source.txs());

        engine.run_to_fixpoint();
        let result = engine.eval(&clustering);

        let mut cluster_sizes: Vec<usize> = result
            .iter_parent_ids()
            .map(|root| result.iter_set(root).count())
            .collect();
        cluster_sizes.sort_unstable_by(|a, b| b.cmp(a));

        println!(
            "same-address clustering: clusters={}, largest={:?}",
            cluster_sizes.len(),
            cluster_sizes.get(0)
        );

        Ok(())
    }
}
