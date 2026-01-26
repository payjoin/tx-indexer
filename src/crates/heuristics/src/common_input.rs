use std::any::TypeId;

use tx_indexer_primitives::{
    abstract_types::EnumerateSpentTxOuts,
    datalog::{ClusterRel, CursorBook, IsCoinJoinRel, Rule, TxRel},
    disjoint_set::{DisJointSet, SparseDisjointSet},
    loose::{TxId, TxOutId},
    storage::{FactStore, MemStore},
};

pub struct MultiInputHeuristic;

// TODO: trait definition for heuristics?
impl MultiInputHeuristic {
    pub fn merge_prevouts(&self, tx: &impl EnumerateSpentTxOuts) -> SparseDisjointSet<TxOutId> {
        if tx.spent_coins().count() == 0 {
            return SparseDisjointSet::new();
        }
        let set = SparseDisjointSet::new();
        tx.spent_coins().reduce(|a, b| {
            set.union(a, b);
            a
        });
        set
    }
}

pub struct MihRule;

impl Rule for MihRule {
    fn name(&self) -> &'static str {
        "mih"
    }

    fn inputs(&self) -> &'static [TypeId] {
        // depends on Tx deltas; also reads IsCoinJoin for gating
        const INS: &[TypeId] = &[TypeId::of::<TxRel>(), TypeId::of::<IsCoinJoinRel>()];
        INS
    }

    fn step(&mut self, rid: usize, store: &mut MemStore, cursors: &mut CursorBook) -> usize {
        let delta_tx_ids: Vec<TxId> = cursors.read_delta::<TxRel>(rid, store);
        if delta_tx_ids.is_empty() {
            return 0;
        }

        let mut out = 0;
        for tx_id in delta_tx_ids {
            // gate: skip coinjoins
            if store.contains::<IsCoinJoinRel>(&(tx_id, true)) {
                continue;
            }

            let tx_handle = tx_id.with(store.index());
            let to_merge = MultiInputHeuristic.merge_prevouts(&tx_handle);
            if store.insert::<ClusterRel>(to_merge) {
                out += 1;
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use tx_indexer_primitives::{
        datalog::{
            AbstractTxWrapper, ClusterRel, CursorBook, GlobalClusteringRel, IsCoinJoinRel,
            RawTxRel, Rule, TxRel,
        },
        disjoint_set::{DisJointSet, SparseDisjointSet},
        loose::{TxId, TxOutId},
        storage::{FactStore, InMemoryIndex, MemStore},
        test_utils::DummyTxData,
    };

    use crate::GlobalClustering;

    use super::{MihRule, MultiInputHeuristic};

    #[test]
    fn test_multi_input_heuristic_merge_prevouts() {
        let tx = DummyTxData {
            id: TxId(100),
            outputs_amounts: vec![500, 300],
            spent_coins: vec![
                TxOutId::new(TxId(1), 0),
                TxOutId::new(TxId(2), 1),
                TxOutId::new(TxId(3), 0),
            ],
        };

        let heuristic = MultiInputHeuristic;
        let cluster = heuristic.merge_prevouts(&tx);

        // All three inputs should be in the same cluster
        let input1 = TxOutId::new(TxId(1), 0);
        let input2 = TxOutId::new(TxId(2), 1);
        let input3 = TxOutId::new(TxId(3), 0);

        assert_eq!(cluster.find(input1), cluster.find(input2));
        assert_eq!(cluster.find(input2), cluster.find(input3));
        assert_eq!(cluster.find(input1), cluster.find(input3));
    }

    #[test]
    fn test_multi_input_rule_step() {
        let tx = DummyTxData {
            id: TxId(200),
            outputs_amounts: vec![1000],
            spent_coins: vec![TxOutId::new(TxId(10), 0), TxOutId::new(TxId(11), 0)],
        };

        let mut store = MemStore::new(InMemoryIndex::new());
        store.initialize::<RawTxRel>();
        store.initialize::<TxRel>();
        store.initialize::<IsCoinJoinRel>();
        store.initialize::<ClusterRel>();

        let tx_wrapper = AbstractTxWrapper::new(tx.clone().into());
        store.insert::<RawTxRel>(tx_wrapper);

        // Run TransactionIngestionRule to add to index and emit TxId
        let mut ingestion_rule = crate::TransactionIngestionRule;
        let mut cursors = CursorBook::new();
        ingestion_rule.step(0, &mut store, &mut cursors);

        let mut rule = MihRule;
        let mut cursors = CursorBook::new();
        let rid = 0;

        // First step should process the transaction
        let count = rule.step(rid, &mut store, &mut cursors);
        assert_eq!(count, 1);

        let clusters: Vec<SparseDisjointSet<TxOutId>> =
            store.read_range::<ClusterRel>(0, store.len::<ClusterRel>());
        assert_eq!(clusters.len(), 1);

        let cluster = &clusters[0];
        let input1 = TxOutId::new(TxId(10), 0);
        let input2 = TxOutId::new(TxId(11), 0);
        assert_eq!(cluster.find(input1), cluster.find(input2));
    }

    #[test]
    fn test_multi_input_rule_skips_coinjoin() {
        // Create a coinjoin transaction with multiple inputs
        let coinjoin_tx = DummyTxData {
            id: TxId(300),
            outputs_amounts: vec![500, 500, 500],
            spent_coins: vec![
                TxOutId::new(TxId(20), 0),
                TxOutId::new(TxId(21), 0),
                TxOutId::new(TxId(22), 0),
            ],
        };

        let mut store = MemStore::new(InMemoryIndex::new());
        store.initialize::<RawTxRel>();
        store.initialize::<TxRel>();
        store.initialize::<IsCoinJoinRel>();
        store.initialize::<ClusterRel>();

        store.insert::<IsCoinJoinRel>((coinjoin_tx.id, true));
        let tx_wrapper = AbstractTxWrapper::new(coinjoin_tx.clone().into());
        store.insert::<RawTxRel>(tx_wrapper);

        // Run TransactionIngestionRule to add to index and emit TxId
        let mut ingestion_rule = crate::TransactionIngestionRule;
        let mut cursors = CursorBook::new();
        ingestion_rule.step(0, &mut store, &mut cursors);

        let mut rule = MihRule;
        let mut cursors = CursorBook::new();
        let rid = 0;

        // Should process but skip the coinjoin
        let count = rule.step(rid, &mut store, &mut cursors);
        assert_eq!(count, 0);

        // No cluster should be created for coinjoin
        let clusters: Vec<SparseDisjointSet<TxOutId>> =
            store.read_range::<ClusterRel>(0, store.len::<ClusterRel>());
        assert_eq!(clusters.len(), 0);
    }

    #[test]
    fn test_multi_input_rule_single_input() {
        // Transaction with only one input should create an empty cluster
        let tx = DummyTxData {
            id: TxId(400),
            outputs_amounts: vec![100],
            spent_coins: vec![TxOutId::new(TxId(30), 0)],
        };

        let heuristic = MultiInputHeuristic;
        let cluster = heuristic.merge_prevouts(&tx);

        let input = TxOutId::new(TxId(30), 0);
        assert_eq!(cluster.find(input), input);
    }

    #[test]
    fn test_multi_input_rule_no_inputs() {
        // Coinbase transaction with no inputs
        let coinbase = DummyTxData {
            id: TxId(500),
            outputs_amounts: vec![50000000],
            spent_coins: vec![],
        };

        let heuristic = MultiInputHeuristic;
        let cluster = heuristic.merge_prevouts(&coinbase);

        assert_eq!(
            cluster.find(TxOutId::new(TxId(999), 0)),
            TxOutId::new(TxId(999), 0)
        );
    }

    #[test]
    fn test_global_clustering() {
        let tx = DummyTxData {
            id: TxId(200),
            outputs_amounts: vec![1000],
            spent_coins: vec![TxOutId::new(TxId(0), 0), TxOutId::new(TxId(1), 0)],
        };

        let mut store = MemStore::new(InMemoryIndex::new());
        store.initialize::<RawTxRel>();
        store.initialize::<TxRel>();
        store.initialize::<IsCoinJoinRel>();
        store.initialize::<ClusterRel>();
        store.initialize::<GlobalClusteringRel>();

        let tx_wrapper = AbstractTxWrapper::new(tx.clone().into());
        store.insert::<RawTxRel>(tx_wrapper);

        // Run TransactionIngestionRule to add to index and emit TxId
        let mut ingestion_rule = crate::TransactionIngestionRule;
        let mut cursors = CursorBook::new();
        ingestion_rule.step(0, &mut store, &mut cursors);

        let mut mih_rule = MihRule;
        let mut global_clustering_rule = GlobalClustering;
        let mut cursors = CursorBook::new();
        let rid = 0;

        mih_rule.step(rid, &mut store, &mut cursors);
        global_clustering_rule.step(rid, &mut store, &mut cursors);

        let global_clustering = store.index().global_clustering.clone();
        assert_eq!(
            global_clustering.find(TxOutId::new(TxId(0), 0)),
            global_clustering.find(TxOutId::new(TxId(1), 0))
        );
    }
}
