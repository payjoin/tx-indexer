use std::{any::TypeId, collections::HashMap};

use tx_indexer_primitives::{
    abstract_types::{OutputCount, TxConstituent},
    datalog::{ChangeIdentificationRel, ClusterRel, CursorBook, Rule, TxRel},
    disjoint_set::{DisJointSet, SparseDisjointSet},
    loose::TxOutId,
    storage::{FactStore, MemStore},
    test_utils::{DummyTxData, DummyTxOut},
};

#[derive(Debug, PartialEq, Eq)]
pub enum ChangeIdentificationResult {
    Change,
    NotChange,
}

pub struct NaiveChangeIdentificationHueristic;

impl NaiveChangeIdentificationHueristic {
    pub fn is_change(txout: impl TxConstituent<Handle: OutputCount>) -> ChangeIdentificationResult {
        let constituent_tx = txout.containing_tx();
        if constituent_tx.output_count() - 1 == txout.index() {
            ChangeIdentificationResult::Change
        } else {
            ChangeIdentificationResult::NotChange
        }
    }
}

pub fn change_identification_map_pass_fn(tx: &DummyTxData) -> HashMap<TxOutId, bool> {
    todo!("Implement or remove this")
    // let mut map = HashMap::new();
    // for (i, _amount) in tx.outputs_amounts.iter().enumerate() {
    //     let txout_id = TxOutId::new(tx.id, i as u32);
    //     let txout = DummyTxOut {
    //         index: i,
    //         containing_tx: tx.clone(),
    //     };
    //     map.insert(
    //         txout_id,
    //         NaiveChangeIdentificationHueristic::is_change(txout),
    //     );
    // }
    // map
}

pub struct ChangeIdentificationRule;

impl Rule for ChangeIdentificationRule {
    fn name(&self) -> &'static str {
        "change_identification"
    }

    fn inputs(&self) -> &'static [TypeId] {
        const INS: &[TypeId] = &[TypeId::of::<TxRel>()];
        INS
    }

    fn step(&mut self, rid: usize, store: &mut MemStore, cursors: &mut CursorBook) -> usize {
        let delta_txs: Vec<DummyTxData> = cursors.read_delta::<TxRel>(rid, store);
        if delta_txs.is_empty() {
            return 0;
        }

        let mut out = 0;
        for tx in delta_txs {
            // gate: skip coinjoins (or change to score threshold if you store scores)

            for (i, _amount) in tx.outputs_amounts.iter().enumerate() {
                let txout_id = TxOutId::new(tx.id, i as u32);
                let txout = DummyTxOut {
                    index: i,
                    containing_tx: tx.clone(),
                };
                let is_change = NaiveChangeIdentificationHueristic::is_change(txout);
                if is_change == ChangeIdentificationResult::Change {
                    store.insert::<ChangeIdentificationRel>((txout_id, true));
                } else {
                    store.insert::<ChangeIdentificationRel>((txout_id, false));
                }
                out += 1;
            }
        }
        out
    }
}

pub struct ChangeIdentificationClusterRule;

impl Rule for ChangeIdentificationClusterRule {
    fn name(&self) -> &'static str {
        "change_identification_cluster"
    }

    fn inputs(&self) -> &'static [TypeId] {
        const INS: &[TypeId] = &[TypeId::of::<ChangeIdentificationRel>()];
        INS
    }

    fn step(&mut self, rid: usize, store: &mut MemStore, cursors: &mut CursorBook) -> usize {
        let delta_change_txouts: Vec<TxOutId> = cursors
            .read_delta::<ChangeIdentificationRel>(rid, store)
            .iter()
            .filter(|(_, is_change)| *is_change)
            .map(|(txout_id, _)| *txout_id)
            .collect();
        if delta_change_txouts.is_empty() {
            return 0;
        }

        let mut out = 0;
        for txout_id in delta_change_txouts {
            let index = store.index();
            let mut set = SparseDisjointSet::<TxOutId>::default();
            let txid = txout_id.txid;
            // gate: skip coinjoins (or change to score threshold if you store scores)
            // Assuming all the inputs are clustered together per naive MIH then we can pick the first input and union with that
            let tx = index
                .txs
                .get(&txid)
                .expect("Transaction should always exist");
            let tx_inputs = tx.inputs().collect::<Vec<_>>();
            if tx_inputs.is_empty() {
                continue;
            }
            let txin = tx_inputs[0].as_ref();
            set.union(txin.prev_txout_id(), txout_id);
            if store.insert::<ClusterRel>(set) {
                out += 1;
            }
        }
        out
    }
}

// TODO
// pub struct FingerprintChangeIdentificationHueristic;

// impl FingerprintChangeIdentificationHueristic {
//     pub fn is_change(
//         &self,
//         txout: impl TxConstituent<Handle: FingerprintVector>,
//     ) -> MutableOperation {
//         let constituent_tx = txout.containing_tx();
//         todo!("Get teh fingerprint vector and implement it")

//         // TODO: instead of the naive heuristic, simulate a strawman version of wallet fingerprint detection by looking at the spending tx txin
//     }
// }
#[cfg(test)]
mod tests {
    use tx_indexer_primitives::{
        loose::TxId,
        test_utils::{DummyTxData, DummyTxOut},
    };

    use super::*;

    #[test]
    fn test_classify_change() {
        let txout = DummyTxOut {
            index: 0,
            containing_tx: DummyTxData {
                id: TxId(1),
                outputs_amounts: vec![100],
                spent_coins: vec![],
            },
        };
        assert_eq!(
            NaiveChangeIdentificationHueristic::is_change(txout),
            ChangeIdentificationResult::Change
        );
    }
}
