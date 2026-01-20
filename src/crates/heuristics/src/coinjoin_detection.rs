use std::{any::TypeId, collections::HashMap};

use tx_indexer_primitives::{
    abstract_types::EnumerateOutputValueInArbitraryOrder,
    datalog::{CursorBook, FactStore, IsCoinJoinRel, MemStore, Rule, TxRel},
    test_utils::DummyTxData,
};

use crate::MutableOperation;

/// This is a super naive implementation that should be replace with a more sophisticated one.
#[derive(Default, Debug)]
pub struct NaiveCoinjoinDetection;

impl NaiveCoinjoinDetection {
    pub fn is_coinjoin(&self, tx: &impl EnumerateOutputValueInArbitraryOrder) -> bool {
        // If there are >= 3 outputs of the same value, tag as coinjoin.
        // TODO: impl actual detection
        let mut counts = HashMap::new();
        for value in tx.output_values() {
            *counts.entry(value).or_insert(0) += 1;
        }

        counts.values().any(|&count| count >= 3)
    }

    // TODO: impl actual detection
    pub fn is_coinjoin_tx(
        &self,
        tx: &impl EnumerateOutputValueInArbitraryOrder,
    ) -> MutableOperation {
        MutableOperation::AnnotateTx(tx.id(), self.is_coinjoin(tx))
    }
}

pub fn coinjoin_detection_filter_pass_fn(tx: &impl EnumerateOutputValueInArbitraryOrder) -> bool {
    !NaiveCoinjoinDetection.is_coinjoin(tx)
}

pub struct CoinJoinRule;

impl Rule for CoinJoinRule {
    fn name(&self) -> &'static str {
        "coinjoin detection"
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

        for tx in &delta_txs {
            store.insert::<IsCoinJoinRel>((tx.id, NaiveCoinjoinDetection.is_coinjoin(tx)));
        }
        delta_txs.len()
    }
}

#[cfg(test)]
mod tests {

    use tx_indexer_primitives::{loose::TxId, test_utils::DummyTxData};

    use super::*;

    #[test]
    fn test_is_coinjoin_tx() {
        let coinjoin_detection = NaiveCoinjoinDetection::default();
        let not_coinjoin = DummyTxData {
            id: TxId(0),
            outputs_amounts: vec![100, 200, 300],
            spent_coins: vec![],
        };
        assert_eq!(
            coinjoin_detection.is_coinjoin_tx(&not_coinjoin),
            MutableOperation::AnnotateTx(TxId(0), false)
        );

        let coinjoin = DummyTxData {
            id: TxId(1),
            outputs_amounts: vec![100, 100, 100, 200, 200, 200, 300, 300, 300],
            spent_coins: vec![],
        };
        assert_eq!(
            coinjoin_detection.is_coinjoin_tx(&coinjoin),
            MutableOperation::AnnotateTx(TxId(1), true)
        );
    }
}
