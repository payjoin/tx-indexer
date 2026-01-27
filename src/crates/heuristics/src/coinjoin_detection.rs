use std::{any::TypeId, collections::HashMap};

use tx_indexer_primitives::{
    abstract_types::EnumerateOutputValueInArbitraryOrder,
    datalog::{IsCoinJoinRel, Rule, TransactionInput, TxRel},
    storage::{FactStore, MemStore},
};

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
}

pub fn coinjoin_detection_filter_pass_fn(tx: &impl EnumerateOutputValueInArbitraryOrder) -> bool {
    !NaiveCoinjoinDetection.is_coinjoin(tx)
}

pub struct CoinJoinRule;

impl Rule for CoinJoinRule {
    type Input = TransactionInput;

    fn name(&self) -> &'static str {
        "coinjoin detection"
    }

    fn inputs(&self) -> &'static [TypeId] {
        const INS: &[TypeId] = &[TypeId::of::<TxRel>()];
        INS
    }

    fn step(&mut self, input: Self::Input, store: &mut MemStore) -> usize {
        let mut count = 0;
        for tx_id in input.iter() {
            let tx_handle = tx_id.with(store.index());
            let is_coinjoin = NaiveCoinjoinDetection.is_coinjoin(&tx_handle);
            store.insert::<IsCoinJoinRel>((tx_id, is_coinjoin));
            count += 1;
        }
        count
    }
}

#[cfg(test)]
mod tests {

    use tx_indexer_primitives::{
        loose::TxId,
        test_utils::{DummyTxData, DummyTxOutData},
    };

    use super::*;

    #[test]
    fn test_is_coinjoin_tx() {
        let coinjoin_detection = NaiveCoinjoinDetection::default();
        let not_coinjoin = DummyTxData {
            id: TxId(0),
            outputs: vec![
                DummyTxOutData::new_with_amount(100),
                DummyTxOutData::new_with_amount(200),
                DummyTxOutData::new_with_amount(300),
            ],
            spent_coins: vec![],
        };
        assert_eq!(coinjoin_detection.is_coinjoin(&not_coinjoin), false);

        let coinjoin = DummyTxData {
            id: TxId(1),
            outputs: vec![
                DummyTxOutData::new_with_amount(100),
                DummyTxOutData::new_with_amount(100),
                DummyTxOutData::new_with_amount(100),
                DummyTxOutData::new_with_amount(200),
                DummyTxOutData::new_with_amount(200),
                DummyTxOutData::new_with_amount(200),
                DummyTxOutData::new_with_amount(300),
                DummyTxOutData::new_with_amount(300),
                DummyTxOutData::new_with_amount(300),
            ],
            spent_coins: vec![],
        };
        assert_eq!(coinjoin_detection.is_coinjoin(&coinjoin), true);
    }
}
