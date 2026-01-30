use std::collections::HashMap;

use tx_indexer_primitives::abstract_types::EnumerateOutputValueInArbitraryOrder;

#[derive(Debug, PartialEq, Eq)]
// TODO: use this instead of bool
pub enum TxCoinjoinAnnotation {
    CoinJoin,
    NotCoinJoin,
}

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
        assert!(!coinjoin_detection.is_coinjoin(&not_coinjoin));

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
        assert!(coinjoin_detection.is_coinjoin(&coinjoin));
    }
}
