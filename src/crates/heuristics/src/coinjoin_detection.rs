use std::collections::HashMap;

use tx_indexer_primitives::traits::abstract_types::EnumerateOutputValueInArbitraryOrder;

#[derive(Debug, PartialEq, Eq)]
// TODO: use this instead of bool
pub enum TxCoinjoinAnnotation {
    CoinJoin,
    NotCoinJoin,
}

/// This is a super naive implementation that should be replace with a more sophisticated one.
#[derive(Debug)]
pub struct NaiveCoinjoinDetection;

impl NaiveCoinjoinDetection {
    pub fn is_coinjoin(tx: &impl EnumerateOutputValueInArbitraryOrder) -> bool {
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
        let not_coinjoin = DummyTxData {
            id: TxId(1),
            outputs: vec![
                DummyTxOutData::new_with_amount(100, 0, TxId(1)),
                DummyTxOutData::new_with_amount(200, 1, TxId(1)),
                DummyTxOutData::new_with_amount(300, 2, TxId(1)),
            ],
            spent_coins: vec![],
            n_locktime: 0,
        };
        assert!(!NaiveCoinjoinDetection::is_coinjoin(&not_coinjoin));

        let coinjoin = DummyTxData {
            id: TxId(2),
            outputs: vec![
                DummyTxOutData::new_with_amount(100, 0, TxId(2)),
                DummyTxOutData::new_with_amount(100, 1, TxId(2)),
                DummyTxOutData::new_with_amount(100, 2, TxId(2)),
                DummyTxOutData::new_with_amount(200, 3, TxId(2)),
                DummyTxOutData::new_with_amount(200, 4, TxId(2)),
                DummyTxOutData::new_with_amount(200, 5, TxId(2)),
                DummyTxOutData::new_with_amount(300, 6, TxId(2)),
                DummyTxOutData::new_with_amount(300, 7, TxId(2)),
                DummyTxOutData::new_with_amount(300, 8, TxId(2)),
            ],
            spent_coins: vec![],
            n_locktime: 0,
        };
        assert!(NaiveCoinjoinDetection::is_coinjoin(&coinjoin));
    }
}
