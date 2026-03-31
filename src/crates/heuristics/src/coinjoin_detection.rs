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

    use tx_indexer_primitives::test_utils::DummyTxData;

    use super::*;

    #[test]
    fn test_is_coinjoin_tx() {
        let not_coinjoin = DummyTxData::new_with_amounts(vec![100, 200, 300]);
        assert!(!NaiveCoinjoinDetection::is_coinjoin(&not_coinjoin));

        let coinjoin =
            DummyTxData::new_with_amounts(vec![100, 100, 100, 200, 200, 200, 300, 300, 300]);
        assert!(NaiveCoinjoinDetection::is_coinjoin(&coinjoin));
    }
}
