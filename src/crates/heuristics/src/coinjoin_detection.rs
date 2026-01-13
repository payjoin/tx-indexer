use std::collections::HashMap;

use tx_indexer_primitives::{
    abstract_types::EnumerateOutputValueInArbitraryOrder,
    loose::{CoinJoinClassification, TxHandle},
};

/// This is a super naive implementation that should be replace with a more sophisticated one.
#[derive(Default, Debug)]
pub struct NaiveCoinjoinDetection;

impl NaiveCoinjoinDetection {
    fn classify_tx(
        &self,
        tx: &TxHandle,
        coinjoin_classification: &mut impl CoinJoinClassification,
    ) {
        coinjoin_classification.tag_tx(&tx.id(), self.is_coinjoin_tx(tx));
    }

    // TODO: impl actual detection
    fn is_coinjoin_tx(&self, tx: &impl EnumerateOutputValueInArbitraryOrder) -> bool {
        // If there are >= 3 outputs of the same value, tag as coinjoin.
        let mut counts = HashMap::new();
        for value in tx.output_values() {
            *counts.entry(value).or_insert(0) += 1;
        }

        counts.values().any(|&count| count >= 3)
    }
}

#[cfg(test)]
mod tests {
    use bitcoin::Amount;
    use tx_indexer_primitives::abstract_types::AbstractTxHandle;

    use super::*;

    struct DummyOutputsValues(Vec<u64>);

    impl AbstractTxHandle for DummyOutputsValues {}

    impl EnumerateOutputValueInArbitraryOrder for DummyOutputsValues {
        fn output_values(&self) -> impl Iterator<Item = Amount> {
            self.0.iter().map(|amount| Amount::from_sat(*amount))
        }
    }

    #[test]
    fn test_is_coinjoin_tx() {
        let coinjoin_detection = NaiveCoinjoinDetection::default();
        let not_coinjoin = DummyOutputsValues(vec![100, 200, 300]);
        assert!(!coinjoin_detection.is_coinjoin_tx(&not_coinjoin));

        let coinjoin = DummyOutputsValues(vec![100, 100, 100, 200, 200, 200, 300, 300, 300]);
        assert!(coinjoin_detection.is_coinjoin_tx(&coinjoin));
    }
}
