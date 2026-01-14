use std::collections::HashMap;

use tx_indexer_primitives::abstract_types::EnumerateOutputValueInArbitraryOrder;

use crate::MutableOperation;

/// This is a super naive implementation that should be replace with a more sophisticated one.
#[derive(Default, Debug)]
pub struct NaiveCoinjoinDetection;

impl NaiveCoinjoinDetection {
    // TODO: impl actual detection
    pub fn is_coinjoin_tx(
        &self,
        tx: &impl EnumerateOutputValueInArbitraryOrder,
    ) -> MutableOperation {
        // If there are >= 3 outputs of the same value, tag as coinjoin.
        let mut counts = HashMap::new();
        for value in tx.output_values() {
            *counts.entry(value).or_insert(0) += 1;
        }

        MutableOperation::AnnotateTx(tx.id(), counts.values().any(|&count| count >= 3))
    }
}

#[cfg(test)]
mod tests {

    use tx_indexer_primitives::{loose::TxId, test_utils::DummyTxHandle};

    use super::*;

    #[test]
    fn test_is_coinjoin_tx() {
        let coinjoin_detection = NaiveCoinjoinDetection::default();
        let not_coinjoin = DummyTxHandle {
            id: TxId(0),
            outputs: vec![100, 200, 300],
            spent_coins: vec![],
        };
        assert_eq!(
            coinjoin_detection.is_coinjoin_tx(&not_coinjoin),
            MutableOperation::AnnotateTx(TxId(0), false)
        );

        let coinjoin = DummyTxHandle {
            id: TxId(1),
            outputs: vec![100, 100, 100, 200, 200, 200, 300, 300, 300],
            spent_coins: vec![],
        };
        assert_eq!(
            coinjoin_detection.is_coinjoin_tx(&coinjoin),
            MutableOperation::AnnotateTx(TxId(1), true)
        );
    }
}
