use std::collections::HashMap;

use tx_indexer_primitives::abstract_types::EnumerateOutputValueInArbitraryOrder;

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
