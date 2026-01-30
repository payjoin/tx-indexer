use tx_indexer_primitives::abstract_types::{OutputCount, TxConstituent};

#[derive(Debug, PartialEq, Eq)]
pub enum TxOutChangeAnnotation {
    Change,
    NotChange,
}

pub struct NaiveChangeIdentificationHueristic;

impl NaiveChangeIdentificationHueristic {
    /// Check if a txout is change based on its containing transaction.
    pub fn is_change(txout: impl TxConstituent<Handle: OutputCount>) -> TxOutChangeAnnotation {
        let constituent_tx = txout.containing_tx();
        if Self::is_change_vout(&constituent_tx, txout.vout()) {
            TxOutChangeAnnotation::Change
        } else {
            TxOutChangeAnnotation::NotChange
        }
    }

    /// Check if an output at the given vout index is change.
    ///
    /// Uses naive heuristic: the last output of a transaction is assumed to be change.
    pub fn is_change_vout(tx: &impl OutputCount, vout: usize) -> bool {
        tx.output_count() > 0 && vout == tx.output_count() - 1
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
    fn test_classify_change() {
        let txout = tx_indexer_primitives::test_utils::DummyTxOut {
            vout: 0,
            containing_tx: DummyTxData {
                id: TxId(1),
                outputs: vec![DummyTxOutData::new_with_amount(100)],
                spent_coins: vec![],
            },
        };
        assert_eq!(
            NaiveChangeIdentificationHueristic::is_change(txout),
            TxOutChangeAnnotation::Change
        );
    }
}
