use tx_indexer_primitives::{
    abstract_fingerprints::HasNLockTime,
    abstract_types::{OutputCount, TxConstituent},
};

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

pub struct NLockTimeChangeIdentification;

impl NLockTimeChangeIdentification {
    pub fn is_change(
        tx_out: impl TxConstituent<Handle: HasNLockTime>,
        spending_tx: impl HasNLockTime,
    ) -> TxOutChangeAnnotation {
        let containing_tx_n_locktime = tx_out.containing_tx().n_locktime();
        let child_tx_n_locktime = spending_tx.n_locktime();
        if containing_tx_n_locktime == 0 && child_tx_n_locktime == 0 {
            // Probably not change
            TxOutChangeAnnotation::NotChange
        } else if containing_tx_n_locktime > 0 && child_tx_n_locktime > 0 {
            // Probably change
            TxOutChangeAnnotation::Change
        } else {
            // Unknown
            TxOutChangeAnnotation::NotChange
        }
    }
}

#[cfg(test)]
mod tests {
    use tx_indexer_primitives::{
        loose::TxId,
        test_utils::{DummyTxData, DummyTxOut, DummyTxOutData},
    };

    use super::*;

    #[test]
    fn test_classify_change() {
        let txout = DummyTxOut {
            vout: 0,
            containing_tx: DummyTxData {
                id: TxId(1),
                outputs: vec![DummyTxOutData::new_with_amount(100)],
                spent_coins: vec![],
                n_locktime: 0,
            },
        };
        assert_eq!(
            NaiveChangeIdentificationHueristic::is_change(txout),
            TxOutChangeAnnotation::Change
        );
    }

    #[test]
    fn test_n_locktime_change_identification() {
        let tx_out = DummyTxOut {
            vout: 0,
            containing_tx: DummyTxData {
                id: TxId(1),
                outputs: vec![DummyTxOutData::new_with_amount(100)],
                spent_coins: vec![],
                n_locktime: 0,
            },
        };
        let spending_tx = DummyTxData {
            id: TxId(2),
            outputs: vec![DummyTxOutData::new_with_amount(100)],
            spent_coins: vec![],
            n_locktime: 0,
        };
        assert_eq!(
            NLockTimeChangeIdentification::is_change(tx_out, spending_tx),
            TxOutChangeAnnotation::NotChange
        );

        // Same lock time
        let tx_out = DummyTxOut {
            vout: 0,
            containing_tx: DummyTxData {
                id: TxId(1),
                outputs: vec![DummyTxOutData::new_with_amount(100)],
                spent_coins: vec![],
                n_locktime: 1,
            },
        };
        let spending_tx = DummyTxData {
            id: TxId(2),
            outputs: vec![DummyTxOutData::new_with_amount(100)],
            spent_coins: vec![],
            n_locktime: 1,
        };
        assert_eq!(
            NLockTimeChangeIdentification::is_change(tx_out, spending_tx),
            TxOutChangeAnnotation::Change
        );
    }
}
