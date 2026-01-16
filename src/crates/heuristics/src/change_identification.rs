use tx_indexer_primitives::{
    abstract_types::{AbstractTxHandle, OutputCount, TxConstituent},
    loose::TxOutId,
};

use crate::MutableOperation;

pub struct NaiveChangeIdentificationHueristic;

impl NaiveChangeIdentificationHueristic {
    pub fn temp_is_change(&self, txout: impl TxConstituent<Handle: OutputCount>) -> bool {
        let constituent_tx = txout.containing_tx();
        constituent_tx.output_count() - 1 == txout.index()
    }
    pub fn is_change(&self, txout: impl TxConstituent<Handle: OutputCount>) -> MutableOperation {
        let constituent_tx = txout.containing_tx();
        MutableOperation::AnnotateChange(
            TxOutId {
                txid: constituent_tx.id(),
                vout: txout.index() as u32,
            },
            constituent_tx.output_count() - 1 == txout.index(),
        )
        // TODO: instead of the naive heuristic, simulate a strawman version of wallet fingerprint detection by looking at the spending tx txin
    }
}

// TODO
// pub struct FingerprintChangeIdentificationHueristic;

// impl FingerprintChangeIdentificationHueristic {
//     pub fn is_change(
//         &self,
//         txout: impl TxConstituent<Handle: FingerprintVector>,
//     ) -> MutableOperation {
//         let constituent_tx = txout.containing_tx();
//         todo!("Get teh fingerprint vector and implement it")

//         // TODO: instead of the naive heuristic, simulate a strawman version of wallet fingerprint detection by looking at the spending tx txin
//     }
// }
#[cfg(test)]
mod tests {
    use tx_indexer_primitives::{
        abstract_types::AbstractTxHandle,
        disjoint_set::DisJointSet,
        loose::{TxId, TxOutId},
        test_utils::{DummyIndex, DummyTxData, DummyTxOut},
    };

    use crate::{
        OperationExecutor, coinjoin_detection::NaiveCoinjoinDetection,
        common_input::MultiInputHeuristic,
    };

    use super::*;

    #[test]
    fn test_classify_change() {
        let heuristic = NaiveChangeIdentificationHueristic;
        let txout = DummyTxOut {
            index: 0,
            containing_tx: DummyTxData {
                id: TxId(1),
                outputs_amounts: vec![100],
                spent_coins: vec![],
            },
        };
        assert_eq!(
            heuristic.is_change(txout),
            MutableOperation::AnnotateChange(
                TxOutId {
                    txid: TxId(1),
                    vout: 0
                },
                true
            )
        );
    }

    #[test]
    fn test_heuritic_pipeline() {
        // This test sets up two coinbases with many outputs, a spending transaction that spends from both coinbases and create a change and payment transaction.
        // The change should be clustered together with the coinbase txouts. The order of operation should not matter.
        // We should also avoid cluster collapse by checking for coinjoins
        let change_identification = NaiveChangeIdentificationHueristic;
        let coinjoin_detection = NaiveCoinjoinDetection::default();
        let multi_input_heuristic = MultiInputHeuristic;

        let coinbase1 = DummyTxData {
            id: TxId(0),
            outputs_amounts: vec![100, 200, 300],
            spent_coins: vec![],
        };
        let coinbase2 = DummyTxData {
            id: TxId(1),
            outputs_amounts: vec![400, 500, 600],
            spent_coins: vec![],
        };

        let spending_tx = DummyTxData {
            id: TxId(2),
            outputs_amounts: vec![
                200, // payment output
                100, // Change outputs
            ],
            spent_coins: vec![
                TxOutId {
                    txid: coinbase1.id(),
                    vout: 0,
                },
                TxOutId {
                    txid: coinbase2.id(),
                    vout: 0,
                },
            ],
        };
        let payment_tx = DummyTxData {
            id: TxId(3),
            outputs_amounts: vec![100],
            spent_coins: vec![TxOutId {
                txid: spending_tx.id(),
                vout: 0,
            }],
        };

        let change_tx = DummyTxData {
            id: TxId(4),
            outputs_amounts: vec![200],
            spent_coins: vec![TxOutId {
                txid: spending_tx.id(),
                vout: 1,
            }],
        };

        let mut index = DummyIndex::default();

        let all_txs = vec![
            &coinbase1,
            &coinbase2,
            &spending_tx,
            &payment_tx,
            &change_tx,
        ];
        // Run All txs through coinjoin classification
        for tx in all_txs {
            index.execute(&coinjoin_detection.is_coinjoin_tx(&tx.clone()));
        }

        // Cluster spending tx outputs
        for op in multi_input_heuristic.merge_prevouts(&spending_tx) {
            index.execute(&op);
        }
        // Ensure the coinbase outputs are clustered together
        let root = index.clustered_txouts.find(coinbase1.id().txout_id(0));
        assert_eq!(
            root,
            index.clustered_txouts.find(coinbase2.id().txout_id(0))
        );
        // And the spending txins are also int he same cluster
        assert_eq!(
            root,
            index.clustered_txouts.find(spending_tx.spent_coins[0])
        );
        assert_eq!(
            root,
            index.clustered_txouts.find(spending_tx.spent_coins[1])
        );

        // Gather change info
        for (i, _amount) in spending_tx.outputs_amounts.iter().enumerate() {
            let dummy_txout = DummyTxOut {
                index: i,
                containing_tx: spending_tx.clone(),
            };
            index.execute(&change_identification.is_change(dummy_txout));
        }

        // Now we shoudl cluster change txout with the spending txouts of the spending tx
        let change_txout_id = TxOutId {
            txid: spending_tx.id(),
            vout: 1,
        };
        if *index.change_tags.get(&change_txout_id).unwrap() {
            println!("Clustering change txout with spending txout");
            index.clustered_txouts.union(root, change_txout_id);
        }
        // Change txout should be clustered now with the spent coinbase txouts
        assert_eq!(
            index.clustered_txouts.find(root),
            index.clustered_txouts.find(change_txout_id)
        );
    }
}
