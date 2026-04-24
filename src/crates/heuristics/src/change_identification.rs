use tx_indexer_primitives::{
    handle::TxHandle,
    output_type::OutputType,
    traits::abstract_types::{HasNLockTime, HasScriptPubkey, OutputCount, TxConstituent},
};

#[derive(Debug, PartialEq, Eq)]
pub enum TxOutChangeAnnotation {
    Change,
    NotChange,
}

pub struct NaiveChangeIdentificationHueristic;

impl NaiveChangeIdentificationHueristic {
    /// Check if a txout is change based on its containing transaction.
    /// OP_RETURN outputs are never considered change.
    pub fn is_change<T>(txout: T) -> TxOutChangeAnnotation
    where
        T: TxConstituent<Handle: OutputCount> + HasScriptPubkey,
    {
        if txout.is_op_return() {
            return TxOutChangeAnnotation::NotChange;
        }

        let tx = txout.containing_tx();
        if tx.output_count() > 0 && txout.vout() == tx.output_count() - 1 {
            TxOutChangeAnnotation::Change
        } else {
            TxOutChangeAnnotation::NotChange
        }
    }
}

pub struct NLockTimeChangeIdentification;

impl NLockTimeChangeIdentification {
    /// Check if a txout is change based on nLockTime comparison.
    /// OP_RETURN outputs are never considered change.
    pub fn is_change<T>(tx_out: T, spending_tx: impl HasNLockTime) -> TxOutChangeAnnotation
    where
        T: TxConstituent<Handle: HasNLockTime> + HasScriptPubkey,
    {
        if tx_out.is_op_return() {
            return TxOutChangeAnnotation::NotChange;
        }

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

pub struct ScriptTypesMatchingChangeIdentification;

impl ScriptTypesMatchingChangeIdentification {
    /// Classifies an output as change when its containing transaction has a
    /// unanimous input script type and this output is the only output matching
    /// that type.
    ///
    /// This applies the address-type heuristic conservatively: mixed input
    /// types, unresolved prevouts, or multiple matching outputs are all treated
    /// as inconclusive and return `NotChange`.
    ///
    /// OP_RETURN outputs are never considered change.
    pub fn is_change<'a, T>(tx_out: T) -> TxOutChangeAnnotation
    where
        T: TxConstituent<Handle = TxHandle<'a>> + HasScriptPubkey,
    {
        if tx_out.is_op_return() {
            return TxOutChangeAnnotation::NotChange;
        }

        let tx = tx_out.containing_tx();
        let mut input_types = tx.inputs().map(|input| input.output_type());

        let Some(input_type) = input_types.next().flatten() else {
            return TxOutChangeAnnotation::NotChange;
        };

        if input_types.any(|candidate_type| {
            let Some(candidate_type) = candidate_type else {
                return true;
            };
            candidate_type != input_type
        }) {
            return TxOutChangeAnnotation::NotChange;
        }

        let matching_outputs: Vec<usize> = tx
            .outputs()
            .enumerate()
            .filter_map(|(index, output)| {
                let out_type = output.output_type();
                // Skip OP_RETURN outputs when looking for change candidates
                (out_type == input_type && out_type != OutputType::OpReturn).then_some(index)
            })
            .collect();

        if matching_outputs.len() == 1 && matching_outputs[0] == tx_out.vout() {
            TxOutChangeAnnotation::Change
        } else {
            TxOutChangeAnnotation::NotChange
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use tx_indexer_primitives::{
        UnifiedStorage,
        loose::LooseIndexBuilder,
        loose::{TxId, TxOutId},
        test_utils::{DummyTxData, DummyTxOut, DummyTxOutData},
        unified::AnyOutId,
    };

    use super::*;

    fn storage_from_loose_txs(txs: Vec<DummyTxData>) -> UnifiedStorage {
        let mut builder = LooseIndexBuilder::new();
        for tx in txs {
            builder.add_tx(std::sync::Arc::new(tx));
        }
        UnifiedStorage::from(builder)
    }

    fn script_from_address(address: &str) -> Vec<u8> {
        bitcoin::Address::from_str(address)
            .unwrap()
            .require_network(bitcoin::Network::Bitcoin)
            .unwrap()
            .script_pubkey()
            .to_bytes()
    }

    #[test]
    fn test_classify_change() {
        let txout = DummyTxOut {
            vout: 0,
            containing_tx: DummyTxData::new_with_amounts(vec![100]),
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
            containing_tx: DummyTxData::new_with_amounts(vec![100]),
        };
        let spending_tx = DummyTxData::new_with_amounts(vec![100]);
        assert_eq!(
            NLockTimeChangeIdentification::is_change(tx_out, spending_tx),
            TxOutChangeAnnotation::NotChange
        );

        // Same lock time
        let tx_out = DummyTxOut {
            vout: 0,
            containing_tx: DummyTxData::new(vec![DummyTxOutData::new(100, 0)], vec![], 1),
        };
        let spending_tx = DummyTxData::new(vec![DummyTxOutData::new(100, 0)], vec![], 1);
        assert_eq!(
            NLockTimeChangeIdentification::is_change(tx_out, spending_tx),
            TxOutChangeAnnotation::Change
        );
    }

    #[test]
    fn test_script_types_matching_is_change() {
        let storage = storage_from_loose_txs(vec![
            // inputs: P2PKH
            DummyTxData::new_with_outputs(vec![DummyTxOutData::new_with_script(
                100,
                0,
                script_from_address("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"),
            )]),
            DummyTxData::new_with_outputs(vec![DummyTxOutData::new_with_script(
                150,
                0,
                script_from_address("1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2"),
            )]),
            DummyTxData::new(
                vec![
                    // payment output: P2TR (different type from inputs → not change)
                    DummyTxOutData::new_with_script(
                        120,
                        0,
                        script_from_address(
                            "bc1p5cyxnuxmeuwuvkwfem96lqzszd02n6xdcjrs20cac6yqjjwudpxqkedrcr",
                        ),
                    ),
                    // change output: P2PKH (matches input type, unique → change)
                    DummyTxOutData::new_with_script(
                        130,
                        1,
                        script_from_address("1BoatSLRHtKNngkdXEeobR76b53LETtpyT"),
                    ),
                ],
                vec![TxOutId::new(TxId(1), 0), TxOutId::new(TxId(2), 0)],
                0,
            ),
        ]);

        let payment = AnyOutId::from(TxOutId::new(TxId(3), 0)).with(&storage);
        let change = AnyOutId::from(TxOutId::new(TxId(3), 1)).with(&storage);

        assert_eq!(
            ScriptTypesMatchingChangeIdentification::is_change(payment),
            TxOutChangeAnnotation::NotChange
        );
        assert_eq!(
            ScriptTypesMatchingChangeIdentification::is_change(change),
            TxOutChangeAnnotation::Change
        );
    }

    #[test]
    fn test_script_types_matching_requires_unanimous_inputs() {
        let storage = storage_from_loose_txs(vec![
            // inputs: mixed (P2PKH + P2TR) — not unanimous, so no change can be identified
            DummyTxData::new_with_outputs(vec![DummyTxOutData::new_with_script(
                100,
                0,
                script_from_address("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"), // P2PKH
            )]),
            DummyTxData::new_with_outputs(vec![DummyTxOutData::new_with_script(
                150,
                0,
                script_from_address(
                    "bc1p5cyxnuxmeuwuvkwfem96lqzszd02n6xdcjrs20cac6yqjjwudpxqkedrcr",
                ), // P2TR
            )]),
            DummyTxData::new(
                vec![
                    DummyTxOutData::new_with_script(
                        120,
                        0,
                        script_from_address(
                            "bc1p5cyxnuxmeuwuvkwfem96lqzszd02n6xdcjrs20cac6yqjjwudpxqkedrcr",
                        ),
                    ), // P2TR
                    DummyTxOutData::new_with_script(
                        130,
                        1,
                        script_from_address("1BoatSLRHtKNngkdXEeobR76b53LETtpyT"), // P2PKH
                    ),
                ],
                vec![TxOutId::new(TxId(1), 0), TxOutId::new(TxId(2), 0)],
                0,
            ),
        ]);

        let payment = AnyOutId::from(TxOutId::new(TxId(3), 0)).with(&storage);
        let change = AnyOutId::from(TxOutId::new(TxId(3), 1)).with(&storage);

        assert_eq!(
            ScriptTypesMatchingChangeIdentification::is_change(payment),
            TxOutChangeAnnotation::NotChange
        );
        assert_eq!(
            ScriptTypesMatchingChangeIdentification::is_change(change),
            TxOutChangeAnnotation::NotChange
        );
    }

    #[test]
    fn test_script_types_matching_excludes_op_return() {
        let storage = storage_from_loose_txs(vec![
            // inputs: P2PKH
            DummyTxData::new_with_outputs(vec![DummyTxOutData::new_with_script(
                100,
                0,
                script_from_address("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"),
            )]),
            DummyTxData::new_with_outputs(vec![DummyTxOutData::new_with_script(
                150,
                0,
                script_from_address("1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2"),
            )]),
            DummyTxData::new(
                vec![
                    // OP_RETURN output - should never be considered change
                    DummyTxOutData::new_with_script(
                        0,
                        0,
                        vec![0x6a, 0x04, 0x48, 0x65, 0x6c, 0x6c], // OP_RETURN "Hell"
                    ),
                    // P2PKH output - should be considered change since it's the only spendable P2PKH
                    DummyTxOutData::new_with_script(
                        249,
                        1,
                        script_from_address("1BoatSLRHtKNngkdXEeobR76b53LETtpyT"),
                    ),
                ],
                vec![TxOutId::new(TxId(1), 0), TxOutId::new(TxId(2), 0)],
                0,
            ),
        ]);

        let op_return_output = AnyOutId::from(TxOutId::new(TxId(3), 0)).with(&storage);
        let p2pkh_output = AnyOutId::from(TxOutId::new(TxId(3), 1)).with(&storage);

        // OP_RETURN should never be classified as change
        assert_eq!(
            ScriptTypesMatchingChangeIdentification::is_change(op_return_output),
            TxOutChangeAnnotation::NotChange
        );

        // P2PKH output should be change since it's the only spendable output matching input type
        assert_eq!(
            ScriptTypesMatchingChangeIdentification::is_change(p2pkh_output),
            TxOutChangeAnnotation::Change
        );
    }

    #[test]
    fn test_all_heuristics_filter_op_return() {
        // Create an OP_RETURN output to test that all heuristics filter it out
        let op_return_script = vec![0x6a, 0x04, 0x48, 0x65, 0x6c, 0x6c]; // OP_RETURN "Hell"

        // Test NaiveChangeIdentificationHueristic with OP_RETURN as last output
        let txout_op_return = DummyTxOut {
            vout: 1, // Last output in a 2-output tx
            containing_tx: DummyTxData::new(
                vec![
                    DummyTxOutData::new_with_script(
                        100,
                        0,
                        script_from_address("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"),
                    ),
                    DummyTxOutData::new_with_script(0, 1, op_return_script.clone()),
                ],
                vec![],
                0,
            ),
        };
        // Even though it's the last output, OP_RETURN should not be change
        assert_eq!(
            NaiveChangeIdentificationHueristic::is_change(txout_op_return),
            TxOutChangeAnnotation::NotChange
        );

        // Test NLockTimeChangeIdentification with OP_RETURN
        let tx_out_op_return = DummyTxOut {
            vout: 0,
            containing_tx: DummyTxData::new(
                vec![DummyTxOutData::new_with_script(
                    0,
                    0,
                    op_return_script.clone(),
                )],
                vec![],
                1, // Non-zero locktime
            ),
        };
        let spending_tx = DummyTxData::new(vec![], vec![], 1); // Same locktime
        // Even with matching locktimes, OP_RETURN should not be change
        assert_eq!(
            NLockTimeChangeIdentification::is_change(tx_out_op_return, spending_tx),
            TxOutChangeAnnotation::NotChange
        );
    }

    #[test]
    fn test_script_types_matching_requires_unique_output_match() {
        let storage = storage_from_loose_txs(vec![
            // inputs: P2PKH
            DummyTxData::new_with_outputs(vec![DummyTxOutData::new_with_script(
                100,
                0,
                script_from_address("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"),
            )]),
            DummyTxData::new_with_outputs(vec![DummyTxOutData::new_with_script(
                150,
                0,
                script_from_address("1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2"),
            )]),
            DummyTxData::new(
                vec![
                    // two P2PKH outputs — ambiguous, neither qualifies as unique change
                    DummyTxOutData::new_with_script(
                        120,
                        0,
                        script_from_address("1BoatSLRHtKNngkdXEeobR76b53LETtpyT"),
                    ),
                    DummyTxOutData::new_with_script(
                        130,
                        1,
                        script_from_address("1Cdid9KFAaatwczBwBttQcwXYCpvK8h7FK"),
                    ),
                ],
                vec![TxOutId::new(TxId(1), 0), TxOutId::new(TxId(2), 0)],
                0,
            ),
        ]);

        let output0 = AnyOutId::from(TxOutId::new(TxId(3), 0)).with(&storage);
        let output1 = AnyOutId::from(TxOutId::new(TxId(3), 1)).with(&storage);

        assert_eq!(
            ScriptTypesMatchingChangeIdentification::is_change(output0),
            TxOutChangeAnnotation::NotChange
        );
        assert_eq!(
            ScriptTypesMatchingChangeIdentification::is_change(output1),
            TxOutChangeAnnotation::NotChange
        );
    }
}
