use bitcoin::Amount;
use tx_indexer_primitives::{
    AbstractTransaction,
    handle::SpendableTxConstituent,
    traits::abstract_types::{
        EnumerateInputValueInArbitraryOrder, EnumerateOutputValueInArbitraryOrder, TxConstituent,
    },
};

pub struct UnnecessaryInputHeuristic;

impl UnnecessaryInputHeuristic {
    /// Returns the minimum output value that is smaller than the minimum input value.
    pub fn is_uih1_candidate<T>(txout: SpendableTxConstituent<T>) -> bool
    where
        T: TxConstituent<
            Handle: EnumerateInputValueInArbitraryOrder + EnumerateOutputValueInArbitraryOrder,
        >,
    {
        let tx = txout.containing_tx();
        let min_in = tx.input_values().min();
        let output_val = tx
            .output_at(txout.vout())
            .expect("vout should be present")
            .value();

        if let Some(min_in) = min_in
            && output_val < min_in
        {
            return true;
        }

        false
    }

    pub fn is_uih2<T>(tx: &T) -> bool
    where
        T: EnumerateInputValueInArbitraryOrder + EnumerateOutputValueInArbitraryOrder,
    {
        let input_values: Vec<Amount> = tx.input_values().collect();
        let output_values: Vec<Amount> = tx.output_values().collect();

        if input_values.len() < 2 || output_values.is_empty() {
            return false;
        }

        let sum_in = input_values.iter().fold(Amount::from_sat(0), |a, b| a + *b);
        let min_in = input_values.iter().min().copied().expect("len >= 2");
        let sum_out = output_values
            .iter()
            .fold(Amount::from_sat(0), |a, b| a + *b);
        let min_out = output_values.iter().min().copied().expect("non-empty");

        (sum_in - min_in) >= (sum_out - min_out)
    }
}
