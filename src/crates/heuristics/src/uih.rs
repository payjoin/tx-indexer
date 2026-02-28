use bitcoin::Amount;
use tx_indexer_primitives::traits::abstract_types::{
    EnumerateInputValueInArbitraryOrder, EnumerateOutputValueInArbitraryOrder,
};

pub struct UnnecessaryInputHeuristic;

impl UnnecessaryInputHeuristic {
    pub fn uih1_min_output_value<T>(tx: &T) -> Option<Amount>
    where
        T: EnumerateInputValueInArbitraryOrder + EnumerateOutputValueInArbitraryOrder,
    {
        let input_values: Vec<Amount> = tx.input_values().collect();
        let output_values: Vec<Amount> = tx.output_values().collect();

        if input_values.is_empty() || output_values.is_empty() {
            return None;
        }

        let min_in = input_values
            .iter()
            .min()
            .copied()
            .expect("non-empty inputs");
        let min_out = output_values
            .iter()
            .min()
            .copied()
            .expect("non-empty outputs");

        if min_out < min_in {
            Some(min_out)
        } else {
            None
        }
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
