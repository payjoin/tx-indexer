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
        let min_in = tx.input_values().min()?;
        let min_out = tx.output_values().min()?;

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
        let mut input_count = 0usize;
        let mut sum_in = Amount::from_sat(0);
        let mut min_in = Amount::MAX_MONEY;
        for v in tx.input_values() {
            input_count += 1;
            sum_in += v;
            if v < min_in {
                min_in = v;
            }
        }
        if input_count < 2 {
            return false;
        }

        let mut has_output = false;
        let mut sum_out = Amount::from_sat(0);
        let mut min_out = Amount::MAX_MONEY;
        for v in tx.output_values() {
            has_output = true;
            sum_out += v;
            if v < min_out {
                min_out = v;
            }
        }
        if !has_output {
            return false;
        }

        (sum_in - min_in) >= (sum_out - min_out)
    }
}
