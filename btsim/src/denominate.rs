//! Greedy is used instead of the dep's exact `radix_decompose` + `radix_sumset` because
//! building the full sumset at k=6 takes >200s; greedy is O(MAX_K · |denoms|) per call.
//! The dropped remainder (amount − sum) becomes fee because fee = inputs − outputs in the sim.

use std::sync::LazyLock;

use bitcoin::Amount;
use dense_subset_sum::{
    standard_denoms_in_range, DEFAULT_MAX_COMBINATION_SIZE, DEFAULT_MAX_DENOM_SATS,
    DEFAULT_MIN_DENOM_SATS,
};

const MAX_K: usize = DEFAULT_MAX_COMBINATION_SIZE; // 6

static DENOMS: LazyLock<Vec<u64>> = LazyLock::new(|| {
    let mut d = standard_denoms_in_range(DEFAULT_MIN_DENOM_SATS, DEFAULT_MAX_DENOM_SATS);
    d.sort_unstable_by(|a, b| b.cmp(a));
    d
});

pub(crate) fn denominate(amount: Amount) -> Vec<Amount> {
    let denoms = &*DENOMS;
    let mut remaining = amount.to_sat();
    let mut out = Vec::with_capacity(MAX_K);
    for _ in 0..MAX_K {
        match denoms.iter().copied().find(|&d| d <= remaining) {
            Some(d) => {
                out.push(Amount::from_sat(d));
                remaining -= d;
            }
            None => break,
        }
    }
    if out.is_empty() {
        vec![amount]
    } else {
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dense_subset_sum::is_standard_denom;

    #[test]
    fn denominable_amount_is_below_and_standard() {
        let amount = Amount::from_sat(1_234_567);
        let out = denominate(amount);
        let sum: u64 = out.iter().map(|a| a.to_sat()).sum();
        let smallest = *DENOMS.last().expect("denom set non-empty");
        let remainder = amount.to_sat() - sum;
        assert!(sum <= amount.to_sat(), "must approximate from below");
        assert!(out.len() <= MAX_K, "at most MAX_K denominations");
        // Greedy stops when nothing smaller fits OR the MAX_K cap is hit; it does not
        // guarantee remainder < dust when the cap is reached — the leftover is dropped to fee.
        assert!(
            remainder < smallest || out.len() == MAX_K,
            "remainder {remainder} must be < smallest denom ({smallest}) unless MAX_K reached"
        );
        for a in &out {
            assert!(
                is_standard_denom(a.to_sat()),
                "{} should be a standard denom",
                a.to_sat()
            );
        }
    }

    #[test]
    fn exact_denomination_returns_itself() {
        let out = denominate(Amount::from_sat(1000));
        let sum: u64 = out.iter().map(|a| a.to_sat()).sum();
        assert_eq!(sum, 1000);
        assert!(out.iter().all(|a| is_standard_denom(a.to_sat())));
    }

    #[test]
    fn below_smallest_denom_falls_back() {
        assert_eq!(
            denominate(Amount::from_sat(100)),
            vec![Amount::from_sat(100)]
        );
    }
}
