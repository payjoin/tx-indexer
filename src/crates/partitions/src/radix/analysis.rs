//! Naive radix-ness analysis.

use std::collections::BTreeMap;

use super::denoms::{
    P2WPKH_DUST_SATS, binary_denoms_in_range, decimal_denoms_in_range, ternary_denoms_in_range,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PerSeriesAnalysis {
    pub multiplicities: BTreeMap<u64, usize>,
}

/// Callers compose their own classification from the per-series data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RadixAnalysis {
    pub pow2: PerSeriesAnalysis,
    pub base3: PerSeriesAnalysis,
    pub base10: PerSeriesAnalysis,
}

pub fn analyze(inputs: &[u64], outputs: &[u64]) -> RadixAnalysis {
    let amounts: Vec<u64> = inputs.iter().chain(outputs).copied().collect();
    let max_amount = amounts.iter().copied().max().unwrap_or(0);
    debug_assert!(
        max_amount <= bitcoin::Amount::MAX_MONEY.to_sat(),
        "amount exceeds bitcoin supply ceiling"
    );

    let pow2_denoms = binary_denoms_in_range(*P2WPKH_DUST_SATS, max_amount);
    let base3_denoms = ternary_denoms_in_range(*P2WPKH_DUST_SATS, max_amount);
    let base10_denoms = decimal_denoms_in_range(*P2WPKH_DUST_SATS, max_amount);

    RadixAnalysis {
        pow2: count_multiplicities(&amounts, &pow2_denoms),
        base3: count_multiplicities(&amounts, &base3_denoms),
        base10: count_multiplicities(&amounts, &base10_denoms),
    }
}

fn count_multiplicities(amounts: &[u64], denoms: &[u64]) -> PerSeriesAnalysis {
    // denoms is sorted (BTreeSet output), so binary_search avoids HashSet alloc.
    let mut multiplicities: BTreeMap<u64, usize> = BTreeMap::new();
    for &a in amounts {
        if denoms.binary_search(&a).is_ok() {
            *multiplicities.entry(a).or_insert(0) += 1;
        }
    }
    PerSeriesAnalysis { multiplicities }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_inputs_and_outputs() {
        let a = analyze(&[], &[]);
        assert!(a.pow2.multiplicities.is_empty());
        assert!(a.base3.multiplicities.is_empty());
        assert!(a.base10.multiplicities.is_empty());
    }

    #[test]
    fn per_series_multiplicities_count_exact_denom_matches() {
        // 1000, 2000 → Base10; 1024 → Pow2; 1500 → none.
        let a = analyze(&[], &[1000, 1000, 2000, 1024, 1500]);
        assert_eq!(a.base10.multiplicities.get(&1000), Some(&2));
        assert_eq!(a.base10.multiplicities.get(&2000), Some(&1));
        assert!(!a.base10.multiplicities.contains_key(&1024));
        assert_eq!(a.pow2.multiplicities.get(&1024), Some(&1));
    }

    #[test]
    fn analyze_concatenates_inputs_and_outputs() {
        let a = analyze(&[50_000], &[100_000]);
        assert_eq!(a.base10.multiplicities.get(&50_000), Some(&1));
        assert_eq!(a.base10.multiplicities.get(&100_000), Some(&1));
    }
}
