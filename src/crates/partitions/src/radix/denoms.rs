//! Standard denomination series (Hamming-weight-1 values in bases 2, 3, 10).

use std::collections::BTreeSet;
use std::sync::LazyLock;

use bitcoin::ScriptBuf;
use bitcoin::WPubkeyHash;
use bitcoin::hashes::Hash;

// TODO: other script types have different dust thresholds; parameterize when we support them.
/// P2WPKH dust at the default 3 sat/vb relay feerate, from rust-bitcoin's
/// `minimal_non_dust()` (294 sats).
pub static P2WPKH_DUST_SATS: LazyLock<u64> = LazyLock::new(|| {
    ScriptBuf::new_p2wpkh(&WPubkeyHash::from_byte_array([0u8; 20]))
        .minimal_non_dust()
        .to_sat()
});

pub fn powers_in_range(b: u64, min: u64, max: u64) -> Vec<u64> {
    if b < 2 {
        return Vec::new();
    }
    std::iter::successors(Some(1u64), |&p| p.checked_mul(b))
        .skip_while(|&p| p < min)
        .take_while(|&p| p <= max)
        .collect()
}

pub fn multiples_in_range(values: &[u64], coefficients: &[u64], min: u64, max: u64) -> Vec<u64> {
    let mut s: BTreeSet<u64> = BTreeSet::new();
    for &v in values {
        for &c in coefficients {
            if let Some(cv) = v.checked_mul(c)
                && cv >= min
                && cv <= max
            {
                s.insert(cv);
            }
        }
    }
    s.into_iter().collect()
}

/// `{2^k}` series — powers of 2; `{1}` coefficient is identity.
pub fn binary_denoms_in_range(min: u64, max: u64) -> Vec<u64> {
    powers_in_range(2, min, max)
}

/// `{1, 2}·3^k` series — 2 multiples per power of 3.
pub fn ternary_denoms_in_range(min: u64, max: u64) -> Vec<u64> {
    multiples_in_range(&powers_in_range(3, min, max), &[1, 2], min, max)
}

/// `{1, 2, 5}·10^k` series — 3 multiples per power of 10.
pub fn decimal_denoms_in_range(min: u64, max: u64) -> Vec<u64> {
    multiples_in_range(&powers_in_range(10, min, max), &[1, 2, 5], min, max)
}

/// Combined set.
pub fn standard_denoms_in_range(min: u64, max: u64) -> Vec<u64> {
    if max == 0 || max < min {
        return Vec::new();
    }
    let mut s: BTreeSet<u64> = BTreeSet::new();
    s.extend(binary_denoms_in_range(min, max));
    s.extend(ternary_denoms_in_range(min, max));
    s.extend(decimal_denoms_in_range(min, max));
    s.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn p2wpkh_dust_is_294() {
        assert_eq!(*P2WPKH_DUST_SATS, 294);
    }

    #[test]
    fn powers_in_range_b2_dust_to_1e8() {
        let p = powers_in_range(2, 294, 100_000_000);
        assert_eq!(p.first(), Some(&512));
        assert_eq!(p.last(), Some(&67_108_864));
    }

    #[test]
    fn powers_in_range_b_below_2_is_empty() {
        assert!(powers_in_range(0, 1, 100).is_empty());
        assert!(powers_in_range(1, 1, 100).is_empty());
    }

    #[test]
    fn multiples_in_range_sorted_and_deduplicated() {
        let m = multiples_in_range(&[2, 3, 6], &[1, 2, 3], 1, 100);
        assert_eq!(m, vec![2, 3, 4, 6, 9, 12, 18]);
    }

    #[test]
    fn binary_denoms_in_range_default_params() {
        let b = binary_denoms_in_range(294, 100_000_000);
        assert_eq!(b.first(), Some(&512));
        assert_eq!(b.last(), Some(&67_108_864));
    }

    #[test]
    fn ternary_denoms_in_range_default_params() {
        let t = ternary_denoms_in_range(294, 100_000_000);
        assert!(t.contains(&729));
        assert!(t.contains(&1458));
        // 3^5 = 243 < dust, so 2·3^5 = 486 is dropped.
        assert!(!t.contains(&486));
    }

    #[test]
    fn decimal_denoms_in_range_default_params() {
        let d = decimal_denoms_in_range(294, 100_000_000);
        assert!(d.contains(&1_000));
        assert!(d.contains(&5_000));
        assert!(!d.contains(&200_000_000));
    }

    #[test]
    fn standard_denoms_in_range_is_sorted_union_of_three_series() {
        let s = standard_denoms_in_range(294, 100_000_000);
        for w in s.windows(2) {
            assert!(w[0] < w[1]);
        }
        assert!(s.contains(&512));
        assert!(s.contains(&729));
        assert!(s.contains(&1_000));
        // 3·2^k is not in any series.
        assert!(!s.contains(&768));
    }

    #[test]
    fn standard_denoms_in_range_empty_when_max_below_min() {
        assert!(standard_denoms_in_range(1_000, 500).is_empty());
        assert!(standard_denoms_in_range(0, 0).is_empty());
    }
}
