/// Decimal Hamming weight: count of non-zero base-10 digits in `n`.
pub fn decimal_hamming_weight(mut n: u64) -> u32 {
    let mut weight = 0;
    while n > 0 {
        if !n.is_multiple_of(10) {
            weight += 1;
        }
        n /= 10;
    }
    weight
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_has_weight_zero() {
        assert_eq!(decimal_hamming_weight(0), 0);
    }

    #[test]
    fn powers_of_ten_have_weight_one() {
        assert_eq!(decimal_hamming_weight(1), 1);
        assert_eq!(decimal_hamming_weight(10), 1);
        assert_eq!(decimal_hamming_weight(100), 1);
        assert_eq!(decimal_hamming_weight(100_000_000), 1);
    }

    #[test]
    fn round_ish_amounts() {
        // 1600 has two non-zero digits: 1, 6
        assert_eq!(decimal_hamming_weight(1600), 2);
        // 143_000 has three non-zero digits: 1, 4, 3
        assert_eq!(decimal_hamming_weight(143_000), 3);
        // 50_000_000 (0.5 BTC) has one non-zero digit
        assert_eq!(decimal_hamming_weight(50_000_000), 1);
    }

    #[test]
    fn high_precision_amounts() {
        // 34_567_891 has 8 non-zero digits
        assert_eq!(decimal_hamming_weight(34_567_891), 8);
        // 12_345_678 has 8 non-zero digits
        assert_eq!(decimal_hamming_weight(12_345_678), 8);
    }

    #[test]
    fn interior_zeros_dont_count() {
        // 10_001 has two non-zero digits
        assert_eq!(decimal_hamming_weight(10_001), 2);
        // 1_000_001 has two non-zero digits
        assert_eq!(decimal_hamming_weight(1_000_001), 2);
    }
}
