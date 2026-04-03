use std::collections::HashMap;

use tx_indexer_primitives::traits::abstract_types::EnumerateOutputValueInArbitraryOrder;
use tx_indexer_primitives::traits::{HasBlockHeight, HasNLockTime};

#[derive(Debug, PartialEq, Eq)]
// TODO: use this instead of bool
pub enum TxCoinjoinAnnotation {
    CoinJoin,
    NotCoinJoin,
}

/// This is a super naive implementation that should be replace with a more sophisticated one.
#[derive(Debug)]
pub struct NaiveCoinjoinDetection;

impl NaiveCoinjoinDetection {
    pub fn is_coinjoin(tx: &impl EnumerateOutputValueInArbitraryOrder) -> bool {
        // If there are >= 3 outputs of the same value, tag as coinjoin.
        // TODO: impl actual detection
        let mut counts = HashMap::new();
        for value in tx.output_values() {
            *counts.entry(value).or_insert(0) += 1;
        }

        counts.values().any(|&count| count >= 3)
    }
}

#[derive(Debug)]
pub struct JoinMarketDetection;

/// Pure arithmetic Gregorian calendar computation.
/// Based on Howard Hinnant's algorithm: https://howardhinnant.github.io/date_algorithms.html
///
/// Checks if `ts` is the unix timestamp of the first day of a month at midnight UTC,
/// within JoinMarket's valid timelock range.
///
/// JoinMarket (wallet.py) defines:
///   TIMELOCK_DAY_AND_SHORTER = (1, 0, 0, 0, 0)  # day=1, hour=0, min=0, sec=0
///   TIMENUMBER_COUNT = 80 * 12 = 960             # timenumbers 0..959
///   timenumber=0   → 2020-01-01 00:00:00 UTC = 1_577_836_800  (JOINMARKET_EPOCH)
///   timenumber=959 → 2099-12-01 00:00:00 UTC = 4_099_766_400  (JOINMARKET_ERA_END)
fn is_first_of_month_utc(ts: u32) -> bool {
    const JOINMARKET_EPOCH: u32 = 1_577_836_800;
    const JOINMARKET_ERA_END: u32 = 4_099_766_400;

    if !(JOINMARKET_EPOCH..=JOINMARKET_ERA_END).contains(&ts) {
        return false;
    }
    if !ts.is_multiple_of(86_400) {
        return false;
    }
    let days = ts as u64 / 86_400;
    let z = days as i64 + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    d == 1
}

impl JoinMarketDetection {
    /// Detects locktime as block height (without fidelity bond).
    ///
    /// JoinMarket (wallet.py) sets:
    ///   locktime = current_block_height
    ///   locktime = max(1, height - randint(0, 99))
    ///
    /// P2EP: locktime > 0
    /// Bitcoin: block height uses range [1, 499_999_999]
    pub fn without_fidelity_bond(tx: &(impl HasNLockTime + HasBlockHeight)) -> bool {
        let locktime = tx.n_locktime();

        if locktime == 0 {
            return false;
        }

        if locktime >= 500_000_000 {
            return false;
        }

        let locktime_active =
            tx.input_len() == 0 || tx.inputs().any(|input| input.sequence() < 0xffff_ffff);
        if !locktime_active {
            return false;
        }

        match tx.block_height() {
            Some(height) => {
                let min = (height as u32).saturating_sub(99);
                let max = height as u32;
                locktime >= min && locktime <= max
            }
            None => true,
        }
    }

    /// Detects fidelity bond locktime.
    ///
    /// JoinMarket (taker_utils.py) sets:
    ///   path_locktime = _time_number_to_timestamp(timenumber)
    ///                 → always first-of-month midnight UTC (multiple of 86400)
    ///   tx_locktime   = max(compute_tx_locktime(), path_locktime + 1)
    ///
    /// The +1 ensures tx_locktime > path_locktime (OP_CHECKLOCKTIMEVERIFY requires strict >).
    /// So the actual n_locktime on-chain is always first_of_month + 1 second.
    /// We detect this by checking if (locktime - 1) is a valid JoinMarket first-of-month timestamp.
    pub fn with_fidelity_bond(tx: &impl HasNLockTime) -> bool {
        let locktime = tx.n_locktime();
        if locktime == 0 {
            return false;
        }
        is_first_of_month_utc(locktime - 1)
    }
}

#[cfg(test)]
mod tests {
    use tx_indexer_primitives::loose::TxOutId;
    use tx_indexer_primitives::test_utils::{DummyTxData, DummyTxOutData};

    use super::*;

    fn dummy_txid() -> tx_indexer_primitives::loose::TxId {
        tx_indexer_primitives::loose::TxId::new(1)
    }

    #[test]
    fn test_is_coinjoin_tx() {
        let not_coinjoin = DummyTxData::new_with_amounts(vec![100, 200, 300]);
        assert!(!NaiveCoinjoinDetection::is_coinjoin(&not_coinjoin));

        let coinjoin =
            DummyTxData::new_with_amounts(vec![100, 100, 100, 200, 200, 200, 300, 300, 300]);
        assert!(NaiveCoinjoinDetection::is_coinjoin(&coinjoin));
    }

    #[test]
    fn test_without_fidelity_bond_confirmed_exact_height() {
        let tx =
            DummyTxData::new_confirmed(vec![DummyTxOutData::new(100, 0)], vec![], 800_000, 800_000);
        assert!(JoinMarketDetection::without_fidelity_bond(&tx));
    }

    #[test]
    fn test_without_fidelity_bond_confirmed_within_window() {
        let tx =
            DummyTxData::new_confirmed(vec![DummyTxOutData::new(100, 0)], vec![], 799_950, 800_000);
        assert!(JoinMarketDetection::without_fidelity_bond(&tx));
    }

    #[test]
    fn test_without_fidelity_bond_confirmed_edge_of_window() {
        let tx =
            DummyTxData::new_confirmed(vec![DummyTxOutData::new(100, 0)], vec![], 799_901, 800_000);
        assert!(JoinMarketDetection::without_fidelity_bond(&tx));
    }

    #[test]
    fn test_without_fidelity_bond_confirmed_outside_window() {
        let tx =
            DummyTxData::new_confirmed(vec![DummyTxOutData::new(100, 0)], vec![], 799_900, 800_000);
        assert!(!JoinMarketDetection::without_fidelity_bond(&tx));
    }

    #[test]
    fn test_without_fidelity_bond_confirmed_future_locktime() {
        let tx =
            DummyTxData::new_confirmed(vec![DummyTxOutData::new(100, 0)], vec![], 800_001, 800_000);
        assert!(!JoinMarketDetection::without_fidelity_bond(&tx));
    }

    #[test]
    fn test_without_fidelity_bond_confirmed_low_height_no_underflow() {
        let tx = DummyTxData::new_confirmed(vec![DummyTxOutData::new(100, 0)], vec![], 1, 50);
        assert!(JoinMarketDetection::without_fidelity_bond(&tx));
    }

    #[test]
    fn test_without_fidelity_bond_mempool_no_height() {
        let tx = DummyTxData::new(vec![DummyTxOutData::new(100, 0)], vec![], 800_000);
        assert!(JoinMarketDetection::without_fidelity_bond(&tx));
    }

    #[test]
    fn test_without_fidelity_bond_rejects_locktime_zero() {
        let tx = DummyTxData::new_confirmed(vec![DummyTxOutData::new(100, 0)], vec![], 0, 800_000);
        assert!(!JoinMarketDetection::without_fidelity_bond(&tx));
    }

    #[test]
    fn test_without_fidelity_bond_rejects_unix_timestamp() {
        let tx = DummyTxData::new_confirmed(
            vec![DummyTxOutData::new(100, 0)],
            vec![],
            1_698_796_800,
            800_000,
        );
        assert!(!JoinMarketDetection::without_fidelity_bond(&tx));
    }

    #[test]
    fn test_with_fidelity_bond() {
        // path_locktime = 2023-11-01 00:00:00 UTC = 1_698_796_800
        // tx_locktime = path_locktime + 1 (taker_utils.py)
        let tx = DummyTxData::new(vec![DummyTxOutData::new(100, 0)], vec![], 1_698_796_801);
        assert!(JoinMarketDetection::with_fidelity_bond(&tx));
        assert!(!JoinMarketDetection::without_fidelity_bond(&tx));
    }

    #[test]
    fn test_with_fidelity_bond_accepts_first_of_month_plus_one() {
        // path_locktime = 2024-01-01 00:00:00 UTC = 1_704_067_200
        // tx_locktime = path_locktime + 1 (taker_utils.py)
        let jan = DummyTxData::new(vec![DummyTxOutData::new(100, 0)], vec![], 1_704_067_201);
        assert!(JoinMarketDetection::with_fidelity_bond(&jan));
    }

    #[test]
    fn test_with_fidelity_bond_rejects_non_first_of_month() {
        // 2023-11-15 00:00:00 UTC + 1 — mid-month, not a valid path_locktime
        let mid = DummyTxData::new(vec![DummyTxOutData::new(100, 0)], vec![], 1_700_006_401);
        assert!(!JoinMarketDetection::with_fidelity_bond(&mid));
        // arbitrary timestamp + 1
        let arb = DummyTxData::new(vec![DummyTxOutData::new(100, 0)], vec![], 1_700_000_001);
        assert!(!JoinMarketDetection::with_fidelity_bond(&arb));
    }

    #[test]
    fn test_fidelity_bond_window_boundaries() {
        // timenumber=0 → 2020-01-01 00:00:00 UTC = 1_577_836_800, tx = path_locktime + 1
        let epoch = DummyTxData::new(vec![DummyTxOutData::new(100, 0)], vec![], 1_577_836_801);
        assert!(JoinMarketDetection::with_fidelity_bond(&epoch));
        // raw path_locktime without +1 → locktime - 1 falls before epoch, rejected
        let before = DummyTxData::new(vec![DummyTxOutData::new(100, 0)], vec![], 1_577_836_800);
        assert!(!JoinMarketDetection::with_fidelity_bond(&before));
        // timenumber=959 → 2099-12-01 00:00:00 UTC = 4_099_766_400, tx = path_locktime + 1
        let era_end = DummyTxData::new(vec![DummyTxOutData::new(100, 0)], vec![], 4_099_766_401);
        assert!(JoinMarketDetection::with_fidelity_bond(&era_end));
        // one second beyond era_end + 1 → rejected
        let beyond = DummyTxData::new(vec![DummyTxOutData::new(100, 0)], vec![], 4_099_766_402);
        assert!(!JoinMarketDetection::with_fidelity_bond(&beyond));
    }

    #[test]
    fn test_locktime_zero() {
        let tx = DummyTxData::new(vec![DummyTxOutData::new(100, 0)], vec![], 0);
        assert!(!JoinMarketDetection::without_fidelity_bond(&tx));
        assert!(!JoinMarketDetection::with_fidelity_bond(&tx));
    }

    #[test]
    fn test_locktime_boundary_max_height() {
        let tx = DummyTxData::new_confirmed(
            vec![DummyTxOutData::new(100, 0)],
            vec![],
            499_999_999,
            499_999_999,
        );
        assert!(JoinMarketDetection::without_fidelity_bond(&tx));
        assert!(!JoinMarketDetection::with_fidelity_bond(&tx));
    }

    #[test]
    fn test_without_fidelity_bond_rejects_all_sequence_max() {
        let tx = DummyTxData::new_with_sequences(
            vec![DummyTxOutData::new(100, 0)],
            vec![TxOutId::new(dummy_txid(), 0)],
            vec![0xffff_ffff],
            800_000,
            Some(800_000),
        );
        assert!(!JoinMarketDetection::without_fidelity_bond(&tx));
    }

    #[test]
    fn test_without_fidelity_bond_accepts_one_sequence_below_max() {
        let tx = DummyTxData::new_with_sequences(
            vec![DummyTxOutData::new(100, 0)],
            vec![TxOutId::new(dummy_txid(), 0)],
            vec![0xffff_fffe],
            800_000,
            Some(800_000),
        );
        assert!(JoinMarketDetection::without_fidelity_bond(&tx));
    }

    #[test]
    fn test_without_fidelity_bond_mixed_sequences_one_below_max() {
        let tx = DummyTxData::new_with_sequences(
            vec![DummyTxOutData::new(100, 0)],
            vec![TxOutId::new(dummy_txid(), 0), TxOutId::new(dummy_txid(), 1)],
            vec![0xffff_ffff, 0xffff_fffe],
            800_000,
            Some(800_000),
        );
        assert!(JoinMarketDetection::without_fidelity_bond(&tx));
    }
}
