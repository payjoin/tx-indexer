use bdk_coin_select::{
    metrics::LowestFee, Candidate, ChangePolicy, CoinSelector, DrainWeights, Target,
    TR_DUST_RELAY_MIN_VALUE,
};
use bitcoin::Amount;
use log::warn;

use crate::transaction::Outpoint;

pub(crate) struct CoinCandidate {
    pub(crate) outpoint: Outpoint,
    pub(crate) amount_sats: u64,
    pub(crate) weight_wu: u32,
    pub(crate) is_segwit: bool,
}

/// Long-term feerate for coin selection (10 sat/vb = 2.5 sat/wu).
pub(crate) fn long_term_feerate() -> bdk_coin_select::FeeRate {
    bdk_coin_select::FeeRate::from_sat_per_wu(2.5)
}

fn change_policy_for(target: Target) -> ChangePolicy {
    ChangePolicy::min_value_and_waste(
        DrainWeights::default(),
        TR_DUST_RELAY_MIN_VALUE,
        target.fee.rate,
        long_term_feerate(),
    )
}

fn bdk_candidates(candidates: &[CoinCandidate]) -> Vec<Candidate> {
    candidates
        .iter()
        .map(|c| Candidate {
            value: c.amount_sats,
            weight: c.weight_wu,
            input_count: 1,
            is_segwit: c.is_segwit,
        })
        .collect()
}

fn drain_to_change(drain: bdk_coin_select::Drain) -> Vec<Amount> {
    if drain.value > 0 {
        vec![Amount::from_sat(drain.value)]
    } else {
        vec![]
    }
}

/// Run BNB coin selection over candidates for the given target.
/// Falls back to greedy selection if BNB finds no solution.
/// Returns None if no selection can meet the target.
/// Returns (selected_inputs, change_outputs).
pub(crate) fn select_bnb(
    candidates: &[CoinCandidate],
    target: Target,
) -> Option<(Vec<Outpoint>, Vec<Amount>)> {
    let bdk = bdk_candidates(candidates);
    let mut coin_selector = CoinSelector::new(&bdk);

    let change_policy = change_policy_for(target);
    let metric = LowestFee {
        target,
        long_term_feerate: long_term_feerate(),
        change_policy,
    };

    if let Err(err) = coin_selector.run_bnb(metric, 100_000) {
        warn!("BNB failed to find a solution: {}", err);
        if coin_selector.select_until_target_met(target).is_err() {
            return None;
        }
    }

    let inputs = coin_selector
        .apply_selection(candidates)
        .map(|c| c.outpoint)
        .collect();
    let change = drain_to_change(coin_selector.drain(target, change_policy));
    Some((inputs, change))
}

/// Select all candidates (consolidation / spend-all strategy).
/// Returns (selected_inputs, change_outputs).
pub(crate) fn select_all(
    candidates: &[CoinCandidate],
    target: Target,
) -> (Vec<Outpoint>, Vec<Amount>) {
    let bdk = bdk_candidates(candidates);
    let mut coin_selector = CoinSelector::new(&bdk);
    coin_selector.select_all();

    let change_policy = change_policy_for(target);
    let inputs = candidates.iter().map(|c| c.outpoint).collect();
    let change = drain_to_change(coin_selector.drain(target, change_policy));
    (inputs, change)
}
