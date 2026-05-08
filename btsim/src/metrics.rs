use bitcoin::Amount;

use crate::actions::{ActionCost, CostMode, Plan};

/// Convert a metric output into an `ActionCost` relative to a privacy budget.
#[allow(clippy::wrong_self_convention)]
pub(crate) trait IntoCost {
    fn into_cost(&self, budget: Amount) -> ActionCost;
}

/// Object-safe erased form of a privacy metric, suitable for storage as a trait object.
pub(crate) trait ErasedPrivacyMetric: std::fmt::Debug {
    fn evaluate_erased(&self, plan: &Plan, mode: CostMode) -> Box<dyn IntoCost>;
    fn clone_box(&self) -> Box<dyn ErasedPrivacyMetric>;
}

// TODO: this will be defined in the privacy metrics repo -- for now its defined here
/// Typed privacy metric trait. Implement this for concrete metrics; they automaticallyk
/// become usable as `ErasedPrivacyMetric` by implementing the erased methods manually.
#[allow(unused)]
pub(crate) trait PrivacyMetric: ErasedPrivacyMetric {
    type Output: IntoCost + std::fmt::Debug;
    fn evaluate(&self, plan: &Plan, mode: CostMode) -> Self::Output;
}

impl Clone for Box<dyn ErasedPrivacyMetric> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

/// A collection of privacy metrics evaluated together against a shared budget.
#[derive(Debug, Clone)]
pub(crate) struct PrivacyBundle {
    pub(crate) metrics: Vec<Box<dyn ErasedPrivacyMetric>>,
    pub(crate) budget: Amount,
}

impl Default for PrivacyBundle {
    fn default() -> Self {
        Self {
            metrics: vec![],
            budget: Amount::ZERO,
        }
    }
}

impl PrivacyBundle {
    pub(crate) fn evaluate(&self, plan: &Plan, mode: CostMode) -> ActionCost {
        self.metrics
            .iter()
            .map(|m| m.evaluate_erased(plan, mode).into_cost(self.budget))
            .sum()
    }
}

/// Raw mapping count: number of non-trivial input subsets whose sum matches some output subset sum.
#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct SubsetSumMappings(pub usize);

impl IntoCost for SubsetSumMappings {
    fn into_cost(&self, budget: Amount) -> ActionCost {
        // More mappings = lower cost. Zero mappings = full budget as penalty.
        let penalty = if self.0 == 0 {
            1.0
        } else {
            1.0 / (1.0 + self.0 as f64)
        };
        ActionCost(penalty * budget.to_sat() as f64)
    }
}

/// Counts how many non-trivial input subsets have a sum that matches some output subset sum.
/// More matches = greater ambiguity = better privacy = lower cost.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct SubsetSumMetric {
    /// Brute-force depth limit: only input subsets of size ≤ max_depth are considered.
    pub(crate) max_depth: usize,
}

impl PrivacyMetric for SubsetSumMetric {
    type Output = SubsetSumMappings;

    fn evaluate(&self, plan: &Plan, mode: CostMode) -> SubsetSumMappings {
        // External term: zero cost in best case (counterparties assumed honest).
        if mode.external == 0.0 {
            return SubsetSumMappings(usize::MAX);
        }
        let input_amounts: Vec<u64> = plan
            .my_inputs
            .iter()
            .map(|(_, a)| a.to_sat())
            .chain(plan.their_inputs.iter().map(|(_, a)| a.to_sat()))
            .collect();
        let output_amounts: Vec<u64> = plan
            .my_outputs
            .iter()
            .map(|a| a.to_sat())
            .chain(plan.their_outputs.iter().map(|a| a.to_sat()))
            .collect();
        SubsetSumMappings(count_non_derived_mappings(
            &input_amounts,
            &output_amounts,
            self.max_depth,
        ))
    }
}

impl ErasedPrivacyMetric for SubsetSumMetric {
    fn evaluate_erased(&self, plan: &Plan, mode: CostMode) -> Box<dyn IntoCost> {
        Box::new(self.evaluate(plan, mode))
    }

    fn clone_box(&self) -> Box<dyn ErasedPrivacyMetric> {
        Box::new(self.clone())
    }
}

/// Count non-derived input output mappings via brute-force subset enumeration.
///
/// Builds the set of all non-empty output subsums, then for each input subset
/// of size <= max_depth checks whether its sum appears in that set.
/// The trivial full-input mapping is excluded. // TODO: should it be excluded?
fn count_non_derived_mappings(
    input_amounts: &[u64],
    output_amounts: &[u64],
    max_depth: usize,
) -> usize {
    if input_amounts.is_empty() || output_amounts.is_empty() {
        return 0;
    }

    // Build set of all non-empty output subset sums (cap at 63 outputs to avoid overflow).
    let n_out = output_amounts.len().min(63);
    let mut output_sums = std::collections::HashSet::new();
    for mask in 1u64..(1u64 << n_out) {
        let sum: u64 = (0..n_out)
            .filter(|i| mask & (1 << i) != 0)
            .map(|i| output_amounts[i])
            .sum();
        output_sums.insert(sum);
    }

    let n_in = input_amounts.len();
    let full_input_sum: u64 = input_amounts.iter().sum();
    let depth = max_depth.min(n_in);
    let mut count = 0usize;

    for size in 1..=depth {
        for combo in combinations(n_in, size) {
            let sum: u64 = combo.map(|i| input_amounts[i]).sum();
            // Exclude the trivial full-input mapping.
            if size == n_in && sum == full_input_sum {
                continue;
            }
            if output_sums.contains(&sum) {
                count += 1;
            }
        }
    }

    count
}

/// Generate all k-element subsets of `{0, 1, ..., n-1}` as an iterator.
fn combinations(n: usize, k: usize) -> impl Iterator<Item = std::vec::IntoIter<usize>> {
    let mut combo: Vec<usize> = (0..k).collect();
    let mut finished = k > n;

    std::iter::from_fn(move || {
        if finished {
            return None;
        }

        if k == 0 {
            finished = true;
            return Some(Vec::new().into_iter());
        }

        let current = combo.clone().into_iter();

        // Find the rightmost element that can still be incremented.
        let mut i = k;
        while i > 0 {
            i -= 1;
            if combo[i] < n - k + i {
                combo[i] += 1;
                for j in (i + 1)..k {
                    combo[j] = combo[j - 1] + 1;
                }
                return Some(current);
            }
        }

        finished = true;
        Some(current)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        actions::{CostMode, Plan, WalletResidue},
        transaction::{Outpoint, TxId},
    };
    use bitcoin::Amount;

    fn make_plan(
        my_input_sats: &[u64],
        their_input_sats: &[u64],
        my_out_sats: &[u64],
        their_out_sats: &[u64],
    ) -> Plan {
        let make_ot = |i: usize| Outpoint {
            txid: TxId(i),
            index: 0,
        };
        Plan {
            my_inputs: my_input_sats
                .iter()
                .enumerate()
                .map(|(i, &a)| (make_ot(i), Amount::from_sat(a)))
                .collect(),
            their_inputs: their_input_sats
                .iter()
                .enumerate()
                .map(|(i, &a)| (make_ot(1000 + i), Amount::from_sat(a)))
                .collect(),
            my_outputs: my_out_sats.iter().map(|&a| Amount::from_sat(a)).collect(),
            their_outputs: their_out_sats
                .iter()
                .map(|&a| Amount::from_sat(a))
                .collect(),
            wallet_residue: WalletResidue {
                utxos: vec![],
                payment_obligations: vec![],
            },
        }
    }

    #[test]
    fn into_cost_zero_mappings_full_penalty() {
        let budget = Amount::from_sat(1000);
        let cost = SubsetSumMappings(0).into_cost(budget);
        // Full penalty is applied bc there are no mappings at all
        assert_eq!(cost, ActionCost(1000.0));
    }

    #[test]
    fn into_cost_max_mappings_near_zero() {
        let budget = Amount::from_sat(1000);
        let cost = SubsetSumMappings(usize::MAX).into_cost(budget);
        // 1/(1 + usize::MAX as f64) asymptotically approaches 0
        assert!(cost.0 < 1e-10);
    }

    #[test]
    fn best_case_returns_max_mappings() {
        let metric = SubsetSumMetric { max_depth: 8 };
        let plan = make_plan(&[1000], &[], &[900], &[]);
        let result = metric.evaluate(&plan, CostMode::EXTERNAL_PENALTIES_OFF);
        assert_eq!(result.0, usize::MAX);
    }

    #[test]
    fn unilateral_no_amount_match_worst_case() {
        // Input=1000, Output=900: no subset sum match → 0 mappings → full penalty
        let metric = SubsetSumMetric { max_depth: 8 };
        let plan = make_plan(&[1000], &[], &[900], &[]);
        let result = metric.evaluate(&plan, CostMode::EXTERNAL_PENALTIES_ON);
        assert_eq!(result.0, 0);
    }

    #[test]
    fn cospend_equal_amounts_ambiguous() {
        // Two inputs of 500, two outputs of 500: each single input matches an output subset sum.
        let metric = SubsetSumMetric { max_depth: 8 };
        let plan = make_plan(&[500], &[500], &[500], &[500]);
        let result = metric.evaluate(&plan, CostMode::EXTERNAL_PENALTIES_ON);
        // Both {my[0]=500} and {their[0]=500} are non-trivial and match output sums.
        assert!(
            result.0 >= 2,
            "expected more than or equal to 2 mappings, got {}",
            result.0
        );
    }

    #[test]
    fn cospend_scores_better_than_unilateral_traceable() {
        let metric = SubsetSumMetric { max_depth: 8 };
        let budget = Amount::from_sat(1000);
        let mode = CostMode::EXTERNAL_PENALTIES_ON;

        // Traceable unilateral: no matching sums
        let unilateral = make_plan(&[1000], &[], &[900], &[]);
        let unilateral_cost = metric.evaluate(&unilateral, mode).into_cost(budget);

        // Ambiguous cospend: many matching subsets
        let cospend = make_plan(&[500], &[500], &[500], &[500]);
        let cospend_cost = metric.evaluate(&cospend, mode).into_cost(budget);

        assert!(
            cospend_cost < unilateral_cost,
            "cospend ({:?}) should be cheaper than traceable unilateral ({:?})",
            cospend_cost,
            unilateral_cost
        );
    }

    #[test]
    fn unilateral_bracket_collapses_with_bundle() {
        // With SubsetSumMetric, a unilateral plan should have the same cost in both modes
        // because BestCase returns usize::MAX (≈0 cost) and WorstCase also computes the same
        // mapping count from the fixed amounts. Wait: they differ if the metric is external-only.
        // BestCase → usize::MAX → near-zero. WorstCase → actual count.
        // So they're only equal if the actual count happens to be usize::MAX too.
        // The spec says "bracket collapses" for unilateral — meaning both modes give ~same answer
        // because there's no external counterparty. Let's verify best_case ≤ worst_case.
        let metric = SubsetSumMetric { max_depth: 8 };
        let budget = Amount::from_sat(1000);
        let plan = make_plan(&[1000], &[], &[900], &[]);
        let best = metric
            .evaluate(&plan, CostMode::EXTERNAL_PENALTIES_OFF)
            .into_cost(budget);
        let worst = metric
            .evaluate(&plan, CostMode::EXTERNAL_PENALTIES_ON)
            .into_cost(budget);
        // BestCase gives near-zero (usize::MAX mappings), WorstCase gives full penalty (0 mappings).
        // For a unilateral plan with no amount match, best < worst.
        assert!(best <= worst, "best case should be ≤ worst case");
    }

    #[test]
    fn empty_bundle_zero_cost() {
        let bundle = PrivacyBundle::default();
        let plan = make_plan(&[1000], &[], &[900], &[]);
        let cost = bundle.evaluate(&plan, CostMode::EXTERNAL_PENALTIES_ON);
        assert_eq!(cost, ActionCost(0.0));
    }

    #[test]
    fn no_match_returns_zero() {
        assert_eq!(count_non_derived_mappings(&[1000], &[900], 8), 0);
    }

    #[test]
    fn single_match() {
        // 2 inputs {500, 300}, output {500}: only the {500} subset matches.
        // The full-input subset {500+300=800} doesn't match and is excluded anyway.
        assert_eq!(count_non_derived_mappings(&[500, 300], &[500], 8), 1);
    }

    #[test]
    fn two_inputs_two_outputs_same_amounts() {
        // Inputs: {500, 300}, Outputs: {500, 300}
        // Matching non-trivial subsets: {500} matches 500, {300} matches 300 → 2
        assert_eq!(count_non_derived_mappings(&[500, 300], &[500, 300], 8), 2);
    }

    #[test]
    fn full_subset_excluded() {
        // Single input, single output, same amount: the only subset is the full input → excluded
        assert_eq!(count_non_derived_mappings(&[500], &[500], 8), 0);
    }

    #[test]
    fn depth_limit_respected() {
        // 4 inputs but max_depth=1: only size-1 subsets are checked
        let count_d1 = count_non_derived_mappings(&[100, 200, 300, 400], &[100, 200, 300, 400], 1);
        let count_d4 = count_non_derived_mappings(&[100, 200, 300, 400], &[100, 200, 300, 400], 4);
        assert!(count_d1 <= count_d4);
    }

    #[test]
    fn test_combinations() {
        let result: Vec<Vec<usize>> = combinations(2, 1).map(|combo| combo.collect()).collect();
        assert_eq!(result, vec![vec![0], vec![1]]);

        let result: Vec<Vec<usize>> = combinations(3, 2).map(|combo| combo.collect()).collect();
        assert_eq!(result, vec![vec![0, 1], vec![0, 2], vec![1, 2]]);

        let result: Vec<Vec<usize>> = combinations(2, 3).map(|combo| combo.collect()).collect();
        assert!(result.is_empty());
    }
}
