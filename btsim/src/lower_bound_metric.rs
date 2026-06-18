use bitcoin::Amount;
use dense_subset_sum::{
    radix_density, radix_mappings, w_brute, w_sparse, Ambiguity, DEFAULT_MEMORY_BUDGET,
};

use crate::actions::{ActionCost, CostMode, Plan};
use crate::metrics::{ErasedPrivacyMetric, IntoCost, PrivacyMetric};
use crate::subset_sum::{brute_feasible, subsum_feasible, SUBSUM_MAX_OUTPUTS};

/// Best guaranteed lower bound on W via the brute/sparse ladder. Sasamoto is excluded (an
/// approximation, never the sole basis in the critical path); radix is a different object (it counts
/// k×m! mappings, not W) and lives in its own metric.
#[derive(Debug)]
pub(crate) struct WLowerBound {
    pub(crate) best: u128,
    pub(crate) threshold: u128,
}

impl IntoCost for WLowerBound {
    fn into_cost(&self, budget: Amount) -> ActionCost {
        if self.best >= self.threshold {
            ActionCost(0.0)
        } else {
            let deficit = (self.threshold - self.best) as f64 / self.threshold as f64;
            ActionCost(deficit * budget.to_sat() as f64)
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct WLowerBoundMetric {
    pub(crate) max_size: usize,
    pub(crate) brute_max_terms: usize,
    pub(crate) threshold: u128,
}

impl PrivacyMetric for WLowerBoundMetric {
    type Output = WLowerBound;

    fn evaluate(&self, plan: &Plan, mode: CostMode) -> WLowerBound {
        if mode.external == 0.0 {
            return WLowerBound {
                best: u128::MAX,
                threshold: self.threshold,
            };
        }

        let inputs: Vec<u64> = plan
            .my_inputs
            .iter()
            .chain(plan.their_inputs.iter())
            .map(|(_, a)| a.to_sat())
            .collect();
        let outputs: Vec<u64> = plan
            .my_outputs
            .iter()
            .chain(plan.their_outputs.iter())
            .map(|a| a.to_sat())
            .collect();

        let brute = if brute_feasible(inputs.len(), outputs.len(), self.brute_max_terms) {
            w_brute(&inputs, &outputs, self.max_size)
        } else {
            Ambiguity::Unknown
        };
        let sparse = if subsum_feasible(outputs.len(), SUBSUM_MAX_OUTPUTS) {
            w_sparse(&inputs, &outputs, self.max_size, DEFAULT_MEMORY_BUDGET)
        } else {
            Ambiguity::Unknown
        };

        let best = [brute, sparse]
            .iter()
            .filter_map(Ambiguity::lower_bound_count)
            .max()
            .unwrap_or(0);

        WLowerBound {
            best,
            threshold: self.threshold,
        }
    }
}

impl ErasedPrivacyMetric for WLowerBoundMetric {
    fn evaluate_erased(&self, plan: &Plan, mode: CostMode) -> Box<dyn IntoCost> {
        Box::new(self.evaluate(plan, mode))
    }

    fn clone_box(&self) -> Box<dyn ErasedPrivacyMetric> {
        Box::new(self.clone())
    }
}

/// Radix special-case anonymity facet: Σ k×m! equivalent mappings over the outputs. A DIFFERENT
/// object from W (it ignores inputs), so it is its own metric — never maxed into the W bound. Only
/// credited when the outputs are denomination-dense enough (`radix_density ≥ density_floor`) for the
/// k×m! count to be reliable.
#[derive(Debug)]
pub(crate) struct RadixMapping {
    pub(crate) count: u128,
    pub(crate) threshold: u128,
}

impl IntoCost for RadixMapping {
    fn into_cost(&self, budget: Amount) -> ActionCost {
        if self.count >= self.threshold {
            ActionCost(0.0)
        } else {
            let deficit = (self.threshold - self.count) as f64 / self.threshold as f64;
            ActionCost(deficit * budget.to_sat() as f64)
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RadixMappingMetric {
    pub(crate) max_size: usize,
    pub(crate) threshold: u128,
    pub(crate) density_floor: f64,
}

impl PrivacyMetric for RadixMappingMetric {
    type Output = RadixMapping;

    fn evaluate(&self, plan: &Plan, mode: CostMode) -> RadixMapping {
        if mode.external == 0.0 {
            return RadixMapping {
                count: u128::MAX,
                threshold: self.threshold,
            };
        }
        let outputs: Vec<u64> = plan
            .my_outputs
            .iter()
            .chain(plan.their_outputs.iter())
            .map(|a| a.to_sat())
            .collect();
        let count = if radix_density(&outputs) >= self.density_floor {
            radix_mappings(&outputs, self.max_size)
                .lower_bound_count()
                .unwrap_or(0)
        } else {
            0
        };
        RadixMapping {
            count,
            threshold: self.threshold,
        }
    }
}

impl ErasedPrivacyMetric for RadixMappingMetric {
    fn evaluate_erased(&self, plan: &Plan, mode: CostMode) -> Box<dyn IntoCost> {
        Box::new(self.evaluate(plan, mode))
    }
    fn clone_box(&self) -> Box<dyn ErasedPrivacyMetric> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actions::WalletResidue;
    use crate::transaction::{Outpoint, TxId};

    #[test]
    fn threshold_met_is_zero_cost() {
        let lb = WLowerBound {
            best: 1000,
            threshold: 1000,
        };
        assert_eq!(lb.into_cost(Amount::from_sat(500)), ActionCost(0.0));
    }

    #[test]
    fn zero_best_is_full_penalty() {
        let lb = WLowerBound {
            best: 0,
            threshold: 1000,
        };
        assert_eq!(lb.into_cost(Amount::from_sat(500)), ActionCost(500.0));
    }

    #[test]
    fn partial_is_proportional_deficit() {
        let lb = WLowerBound {
            best: 250,
            threshold: 1000,
        };
        assert_eq!(lb.into_cost(Amount::from_sat(400)), ActionCost(300.0));
    }

    fn plan(my_in: &[u64], their_in: &[u64], my_out: &[u64], their_out: &[u64]) -> Plan {
        let ot = |i: usize| Outpoint {
            txid: TxId(i),
            index: 0,
        };
        Plan {
            my_inputs: my_in
                .iter()
                .enumerate()
                .map(|(i, &a)| (ot(i), Amount::from_sat(a)))
                .collect(),
            their_inputs: their_in
                .iter()
                .enumerate()
                .map(|(i, &a)| (ot(1000 + i), Amount::from_sat(a)))
                .collect(),
            my_outputs: my_out.iter().map(|&a| Amount::from_sat(a)).collect(),
            their_outputs: their_out.iter().map(|&a| Amount::from_sat(a)).collect(),
            wallet_residue: WalletResidue {
                utxos: vec![],
                payment_obligations: vec![],
            },
        }
    }

    fn metric() -> WLowerBoundMetric {
        WLowerBoundMetric {
            max_size: 6,
            brute_max_terms: 15,
            threshold: 2,
        }
    }

    #[test]
    fn external_off_is_best_case() {
        let out = metric().evaluate(
            &plan(&[1000], &[], &[900], &[]),
            CostMode::EXTERNAL_PENALTIES_OFF,
        );
        assert_eq!(out.best, u128::MAX);
    }

    #[test]
    fn cospend_equal_amounts_has_a_bound() {
        let out = metric().evaluate(
            &plan(&[500], &[500], &[500], &[500]),
            CostMode::EXTERNAL_PENALTIES_ON,
        );
        assert!(
            out.best >= 1,
            "expected a positive lower bound, got {}",
            out.best
        );
    }

    fn radix_metric() -> RadixMappingMetric {
        RadixMappingMetric {
            max_size: 6,
            threshold: 2,
            density_floor: 0.5,
        }
    }

    #[test]
    fn radix_gate_credits_denominated_outputs() {
        // outputs all standard denoms (512 = 2^9): density 1.0 >= floor => count = k×m! > 0
        let out = radix_metric().evaluate(
            &plan(&[1536], &[], &[512, 512, 512], &[]),
            CostMode::EXTERNAL_PENALTIES_ON,
        );
        assert!(
            out.count >= 1,
            "denominated outputs must yield a positive radix mapping count, got {}",
            out.count
        );
    }

    #[test]
    fn radix_gate_rejects_arbitrary_outputs() {
        // non-denominated outputs: density 0 < floor => no credit (count 0)
        let out = radix_metric().evaluate(
            &plan(&[2_000_006], &[], &[1_000_003, 1_000_003], &[]),
            CostMode::EXTERNAL_PENALTIES_ON,
        );
        assert_eq!(
            out.count, 0,
            "arbitrary (non-denom) outputs must not be credited, got {}",
            out.count
        );
    }

    #[test]
    fn radix_external_off_is_best_case() {
        let out = radix_metric().evaluate(
            &plan(&[1536], &[], &[512, 512, 512], &[]),
            CostMode::EXTERNAL_PENALTIES_OFF,
        );
        assert_eq!(out.count, u128::MAX);
    }

    #[test]
    fn radix_into_cost_threshold_and_deficit() {
        assert_eq!(
            RadixMapping {
                count: 10,
                threshold: 10
            }
            .into_cost(Amount::from_sat(500)),
            ActionCost(0.0)
        );
        assert_eq!(
            RadixMapping {
                count: 0,
                threshold: 1000
            }
            .into_cost(Amount::from_sat(500)),
            ActionCost(500.0)
        );
    }
}
