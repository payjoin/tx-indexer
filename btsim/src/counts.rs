use std::num::NonZeroUsize;
use std::time::{Duration, Instant};

use dense_subset_sum::{
    radix_mappings, w_brute, w_sasamoto, w_sparse, Ambiguity, Bracket, Transaction,
    DEFAULT_MEMORY_BUDGET,
};

use crate::subset_sum::{brute_feasible, subsum_feasible, SUBSUM_MAX_OUTPUTS};
use crate::transaction::TxId;
use crate::Simulation;

pub(crate) fn sim_tx_to_dss(sim: &Simulation, txid: TxId) -> Option<Transaction> {
    let handle = txid.with(sim);
    if handle.is_coinbase() {
        return None;
    }
    let outputs: Vec<u64> = handle.outputs().map(|o| o.data().amount.to_sat()).collect();
    let inputs: Vec<u64> = handle
        .inputs()
        .map(|i| i.prevout().data().amount.to_sat())
        .collect();
    Some(Transaction::new(inputs, outputs))
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct Limits {
    pub(crate) brute_max_terms: usize,
    pub(crate) max_size: usize,
    pub(crate) radix_max_size: Option<usize>,
    pub(crate) sparse_mem_budget: NonZeroUsize,
    pub(crate) subsum_max_outputs: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            brute_max_terms: 15,
            max_size: 6,
            radix_max_size: None,
            sparse_mem_budget: DEFAULT_MEMORY_BUDGET,
            subsum_max_outputs: SUBSUM_MAX_OUTPUTS,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Status {
    Computed,
    Aborted(&'static str),
}

#[derive(Debug, Clone)]
pub(crate) struct CountRow {
    pub(crate) name: &'static str,
    pub(crate) ambiguity: Ambiguity,
    pub(crate) is_lower_bound: bool,
    pub(crate) status: Status,
    pub(crate) cpu: Duration,
    pub(crate) peak_bytes: usize,
}

fn measure<F: FnOnce() -> Ambiguity>(f: F) -> (Ambiguity, Duration, usize) {
    crate::alloc_probe::reset_peak();
    let t = Instant::now();
    let a = f();
    let cpu = t.elapsed();
    let peak = crate::alloc_probe::peak_bytes();
    (a, cpu, peak)
}

pub(crate) fn four_counts(tx: &Transaction, limits: &Limits) -> [CountRow; 4] {
    let (inputs, outputs) = (&tx.inputs, &tx.outputs);

    let brute = if brute_feasible(inputs.len(), outputs.len(), limits.brute_max_terms) {
        let (a, cpu, peak) = measure(|| w_brute(inputs, outputs, limits.max_size));
        CountRow {
            name: "brute",
            ambiguity: a,
            is_lower_bound: true,
            status: Status::Computed,
            cpu,
            peak_bytes: peak,
        }
    } else {
        CountRow {
            name: "brute",
            ambiguity: Ambiguity::Unknown,
            is_lower_bound: true,
            status: Status::Aborted("N+M over brute cap"),
            cpu: Duration::ZERO,
            peak_bytes: 0,
        }
    };

    let (a, cpu, peak) =
        measure(|| radix_mappings(outputs, limits.radix_max_size.unwrap_or(limits.max_size)));
    let radix = CountRow {
        name: "radix",
        ambiguity: a,
        is_lower_bound: true,
        status: Status::Computed,
        cpu,
        peak_bytes: peak,
    };

    let sparse = if subsum_feasible(outputs.len(), limits.subsum_max_outputs) {
        let (a, cpu, peak) =
            measure(|| w_sparse(inputs, outputs, limits.max_size, limits.sparse_mem_budget));
        CountRow {
            name: "sparse",
            ambiguity: a,
            is_lower_bound: true,
            status: Status::Computed,
            cpu,
            peak_bytes: peak,
        }
    } else {
        CountRow {
            name: "sparse",
            ambiguity: Ambiguity::Unknown,
            is_lower_bound: true,
            status: Status::Aborted("too many outputs for subsum enumeration"),
            cpu: Duration::ZERO,
            peak_bytes: 0,
        }
    };

    let sasamoto = if subsum_feasible(outputs.len(), limits.subsum_max_outputs) {
        let (a, cpu, peak) = measure(|| w_sasamoto(inputs, outputs));
        CountRow {
            name: "sasamoto",
            ambiguity: a,
            is_lower_bound: false,
            status: Status::Computed,
            cpu,
            peak_bytes: peak,
        }
    } else {
        CountRow {
            name: "sasamoto",
            ambiguity: Ambiguity::Unknown,
            is_lower_bound: false,
            status: Status::Aborted("too many outputs for subsum enumeration"),
            cpu: Duration::ZERO,
            peak_bytes: 0,
        }
    };

    [brute, radix, sparse, sasamoto]
}

#[derive(Debug, Clone)]
pub(crate) struct PrimitiveBench {
    pub(crate) name: &'static str,
    pub(crate) n_txs: usize,
    pub(crate) n_aborted: usize,
    pub(crate) n_with_count: usize,
    pub(crate) median_cpu_us: u128,
    pub(crate) max_cpu_us: u128,
    pub(crate) median_peak_bytes: usize,
    pub(crate) max_peak_bytes: usize,
}

impl PrimitiveBench {
    pub(crate) fn header() -> &'static str {
        "primitive | runs | aborted | with_count | med_cpu_us | max_cpu_us | med_peak_B | max_peak_B"
    }

    pub(crate) fn row(&self) -> String {
        format!(
            "{:>8} | {:>4} | {:>7} | {:>10} | {:>10} | {:>10} | {:>10} | {:>9}",
            self.name,
            self.n_txs,
            self.n_aborted,
            self.n_with_count,
            self.median_cpu_us,
            self.max_cpu_us,
            self.median_peak_bytes,
            self.max_peak_bytes,
        )
    }
}

fn median_u128(mut v: Vec<u128>) -> u128 {
    if v.is_empty() {
        return 0;
    }
    v.sort_unstable();
    v[v.len() / 2]
}

pub(crate) fn bench_four_counts(txs: &[Transaction], limits: &Limits) -> [PrimitiveBench; 4] {
    let names = ["brute", "radix", "sparse", "sasamoto"];
    let mut cpu: [Vec<u128>; 4] = std::array::from_fn(|_| Vec::new());
    let mut peak: [Vec<u128>; 4] = std::array::from_fn(|_| Vec::new());
    let mut aborted = [0usize; 4];
    let mut with_count = [0usize; 4];
    for tx in txs {
        for (i, r) in four_counts(tx, limits).iter().enumerate() {
            cpu[i].push(r.cpu.as_micros());
            peak[i].push(r.peak_bytes as u128);
            if matches!(r.status, Status::Aborted(_)) {
                aborted[i] += 1;
            }
            if r.ambiguity.lower_bound_count().is_some() {
                with_count[i] += 1;
            }
        }
    }
    std::array::from_fn(|i| PrimitiveBench {
        name: names[i],
        n_txs: txs.len(),
        n_aborted: aborted[i],
        n_with_count: with_count[i],
        median_cpu_us: median_u128(cpu[i].clone()),
        max_cpu_us: cpu[i].iter().copied().max().unwrap_or(0),
        median_peak_bytes: median_u128(peak[i].clone()) as usize,
        max_peak_bytes: peak[i].iter().copied().max().unwrap_or(0) as usize,
    })
}

fn midpoint_target(inputs: &[u64]) -> u64 {
    inputs.iter().copied().fold(0u64, u64::saturating_add) / 2
}

pub(crate) fn regime_for_tx(tx: &Transaction) -> Option<Bracket> {
    Bracket::new(tx.inputs.iter().copied(), midpoint_target(&tx.inputs))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn returns_four_named_rows() {
        let tx = Transaction::new(vec![1, 2, 4, 8], vec![3, 12]);
        let rows = four_counts(&tx, &Limits::default());
        let names: Vec<&str> = rows.iter().map(|r| r.name).collect();
        assert_eq!(names, vec!["brute", "radix", "sparse", "sasamoto"]);
    }

    #[test]
    fn bench_four_counts_summarizes_cost_and_coverage() {
        use dense_subset_sum::fixtures::wasabi2_positive as wp;
        let txs = vec![
            fixtures::maurer_fig2(),
            fixtures::equal_denominations(),
            Transaction::new(vec![500, 500, 500], vec![500, 1000]),
            wp::wasabi2_pos_4d1424ce_20in22out(),
            wp::wasabi2_pos_03b4bd61_20in34out(),
        ];
        let bench = bench_four_counts(&txs, &Limits::default());
        eprintln!("\n{}", PrimitiveBench::header());
        for b in &bench {
            eprintln!("{}", b.row());
        }
        assert_eq!(bench[1].name, "radix");
        assert_eq!(bench[1].n_aborted, 0, "radix never aborts");
        assert_eq!(
            bench[1].n_with_count,
            txs.len(),
            "radix always yields a count"
        );
        assert!(
            bench[0].n_aborted >= 2,
            "brute aborts on the large coinjoins"
        );
        assert!(
            bench[2].n_aborted >= 2,
            "sparse aborts above subsum_max_outputs"
        );
        assert!(
            bench[3].n_aborted >= 2,
            "sasamoto aborts above subsum_max_outputs"
        );
    }

    #[test]
    fn l_sweep_shifts_regime_and_max_money_is_conservative() {
        use dense_subset_sum::{kappa, regime_at_l, L};
        let tx = dense_subset_sum::fixtures::wasabi2_positive::wasabi2_pos_03b4bd61_20in34out();
        let n = tx.inputs.len();
        let e = midpoint_target(&tx.inputs);
        eprintln!("\nL sweep @ E={e}, N={n}:");
        for &l in L::all() {
            let lv = l.value(&tx.inputs);
            eprintln!(
                "  L={:<12} (={lv}) -> κ={:?} regime={:?}",
                l.to_string(),
                kappa(lv, n),
                regime_at_l(&tx.inputs, e, l)
            );
        }
        let k_max = kappa(L::Max.value(&tx.inputs), n).unwrap();
        let k_money = kappa(L::MaxMoney.value(&tx.inputs), n).unwrap();
        assert!(
            k_max <= k_money,
            "MAX_MONEY (larger L) must give κ ≥ max(A): {k_max} vs {k_money}"
        );
        assert!(
            regime_at_l(&tx.inputs, e, L::Max).is_some(),
            "regime computable at L=max(A)"
        );
    }

    #[test]
    fn regime_for_tx_brackets_a_real_coinjoin() {
        let tx = dense_subset_sum::fixtures::wasabi2_positive::wasabi2_pos_03b4bd61_20in34out();
        let bracket = regime_for_tx(&tx).expect("bracket computable for a non-empty coinjoin");
        eprintln!("\nregime_for_tx(wasabi 20in34out): {bracket}");
        assert!(bracket.kappa().best().is_finite() && bracket.kappa().worst().is_finite());
    }

    #[test]
    fn large_coinjoin_aborts_explosive_paths_and_radix_carries() {
        let tx = dense_subset_sum::fixtures::wasabi2_positive::wasabi2_pos_03b4bd61_20in34out();
        let rows = four_counts(&tx, &Limits::default()); // [brute, radix, sparse, sasamoto]
        assert!(
            matches!(rows[0].status, Status::Aborted(_)),
            "brute should abort on N+M>15"
        );
        assert!(
            matches!(rows[2].status, Status::Aborted(_)),
            "sparse should abort above subsum_max_outputs"
        );
        assert!(
            matches!(rows[3].status, Status::Aborted(_)),
            "sasamoto should abort above subsum_max_outputs"
        );
        let radix = rows[1]
            .ambiguity
            .lower_bound_count()
            .expect("radix yields an exact count");
        assert!(
            radix > 0,
            "radix should produce a positive mapping count on a real coinjoin, got 0"
        );
    }

    #[test]
    fn sasamoto_is_not_a_lower_bound() {
        let tx = Transaction::new(vec![1, 2, 4, 8], vec![3, 12]);
        let rows = four_counts(&tx, &Limits::default());
        assert!(!rows[3].is_lower_bound);
    }

    use dense_subset_sum::fixtures;

    #[test]
    fn sparse_lower_bound_does_not_exceed_brute() {
        let tx = Transaction::new(vec![500, 500, 500], vec![500, 1000]);
        let rows = four_counts(&tx, &Limits::default());
        let brute = rows[0]
            .ambiguity
            .lower_bound_count()
            .expect("brute is Exact on small tx");
        assert!(
            brute > 0,
            "brute count must be > 0 for a meaningful comparison"
        );
        let sparse = rows[2]
            .ambiguity
            .lower_bound_count()
            .expect("sparse is Exact on small tx");
        assert!(
            sparse <= brute,
            "sparse lower bound {} exceeded brute {}",
            sparse,
            brute
        );
    }

    #[test]
    fn radix_is_exact_on_denominated_outputs() {
        let tx = Transaction::new(vec![1_000, 10_000], vec![1_000, 10_000]);
        let rows = four_counts(&tx, &Limits::default());
        assert!(
            rows[1].ambiguity.is_exact(),
            "radix should be Exact, got {:?}",
            rows[1].ambiguity
        );
        let n = rows[1].ambiguity.lower_bound_count().unwrap();
        assert!(
            n > 0,
            "radix should produce nonzero mappings on standard denominations, got 0"
        );
    }

    #[test]
    fn sasamoto_never_yields_a_lower_bound_count() {
        let tx = fixtures::maurer_fig2();
        let rows = four_counts(&tx, &Limits::default());
        assert!(
            rows[3].ambiguity.lower_bound_count().is_none(),
            "sasamoto should never expose a lower-bound count, got {:?}",
            rows[3].ambiguity
        );
    }

    #[test]
    fn sasamoto_is_a_good_estimate_in_regime() {
        let a: Vec<u64> = (1..=20).collect();
        let report =
            dense_subset_sum::harness::vs_oracle::compare(&a, 100, 10, 1_000_000, "btsim-sasamoto");
        assert!(report.sasamoto.n_points > 0, "no comparable points");
        assert!(
            report.sasamoto.median_error < 0.10,
            "sasamoto median error {:.1}% exceeds 10%",
            report.sasamoto.median_error * 100.0
        );
    }

    use crate::config::{ScorerConfig, WalletTypeConfig};
    use crate::script_type::ScriptType;
    use crate::SimulationBuilder;

    #[test]
    fn confirmed_tx_to_dss_extracts_amounts() {
        let mut sim = SimulationBuilder::new(
            42,
            vec![WalletTypeConfig {
                name: "t".into(),
                count: 2,
                strategies: vec!["UnilateralSpender".into()],
                scorer: ScorerConfig {
                    privacy_weight: 0.0,
                    payment_obligation_weight: 1.0,
                    min_fallback_plans: 0,
                    subset_sum_threshold: None,
                    subset_sum_max_size: 6,
                    brute_max_terms: 15,
                    radix_threshold: None,
                    radix_density_floor: 0.5,
                },
                script_type: ScriptType::P2tr,
            }],
            20,
            1,
            4,
        )
        .build();
        sim.build_universe();
        let result = sim.run();
        let any = result
            .dss_transactions()
            .into_iter()
            .find(|t| !t.inputs.is_empty());
        if let Some(tx) = any {
            assert!(tx.inputs.iter().all(|a| *a > 0));
            assert!(!tx.outputs.is_empty());
        }
    }
}
