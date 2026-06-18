//! Correctness-axis benchmark: synthetic ladders measured for oracle agreement + Sasamoto error
//! (via the dep's vs_oracle harness) and cost (via crate::counts::four_counts).

use std::num::NonZeroUsize;

use rand::Rng;
use rand_pcg::rand_core::SeedableRng;
use rand_pcg::Pcg64;

use crate::counts::{four_counts, CountRow, Limits, Status};
use dense_subset_sum::harness::vs_cja::{compare_w_vs_mappings, MappingComparison};
use dense_subset_sum::harness::vs_oracle::{compare, compare_dp_ground_truth, compare_monte_carlo};
use dense_subset_sum::Transaction;
use dense_subset_sum::{
    is_standard_denom, standard_denoms_in_range, Ambiguity, DEFAULT_MAX_DENOM_SATS,
    DEFAULT_MIN_DENOM_SATS,
};

const LADDER_MULT: usize = 3;
const RADIX_MAX_SIZE: usize = 6; // radix decomposition depth (kept small => fast)
const SPARSE_MEM_BUDGET: usize = 4096; // entries; from calibration (visible RAM flip)
const DP_MAX: usize = 4_000_000; // DP cells; from calibration (dp survives flip)
const MIN_W: u64 = 2;
const LOOKUP_K: usize = 6;
const MC_SAMPLES: u64 = 200_000;
const MC_SEED: u64 = 42;

const RANDOM_DENSE_L_MAX: u64 = 16; // from calibration: dense (κ ≤ 0.5) across N=8..24
const RANDOM_DENSE_SEED: u64 = 100; // base seed (per-rung: + n)
const RANDOM_DENSE_SPARSE_BUDGET: usize = 512; // budget where sparse flips Exact→LowerBound at N=12→14
#[cfg(test)]
const SASA_ERR_BOUND: f64 = 0.10; // dense-rung (N≥14) Sasamoto error stays under this (test gate)

const BRUTE: usize = 0;
const RADIX: usize = 1;
const SPARSE: usize = 2;
const SASAMOTO: usize = 3;

/// brute+sparse get a full counting cap (max_size = n) so sparse is Exact until RAM truncation;
/// radix gets RADIX_MAX_SIZE so its DFS stays fast.
fn ladder_limits(n: usize) -> Limits {
    Limits {
        brute_max_terms: 15,
        max_size: n,
        radix_max_size: Some(RADIX_MAX_SIZE),
        sparse_mem_budget: NonZeroUsize::new(SPARSE_MEM_BUDGET).expect("budget > 0"),
        subsum_max_outputs: 20,
    }
}

/// One power of 2 (>=2^1) repeated `mult` times, for `n_powers` consecutive powers.
/// gcd is 2, so DP ground truth (Σa/gcd) stays reachable as N grows.
fn pow2_set(n_powers: u32, mult: usize) -> Vec<u64> {
    let mut v = Vec::with_capacity(n_powers as usize * mult);
    for k in 1..=n_powers {
        for _ in 0..mult {
            v.push(1u64 << k);
        }
    }
    v.sort_unstable();
    v
}

/// N values uniform in [1, l_max], seeded. A narrow l_max keeps κ = log2(l_max)/N small (dense)
/// while the values are randomly sampled — the regime where Sasamoto's asymptotic is stated to hold.
fn random_dense_set(n: usize, l_max: u64, seed: u64) -> Vec<u64> {
    let mut rng = Pcg64::seed_from_u64(seed);
    let mut v: Vec<u64> = (0..n).map(|_| rng.random_range(1..=l_max)).collect();
    v.sort_unstable();
    v
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum AmbiguityKind {
    Exact,
    LowerBound,
    LogApprox,
    Unknown,
}

impl AmbiguityKind {
    fn of(a: &Ambiguity) -> Self {
        match a {
            Ambiguity::Exact(_) => Self::Exact,
            Ambiguity::LowerBound(_) => Self::LowerBound,
            Ambiguity::LogApprox(_) => Self::LogApprox,
            Ambiguity::Unknown => Self::Unknown,
            _ => Self::Unknown,
        }
    }
}

pub(crate) struct Rung {
    pub family: &'static str,
    pub set: Vec<u64>,
}

/// pow2 rungs: consecutive powers of 2 with multiplicity, growing N. Dense by construction.
fn pow2_ladder() -> Vec<Rung> {
    (3..=14u32)
        .map(|n_powers| Rung {
            family: "pow2",
            set: pow2_set(n_powers, LADDER_MULT),
        })
        .collect()
}

/// radix rungs: the first `d` standard denoms repeated `LADDER_MULT` times, growing d.
fn radix_ladder() -> Vec<Rung> {
    let denoms = standard_denoms_in_range(DEFAULT_MIN_DENOM_SATS, DEFAULT_MAX_DENOM_SATS);
    (3..=10usize)
        .filter(|&d| d <= denoms.len())
        .map(|d| {
            let mut set: Vec<u64> = denoms[..d]
                .iter()
                .flat_map(|&v| std::iter::repeat_n(v, LADDER_MULT))
                .collect();
            set.sort_unstable();
            Rung {
                family: "radix",
                set,
            }
        })
        .collect()
}

/// arbitrary rungs: deterministic non-denominated values (skip any that land on a standard denom).
fn arbitrary_ladder() -> Vec<Rung> {
    (3..=12usize)
        .map(|n| {
            let mut set: Vec<u64> = (0..n as u64)
                .map(|i| 1_000_003 + i * 999_983)
                .filter(|&v| !is_standard_denom(v))
                .collect();
            set.sort_unstable();
            Rung {
                family: "arbitrary",
                set,
            }
        })
        .collect()
}

/// random_dense rungs: seeded uniform-in-[1,L_MAX] sets of growing N. κ = log2(L_MAX)/N shrinks as
/// N grows (denser); the L225-faithful Sasamoto test bed (randomly sampled, dense).
fn random_dense_ladder() -> Vec<Rung> {
    (8..=24usize)
        .step_by(2)
        .map(|n| Rung {
            family: "random_dense",
            set: random_dense_set(n, RANDOM_DENSE_L_MAX, RANDOM_DENSE_SEED + n as u64),
        })
        .collect()
}

/// random_dense needs a smaller sparse budget than the default to exhibit the RAM-driven flip
/// (calibration). Other families keep the default `ladder_limits` budget.
fn ladder_limits_for(family: &str, n: usize) -> Limits {
    let mut limits = ladder_limits(n);
    if family == "random_dense" {
        limits.sparse_mem_budget =
            NonZeroUsize::new(RANDOM_DENSE_SPARSE_BUDGET).expect("budget > 0");
    }
    limits
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum GroundTruth {
    Exhaustive,
    Dp,
    MonteCarlo,
    None,
}

/// Picks the ground-truth path by N (the crossover engine). Returns the ground-truth kind and the
/// Sasamoto error/spearman vs that truth (None when there are no comparable points).
fn run_oracle(set: &[u64]) -> (GroundTruth, Option<f64>, Option<f64>) {
    if set.is_empty() {
        return (GroundTruth::None, None, None);
    }
    let (gt, report) = if set.len() <= 22 {
        (
            GroundTruth::Exhaustive,
            compare(set, MIN_W, LOOKUP_K, DP_MAX, "btsim"),
        )
    } else {
        match compare_dp_ground_truth(set, MIN_W, LOOKUP_K, DP_MAX, "btsim") {
            Ok(r) => (GroundTruth::Dp, r),
            Err(_) => (
                GroundTruth::MonteCarlo,
                compare_monte_carlo(
                    set, MIN_W, LOOKUP_K, DP_MAX, "btsim", MC_SAMPLES, 0, MC_SEED,
                ),
            ),
        }
    };
    let s = &report.sasamoto;
    if s.n_points > 0 {
        (gt, Some(s.median_error), Some(s.spearman))
    } else {
        (gt, None, None)
    }
}

/// Whether the internal lower-bound invariant (sparse ≤ exact brute) held.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Consistency {
    Ok,
    Violated,
}

pub(crate) struct CostRow {
    pub kind: [AmbiguityKind; 4],
    pub cpu_us: [u128; 4],
    pub peak_bytes: [usize; 4],
    pub status: [Status; 4],
    pub consistency: Consistency,
}

/// Runs four_counts on a few-outputs instance (inputs = set, outputs = [Σset/2]) so the input
/// sumset RAM — not the output-count guard — is the binding cost. Extracts per-method kind/cost
/// and the internal lower-bound consistency.
fn run_cost(set: &[u64], limits: &Limits) -> CostRow {
    let target = set.iter().copied().fold(0u64, u64::saturating_add) / 2;
    let tx = Transaction::new(set.to_vec(), vec![target.max(1)]);
    let rows = four_counts(&tx, limits);
    CostRow {
        kind: std::array::from_fn(|i| AmbiguityKind::of(&rows[i].ambiguity)),
        cpu_us: std::array::from_fn(|i| rows[i].cpu.as_micros()),
        peak_bytes: std::array::from_fn(|i| rows[i].peak_bytes),
        status: std::array::from_fn(|i| rows[i].status),
        consistency: consistency(&rows),
    }
}

/// When brute is Exact it equals the true aggregate W = Σ_E Σ_m W(m,E). Sparse computes the SAME
/// aggregate (possibly truncated), so `sparse <= brute` must hold. Radix is deliberately NOT checked:
/// `radix_mappings` counts a different object (Σ k×m! output-denomination mappings), not a bound on
/// this W, and can legitimately exceed it. When brute aborted/non-exact, there is no exact reference.
fn consistency(rows: &[CountRow; 4]) -> Consistency {
    if !rows[BRUTE].ambiguity.is_exact() {
        return Consistency::Ok;
    }
    let Some(exact) = rows[BRUTE].ambiguity.lower_bound_count() else {
        return Consistency::Ok;
    };
    match rows[SPARSE].ambiguity.lower_bound_count() {
        Some(v) if v > exact => Consistency::Violated,
        _ => Consistency::Ok,
    }
}

pub(crate) struct CorrectnessRow {
    pub family: &'static str,
    pub n: usize,
    pub regime: String,
    pub radix_dense: bool,
    pub ground_truth: GroundTruth,
    pub sasamoto_err: Option<f64>,
    pub spearman: Option<f64>,
    pub kind: [AmbiguityKind; 4],
    pub cpu_us: [u128; 4],
    pub peak_bytes: [usize; 4],
    pub status: [Status; 4],
    pub consistency: Consistency,
}

fn row_for(rung: &Rung) -> CorrectnessRow {
    let target = rung.set.iter().copied().fold(0u64, u64::saturating_add) / 2;
    let tx = Transaction::new(rung.set.clone(), vec![target.max(1)]);
    let bracket = crate::counts::regime_for_tx(&tx);
    let regime = bracket.map_or_else(|| "(infeasible)".to_string(), |b| b.to_string());
    let radix_dense = dense_subset_sum::radix_density(&rung.set) >= 0.5;
    let (ground_truth, sasamoto_err, spearman) = run_oracle(&rung.set);
    let cost = run_cost(&rung.set, &ladder_limits_for(rung.family, rung.set.len()));
    CorrectnessRow {
        family: rung.family,
        n: rung.set.len(),
        regime,
        radix_dense,
        ground_truth,
        sasamoto_err,
        spearman,
        kind: cost.kind,
        cpu_us: cost.cpu_us,
        peak_bytes: cost.peak_bytes,
        status: cost.status,
        consistency: cost.consistency,
    }
}

pub(crate) fn evaluate_ladders() -> Vec<CorrectnessRow> {
    pow2_ladder()
        .iter()
        .chain(radix_ladder().iter())
        .chain(arbitrary_ladder().iter())
        .chain(random_dense_ladder().iter())
        .map(row_for)
        .collect()
}

fn gt_label(gt: GroundTruth) -> &'static str {
    match gt {
        GroundTruth::Exhaustive => "exhaustive",
        GroundTruth::Dp => "dp",
        GroundTruth::MonteCarlo => "monte_carlo",
        GroundTruth::None => "none",
    }
}

fn kind_label(k: AmbiguityKind) -> &'static str {
    match k {
        AmbiguityKind::Exact => "Exact",
        AmbiguityKind::LowerBound => "LowerBnd",
        AmbiguityKind::LogApprox => "LogApprox",
        AmbiguityKind::Unknown => "Unknown",
    }
}

pub(crate) struct KappaRow {
    pub kappa: f64,
    pub regime: String,
    pub sasamoto_err: Option<f64>,
    pub spearman: Option<f64>,
}

/// At fixed N, sweep the value spread (l_max) from narrow (dense, small κ) to wide (sparse, large κ),
/// measuring Sasamoto error vs the oracle at each κ. Locates the validity boundary empirically
/// (transcript L204). N must be ≤ 22 so the exhaustive `compare` oracle is usable.
pub(crate) fn kappa_sweep(n: usize, seed: u64) -> Vec<KappaRow> {
    use dense_subset_sum::kappa;
    // l_max capped at 256: `compare`'s exact oracle runs a per-target DP whose cost grows with Σa,
    // so wide l_max is intractable. This range spans dense (κ≈0.15) to the boundary onset (κ≈0.40,
    // where Sasamoto error approaches the bound) with exact truth — enough to locate where it degrades.
    [8u64, 16, 32, 64, 128, 256]
        .iter()
        .map(|&l_max| {
            let set = random_dense_set(n, l_max, seed);
            let report = compare(&set, MIN_W, LOOKUP_K, DP_MAX, "kappa-sweep");
            let target = set.iter().copied().fold(0u64, u64::saturating_add) / 2;
            let regime =
                crate::counts::regime_for_tx(&Transaction::new(set.clone(), vec![target.max(1)]))
                    .map_or_else(|| "(infeasible)".to_string(), |b| b.to_string());
            let s = &report.sasamoto;
            KappaRow {
                kappa: kappa(l_max, n).unwrap_or(f64::NAN),
                regime,
                sasamoto_err: (s.n_points > 0).then_some(s.median_error),
                spearman: (s.n_points > 0).then_some(s.spearman),
            }
        })
        .collect()
}

pub(crate) struct SwitchRow {
    pub n: usize,
    pub kappa: f64,
    pub sparse_kind: AmbiguityKind,
    pub sparse_cpu_us: u128,
    pub sparse_peak_bytes: usize,
    pub sasamoto_err: Option<f64>,
    pub spearman: Option<f64>,
}

/// Empirically locate the sparse→Sasamoto switch instead of hard-coding a cap. At a fixed dense
/// value spread, sweep the problem size N and measure, per N, whether the exact sparse convolution
/// still fits the memory budget (Exact) or has outgrown it (LowerBound) plus its CPU, alongside the
/// Sasamoto approximation's error vs the oracle. The switch is the N where sparse stops being Exact;
/// the sweep confirms Sasamoto remains a good estimate (low error) there — the transcript's "find
/// where [sparse] becomes too computationally costly and make sure that at that point the Sasamoto
/// estimate is a good estimate" (L1163), measured rather than assumed. `budget` is the sparse memory
/// budget the switch is measured against — the real knob a fixed output cap only stands in for.
/// (Peak RAM is recorded only under the `alloc-probe` feature; CPU and kind are always measured.)
pub(crate) fn switch_sweep(l_max: u64, seed: u64, budget: usize) -> Vec<SwitchRow> {
    use dense_subset_sum::kappa;
    (8..=22usize)
        .step_by(2)
        .map(|n| {
            let limits = Limits {
                sparse_mem_budget: NonZeroUsize::new(budget).expect("budget > 0"),
                ..ladder_limits(n)
            };
            let set = random_dense_set(n, l_max, seed + n as u64);
            let cost = run_cost(&set, &limits);
            let report = compare(&set, MIN_W, LOOKUP_K, DP_MAX, "switch-sweep");
            let s = &report.sasamoto;
            SwitchRow {
                n,
                kappa: kappa(l_max, n).unwrap_or(f64::NAN),
                sparse_kind: cost.kind[SPARSE],
                sparse_cpu_us: cost.cpu_us[SPARSE],
                sparse_peak_bytes: cost.peak_bytes[SPARSE],
                sasamoto_err: (s.n_points > 0).then_some(s.median_error),
                spearman: (s.n_points > 0).then_some(s.spearman),
            }
        })
        .collect()
}

/// CJA comparison (transcript L1405: "testing ... with CJA"): for small coinjoins, compares the W /
/// Sasamoto lower-bound side against the enumerated sub-transaction MAPPINGS side (Maurer/Boltzmann
/// entropy). Both are anonymity facets (§2.7: "everything is anonymity") — this surfaces them side by
/// side. Mapping enumeration is exponential, so only small instances (≤ max_coins) yield a comparison.
pub(crate) fn cja_comparison() -> Vec<MappingComparison> {
    let knee = dense_subset_sum::KNEE;
    let max_coins = 20;
    let txs = [
        dense_subset_sum::fixtures::maurer_fig2(),
        Transaction::new(vec![512, 512, 512, 1024], vec![512, 512, 1024, 512]),
        Transaction::new(vec![1000, 1000, 2000], vec![1000, 1000, 2000]),
    ];
    txs.iter()
        .enumerate()
        .filter_map(|(i, tx)| compare_w_vs_mappings(tx, &format!("cja_{i}"), knee, max_coins))
        .collect()
}

pub fn print_correctness_report() {
    let rows = evaluate_ladders();
    println!(
        "(sparse_mem_budget={} entries, dp_max={} cells, radix_max_size={})",
        SPARSE_MEM_BUDGET, DP_MAX, RADIX_MAX_SIZE
    );
    println!(
        "family | N | ground_truth | sparse_kind | sasamoto_err% | spearman | brute_us | radix_us | sparse_us | sasa_us | peakB | lb_ok | aborts | radix_dense | regime"
    );
    for r in &rows {
        let err = r
            .sasamoto_err
            .map_or_else(|| "-".to_string(), |e| format!("{:.1}", e * 100.0));
        let sp = r
            .spearman
            .map_or_else(|| "-".to_string(), |s| format!("{:.3}", s));
        let aborts: String = [BRUTE, RADIX, SPARSE, SASAMOTO]
            .iter()
            .filter(|&&i| matches!(r.status[i], Status::Aborted(_)))
            .map(|&i| ["b", "r", "s", "z"][i])
            .collect::<Vec<_>>()
            .join(",");
        let aborts = if aborts.is_empty() {
            "-".to_string()
        } else {
            aborts
        };
        let lb_ok = match r.consistency {
            Consistency::Ok => "ok",
            Consistency::Violated => "BAD",
        };
        println!(
            "{:>9} | {:>2} | {:>11} | {:>8} | {:>12} | {:>8} | {:>8} | {:>8} | {:>9} | {:>7} | {:>9} | {:>5} | {:>8} | {:>11} | {}",
            r.family, r.n, gt_label(r.ground_truth), kind_label(r.kind[SPARSE]), err, sp,
            r.cpu_us[BRUTE], r.cpu_us[RADIX], r.cpu_us[SPARSE], r.cpu_us[SASAMOTO],
            r.peak_bytes[SPARSE], lb_ok, aborts, if r.radix_dense { "yes" } else { "no" }, r.regime,
        );
    }

    println!("\nkappa sweep @ N=20 (Sasamoto error vs oracle as density falls):");
    println!("kappa | regime | sasamoto_err% | spearman");
    for r in kappa_sweep(20, RANDOM_DENSE_SEED) {
        let err = r
            .sasamoto_err
            .map_or_else(|| "-".to_string(), |e| format!("{:.1}", e * 100.0));
        let sp = r
            .spearman
            .map_or_else(|| "-".to_string(), |s| format!("{:.3}", s));
        println!(
            "{:>5.2} | {:>11} | {:>12} | {:>8}",
            r.kappa, r.regime, err, sp
        );
    }

    println!("\nswitch sweep @ l_max=16, sparse budget=512 (where sparse stops being exact, Sasamoto takes over):");
    println!("  N | kappa | sparse_kind | sparse_us | sparse_peakB | sasamoto_err% | spearman");
    for r in switch_sweep(16, RANDOM_DENSE_SEED, 512) {
        let err = r
            .sasamoto_err
            .map_or_else(|| "-".to_string(), |e| format!("{:.1}", e * 100.0));
        let sp = r
            .spearman
            .map_or_else(|| "-".to_string(), |s| format!("{:.3}", s));
        println!(
            "{:>3} | {:>5.2} | {:>11} | {:>9} | {:>12} | {:>13} | {:>8}",
            r.n,
            r.kappa,
            kind_label(r.sparse_kind),
            r.sparse_cpu_us,
            r.sparse_peak_bytes,
            err,
            sp,
        );
    }

    println!("\nCJA: W vs sub-transaction mappings (both anonymity facets) on small coinjoins:");
    println!("label | n_in | n_out | non_derived | entropy_bits | sasamoto_log | w_lookup_log");
    for c in cja_comparison() {
        println!(
            "{:>6} | {:>4} | {:>5} | {:>11} | {:>12.2} | {:>12.2} | {:>12.2}",
            c.label,
            c.n_inputs,
            c.n_outputs,
            c.n_non_derived,
            c.entropy,
            c.max_log_sasamoto_approx,
            c.max_log_w_lookup,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::OnceLock;

    fn ladder_rows() -> &'static Vec<CorrectnessRow> {
        static ROWS: OnceLock<Vec<CorrectnessRow>> = OnceLock::new();
        ROWS.get_or_init(evaluate_ladders)
    }

    #[test]
    fn pow2_set_is_ascending_with_right_multiplicity() {
        let s = pow2_set(3, 3);
        assert_eq!(s, vec![2, 2, 2, 4, 4, 4, 8, 8, 8]);
        assert!(s.windows(2).all(|w| w[0] <= w[1]), "must be ascending");
    }

    use crate::counts::{four_counts, Limits, Status};
    use dense_subset_sum::harness::vs_oracle::compare_dp_ground_truth;
    use dense_subset_sum::is_standard_denom;
    use dense_subset_sum::{Ambiguity, Transaction};

    #[test]
    fn sasamoto_sharpens_toward_dense() {
        let rows = kappa_sweep(20, 7);
        assert!(rows.len() >= 4, "need several kappa points");
        assert!(
            rows.windows(2).all(|w| w[0].kappa <= w[1].kappa),
            "kappa ascends (dense -> sparse)"
        );
        let dense_err = rows.first().unwrap().sasamoto_err;
        assert!(
            dense_err.is_some_and(|e| e < SASA_ERR_BOUND),
            "densest point: Sasamoto good (< bound)"
        );
        // the densest point must not be worse than the sparsest measured point
        let sparse_err = rows.iter().rev().find_map(|r| r.sasamoto_err);
        if let (Some(d), Some(s)) = (dense_err, sparse_err) {
            assert!(d <= s, "Sasamoto error should not be worse at the dense end ({d:.3}) than the sparse end ({s:.3})");
        }
    }

    #[test]
    fn random_dense_set_is_deterministic_and_in_range() {
        let a = random_dense_set(20, 64, 7);
        let b = random_dense_set(20, 64, 7);
        assert_eq!(a, b, "same seed => same set");
        assert_eq!(a.len(), 20);
        assert!(a.iter().all(|&v| (1..=64).contains(&v)));
        assert!(a.windows(2).all(|w| w[0] <= w[1]));
    }

    #[test]
    fn cja_comparison_relates_w_and_mappings() {
        let comps = cja_comparison();
        assert!(
            !comps.is_empty(),
            "CJA comparison must produce at least one small-tx result"
        );
        // mappings side: at least one tx has sub-transaction mappings (anonymity via Maurer/Boltzmann)
        assert!(
            comps.iter().any(|c| c.n_non_derived > 0),
            "a tx must have sub-transaction mappings"
        );
        // both facets present: mapping entropy (bits) is finite where mappings exist
        assert!(
            comps
                .iter()
                .filter(|c| c.n_non_derived > 0)
                .all(|c| c.entropy.is_finite()),
            "mapping entropy must be finite where mappings exist"
        );
    }

    // cargo test --lib correctness::tests::calibrate_random_dense -- --ignored --nocapture
    #[test]
    #[ignore]
    fn calibrate_random_dense() {
        use crate::counts::{four_counts, Limits, Status};
        use dense_subset_sum::harness::vs_oracle::{compare, compare_dp_ground_truth};
        use dense_subset_sum::{kappa, Ambiguity, Transaction};
        let l_max = 16u64;
        let sparse_budget = 512usize;
        let dp_max = 4_000_000usize;
        println!("\nN | kappa | sparse_kind | dp | sasa_err% | brute");
        for n in (8..=24usize).step_by(2) {
            let set = random_dense_set(n, l_max, 100 + n as u64);
            let target = set.iter().copied().fold(0u64, u64::saturating_add) / 2;
            let tx = Transaction::new(set.clone(), vec![target.max(1)]);
            let limits = Limits {
                brute_max_terms: 15,
                max_size: n,
                radix_max_size: Some(6),
                sparse_mem_budget: NonZeroUsize::new(sparse_budget).unwrap(),
                subsum_max_outputs: 20,
            };
            let rows = four_counts(&tx, &limits);
            let sk = match rows[2].ambiguity {
                Ambiguity::Exact(_) => "Exact",
                Ambiguity::LowerBound(_) => "LowerBound",
                _ => "other",
            };
            let (dp, err) = if n <= 22 {
                let r = compare(&set, 2, 6, dp_max, "cal");
                (
                    "exh",
                    if r.sasamoto.n_points > 0 {
                        r.sasamoto.median_error * 100.0
                    } else {
                        -1.0
                    },
                )
            } else {
                match compare_dp_ground_truth(&set, 2, 6, dp_max, "cal") {
                    Ok(r) => (
                        "dp",
                        if r.sasamoto.n_points > 0 {
                            r.sasamoto.median_error * 100.0
                        } else {
                            -1.0
                        },
                    ),
                    Err(_) => ("ERR", -1.0),
                }
            };
            let brute = if matches!(rows[0].status, Status::Aborted(_)) {
                "abort"
            } else {
                "ok"
            };
            let k = kappa(l_max, n).unwrap_or(f64::NAN);
            println!("{n:>2} | {k:>5.2} | {sk:>10} | {dp:>3} | {err:>6.1} | {brute}");
        }
    }

    // cargo test --lib correctness::tests::switch_sweep_locates_handoff -- --nocapture
    #[test]
    fn switch_sweep_locates_handoff() {
        // Dense spread (l_max=16) so Sasamoto is in its valid regime; small budget so the sparse
        // exact→degraded switch falls within the N range.
        let rows = switch_sweep(16, RANDOM_DENSE_SEED, 512);
        println!("\n  N | kappa | sparse_kind | sparse_us | sparse_peakB | sasa_err% | spearman");
        for r in &rows {
            println!(
                "{:>3} | {:>5.2} | {:>11} | {:>9} | {:>12} | {:>9} | {:>8}",
                r.n,
                r.kappa,
                kind_label(r.sparse_kind),
                r.sparse_cpu_us,
                r.sparse_peak_bytes,
                r.sasamoto_err
                    .map(|e| format!("{:.1}", e * 100.0))
                    .unwrap_or_else(|| "-".into()),
                r.spearman
                    .map(|s| format!("{s:.3}"))
                    .unwrap_or_else(|| "-".into()),
            );
        }
        // The sweep must straddle the switch: sparse exact at small N, degraded (budget exceeded)
        // at large N — i.e. the handoff point is located by measurement, not assumed.
        assert!(
            rows.iter().any(|r| r.sparse_kind == AmbiguityKind::Exact),
            "expected small-N rungs where sparse is still Exact"
        );
        assert!(
            rows.iter()
                .any(|r| r.sparse_kind == AmbiguityKind::LowerBound),
            "expected large-N rungs where sparse degrades (the switch to relying on Sasamoto)"
        );
        // At the switch, Sasamoto must still be a good estimate (else the handoff is unsafe).
        let degraded_err = rows
            .iter()
            .filter(|r| r.sparse_kind == AmbiguityKind::LowerBound)
            .filter_map(|r| r.sasamoto_err)
            .fold(0.0_f64, f64::max);
        assert!(
            degraded_err < SASA_ERR_BOUND,
            "Sasamoto error {:.1}% at/after the switch exceeds the {:.0}% bound",
            degraded_err * 100.0,
            SASA_ERR_BOUND * 100.0
        );
    }

    #[test]
    fn ambiguity_kind_projects_all_variants() {
        assert_eq!(
            AmbiguityKind::of(&Ambiguity::Exact(3)),
            AmbiguityKind::Exact
        );
        assert_eq!(
            AmbiguityKind::of(&Ambiguity::LowerBound(3)),
            AmbiguityKind::LowerBound
        );
        assert_eq!(
            AmbiguityKind::of(&Ambiguity::LogApprox(1.0)),
            AmbiguityKind::LogApprox
        );
        assert_eq!(
            AmbiguityKind::of(&Ambiguity::Unknown),
            AmbiguityKind::Unknown
        );
    }

    #[test]
    fn run_cost_extracts_kinds_and_consistency_for_small_set() {
        // N=9: brute is exact => no lower bound may exceed it.
        let cost = run_cost(&pow2_set(3, 3), &ladder_limits(9));
        assert_eq!(cost.kind.len(), 4);
        assert_eq!(
            cost.consistency,
            Consistency::Ok,
            "no lower bound may exceed the exact brute aggregate"
        );
    }

    #[test]
    fn random_dense_ladder_is_dense_and_grows() {
        let rungs = random_dense_ladder();
        assert!(rungs.len() >= 4);
        assert!(rungs.iter().all(|r| r.family == "random_dense"));
        let ns: Vec<usize> = rungs.iter().map(|r| r.set.len()).collect();
        assert!(
            ns.windows(2).all(|w| w[0] <= w[1]),
            "N grows along the ladder"
        );
    }

    #[test]
    fn radix_ladder_values_are_standard_denoms() {
        let rungs = radix_ladder();
        assert!(!rungs.is_empty());
        for r in &rungs {
            assert_eq!(r.family, "radix");
            assert!(
                r.set.iter().all(|&v| is_standard_denom(v)),
                "radix ladder must use standard denoms"
            );
        }
    }

    #[test]
    fn arbitrary_ladder_values_are_not_standard_denoms() {
        let rungs = arbitrary_ladder();
        assert!(!rungs.is_empty());
        for r in &rungs {
            assert_eq!(r.family, "arbitrary");
            assert!(
                r.set.iter().all(|&v| !is_standard_denom(v)),
                "arbitrary ladder must avoid denoms"
            );
        }
    }

    #[test]
    fn run_oracle_exhaustive_for_small_dense_set() {
        // small pow2 set: N<=22 => Exhaustive ground truth, Sasamoto measured in the dense regime
        let (gt, err, spearman) = run_oracle(&pow2_set(4, 3)); // N=12
        assert!(matches!(gt, GroundTruth::Exhaustive));
        assert!(
            err.is_some(),
            "exact ground truth must yield a Sasamoto error"
        );
        assert!(spearman.is_some());
        assert!(err.unwrap() >= 0.0);
    }

    #[test]
    fn pow2_ladder_is_dense_by_construction() {
        let rungs = pow2_ladder();
        assert!(rungs.len() >= 4, "need several rungs to span the crossover");
        assert!(rungs.iter().all(|r| r.family == "pow2"));
    }

    #[test]
    fn evaluate_ladders_covers_all_three_families() {
        let rows = ladder_rows();
        for fam in ["pow2", "radix", "arbitrary"] {
            assert!(rows.iter().any(|r| r.family == fam), "missing family {fam}");
        }
        assert!(rows.iter().all(|r| r.n > 0 && !r.regime.is_empty()));
    }

    #[test]
    fn correctness_invariants_hold() {
        let rows = ladder_rows();

        for r in rows.iter() {
            assert_eq!(
                r.consistency,
                Consistency::Ok,
                "lower-bound monotonicity must hold for {} N={}",
                r.family,
                r.n
            );
            // Hard Sasamoto bounds only on random_dense (L225-faithful: randomly sampled, dense) AND
            // only on the rungs where Sasamoto is actually USED — where sparse has truncated
            // (kind[SPARSE] == LowerBound) and exact truth still exists. Calibration showed Sasamoto
            // is only accurate once N is large/dense enough; the small-N rungs (sparse still Exact,
            // so Sasamoto is not needed) are noisy and not asserted. pow2 is a radix-density
            // illustration only — "powers of 2 dense by construction" (L1158) is the RADIX rationale,
            // not Sasamoto's.
            let used_here = r.family == "random_dense"
                && r.kind[SPARSE] == AmbiguityKind::LowerBound
                && matches!(r.ground_truth, GroundTruth::Exhaustive | GroundTruth::Dp);
            if used_here {
                let err = r
                    .sasamoto_err
                    .expect("overlap rung must have a Sasamoto error");
                assert!(
                    err < SASA_ERR_BOUND,
                    "Sasamoto error {:.3} must stay < {:.2} at the crossover ({} N={})",
                    err,
                    SASA_ERR_BOUND,
                    r.family,
                    r.n
                );
                let sp = r.spearman.expect("overlap rung must have spearman");
                assert!(
                    sp > 0.90,
                    "Sasamoto spearman {:.3} must stay > 0.9 at the crossover ({} N={})",
                    sp,
                    r.family,
                    r.n
                );
            }
        }

        // overlap band answering L1163, now on the faithful family:
        assert!(
            rows.iter().any(|r| r.family == "random_dense"
                && r.kind[SPARSE] == AmbiguityKind::LowerBound
                && matches!(r.ground_truth, GroundTruth::Exhaustive | GroundTruth::Dp)
                && r.sasamoto_err.is_some()),
            "random_dense ladder must have an overlap rung (sparse=LowerBound AND exact truth AND Sasamoto measured); \
             re-calibrate RANDOM_DENSE params (Task 4) if this fails"
        );
    }

    // Run with: cargo test --lib correctness::tests::calibrate_overlap -- --ignored --nocapture
    #[test]
    #[ignore]
    fn calibrate_overlap() {
        const MULT: usize = 3;
        const RADIX_MAX_SIZE: usize = 6;
        let candidate_sparse_budget = 4096usize;
        let candidate_dp_max = 4_000_000usize;
        println!("\nN | sparse_kind | dp_ground_truth | brute_status");
        for n_powers in 3..=14u32 {
            let set = pow2_set(n_powers, MULT);
            let n = set.len();
            let target = set.iter().copied().fold(0u64, u64::saturating_add) / 2;
            let tx = Transaction::new(set.clone(), vec![target.max(1)]);
            let limits = Limits {
                brute_max_terms: 15,
                max_size: n,
                radix_max_size: Some(RADIX_MAX_SIZE),
                sparse_mem_budget: NonZeroUsize::new(candidate_sparse_budget).unwrap(),
                subsum_max_outputs: 20,
            };
            let rows = four_counts(&tx, &limits);
            let sparse_kind = match rows[2].ambiguity {
                Ambiguity::Exact(_) => "Exact",
                Ambiguity::LowerBound(_) => "LowerBound",
                _ => "other",
            };
            let dp = match compare_dp_ground_truth(&set, 2, RADIX_MAX_SIZE, candidate_dp_max, "cal")
            {
                Ok(_) => "Ok",
                Err(_) => "Err",
            };
            let brute = if matches!(rows[0].status, Status::Aborted(_)) {
                "aborted"
            } else {
                "computed"
            };
            println!("{:>2} | {:>10} | {:>3} | {}", n, sparse_kind, dp, brute);
        }
    }
}
