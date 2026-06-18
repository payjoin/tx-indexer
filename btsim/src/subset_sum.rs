//! Feasibility guards shared by the cost metric and the measurement harness.

/// Sparse and Sasamoto enumerate all `2^#outputs` output subset sums (the dep
/// allocates `HashSet::with_capacity(1<<n)`), so without this cap a real coinjoin
/// (30-100+ outputs) would OOM. `2^20` subsums ≈ tens of MB.
pub(crate) const SUBSUM_MAX_OUTPUTS: usize = 20;

pub(crate) fn brute_feasible(n_inputs: usize, n_outputs: usize, max_terms: usize) -> bool {
    n_inputs + n_outputs <= max_terms
}

pub(crate) fn subsum_feasible(n_outputs: usize, max_outputs: usize) -> bool {
    n_outputs <= max_outputs
}
