use bitcoin::Amount;

use crate::transaction::Outpoint;

/// A single registration event in a multi-party session.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum StepAction {
    RegisterInput(Outpoint),
    RegisterOutput(Amount),
}

/// One node in the plan tree, representing a step and its continuations.
#[derive(Debug)]
pub(crate) struct PlanNode {
    pub(crate) action: StepAction,
    pub(crate) children: Vec<PlanNode>,
}

/// The full tree of viable contribution sequences for a session, with a cursor
/// tracking which actions have already been committed.
#[derive(Debug)]
pub(crate) struct PlanTree {
    /// One subtree per input candidate (children of the sentinel root).
    pub(crate) roots: Vec<PlanNode>,
    /// Indices into each level's children, starting from roots.
    cursor_path: Vec<usize>,
}

/// Standard denomination amounts used when decomposing change.
#[derive(Debug, Clone)]
pub(crate) struct DenominationMenu {
    /// Sorted descending.
    pub(crate) denominations: Vec<Amount>,
}

/// Placeholder leaf score — full cost function is deferred.
#[derive(Debug, Clone, Copy)]
pub(crate) struct LeafScore(pub(crate) f64);

/// Observed registrations from other session participants.
#[derive(Debug, Default)]
pub(crate) struct PeerState {
    pub(crate) their_inputs: Vec<(Outpoint, Amount)>,
    pub(crate) their_outputs: Vec<Amount>,
}

#[derive(Debug)]
pub(crate) enum CommitError {
    ActionNotFound,
    AlreadyAtLeaf,
}

impl PlanTree {
    pub(crate) fn new(roots: Vec<PlanNode>) -> Self {
        Self { roots, cursor_path: vec![] }
    }

    pub(crate) fn next_actions(&self) -> Vec<&StepAction> {
        todo!()
    }

    pub(crate) fn reachable_leaves(&self) -> Vec<Vec<&StepAction>> {
        todo!()
    }

    pub(crate) fn commit(&mut self, _action: &StepAction) -> Result<(), CommitError> {
        todo!()
    }

    pub(crate) fn score_leaves(&self, _peer: &PeerState) -> Vec<(Vec<&StepAction>, LeafScore)> {
        todo!()
    }
}

impl DenominationMenu {
    /// Powers of two and powers of ten in satoshis, deduplicated and sorted descending,
    /// covering 1_000 sat to 100_000_000 sat (1 BTC).
    pub(crate) fn standard() -> Self {
        let mut denoms: Vec<u64> = Vec::new();

        // Powers of two: 2^10 (1024) through 2^26 (67_108_864)
        let mut p2: u64 = 1 << 10;
        while p2 <= 100_000_000 {
            denoms.push(p2);
            p2 *= 2;
        }

        // Powers of ten: 10^3 (1_000) through 10^8 (100_000_000)
        let mut p10: u64 = 1_000;
        while p10 <= 100_000_000 {
            denoms.push(p10);
            p10 *= 10;
        }

        denoms.sort_unstable();
        denoms.dedup();
        denoms.reverse();

        Self {
            denominations: denoms.into_iter().map(Amount::from_sat).collect(),
        }
    }

    fn min(&self) -> Option<Amount> {
        self.denominations.last().copied()
    }
}

/// Recursively decomposes `remaining` into a set of `RegisterOutput` nodes branching on each
/// denomination in `menu`. Every root-to-leaf path through the result sums to exactly
/// `remaining` (or to `remaining` rounded down to the nearest denomination, with the residual
/// emitted as a single unstructured change leaf when `remaining < min(menu)`).
pub(crate) fn decompose(remaining: Amount, menu: &DenominationMenu) -> Vec<PlanNode> {
    if remaining == Amount::ZERO {
        return vec![];
    }

    // If remaining is below the smallest denomination, emit it as a single residual leaf.
    if menu.min().map_or(true, |min| remaining < min) {
        return vec![PlanNode {
            action: StepAction::RegisterOutput(remaining),
            children: vec![],
        }];
    }

    menu.denominations
        .iter()
        .copied()
        .filter(|&d| d <= remaining)
        .map(|d| PlanNode {
            action: StepAction::RegisterOutput(d),
            children: decompose(remaining - d, menu),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn leaf_paths(nodes: &[PlanNode], prefix: Vec<Amount>) -> Vec<Vec<Amount>> {
        let mut paths = Vec::new();
        for node in nodes {
            let StepAction::RegisterOutput(amt) = node.action else { continue };
            let mut path = prefix.clone();
            path.push(amt);
            if node.children.is_empty() {
                paths.push(path);
            } else {
                paths.extend(leaf_paths(&node.children, path));
            }
        }
        paths
    }

    #[test]
    fn decompose_paths_sum_to_remaining() {
        let menu = DenominationMenu::standard();
        let remaining = Amount::from_sat(15_000);
        let nodes = decompose(remaining, &menu);
        assert!(!nodes.is_empty(), "expected at least one branch");
        for path in leaf_paths(&nodes, vec![]) {
            let total: Amount = path.iter().copied().sum();
            assert_eq!(total, remaining, "path {:?} does not sum to {remaining}", path);
        }
    }

    #[test]
    fn decompose_zero_returns_empty() {
        let menu = DenominationMenu::standard();
        assert!(decompose(Amount::ZERO, &menu).is_empty());
    }

    #[test]
    fn decompose_residual_below_min_denom() {
        let menu = DenominationMenu::standard();
        // 999 sat is below the 1_000 sat minimum denomination
        let nodes = decompose(Amount::from_sat(999), &menu);
        assert_eq!(nodes.len(), 1);
        assert!(nodes[0].children.is_empty());
        assert_eq!(nodes[0].action, StepAction::RegisterOutput(Amount::from_sat(999)));
    }

    #[test]
    fn standard_menu_is_sorted_descending() {
        let menu = DenominationMenu::standard();
        assert!(!menu.denominations.is_empty());
        for window in menu.denominations.windows(2) {
            assert!(window[0] >= window[1], "denominations not sorted descending");
        }
    }
}

pub(crate) fn build_plan_tree(
    _input_candidates: Vec<Vec<Outpoint>>,
    _payment_amounts: Vec<Amount>,
    _input_amounts: impl Fn(&Outpoint) -> Amount,
    _menu: &DenominationMenu,
    _fee: Amount,
) -> PlanTree {
    todo!()
}
