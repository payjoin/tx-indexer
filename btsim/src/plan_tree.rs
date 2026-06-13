use bdk_coin_select::Target;
use bitcoin::Amount;

use crate::{
    actions::{CompositeScorer, CostMode, Plan, WalletResidue},
    coin_selection::{select_all, select_bnb, CoinCandidate},
    cospend::UtxoWithMetadata,
    transaction::Outpoint,
    wallet::{PaymentObligationData, WalletHandle},
};

/// A single registration event in a multi-party session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StepAction {
    RegisterInput(Outpoint),
    RegisterOutput(Amount),
}

/// One node in the plan tree, representing a step and its continuations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlanNode {
    pub(crate) action: StepAction,
    pub(crate) children: Vec<PlanNode>,
}

/// The full tree of viable contribution sequences for a session, with a cursor
/// tracking which actions have already been committed.
///
/// At depth 0 the cursor is at the phantom root; `next_actions` returns all root-level
/// actions. Each `commit` prunes siblings and advances depth by 1.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlanTree {
    /// One subtree per input candidate (children of the sentinel root).
    pub(crate) roots: Vec<PlanNode>,
    /// Number of commits made. After each commit, siblings are pruned so the path
    /// is always `roots[0].children[0]...` — no index array needed.
    pub(crate) depth: usize,
    /// Number of `RegisterInput` nodes at the top of each root-to-leaf path.
    pub(crate) n_inputs: usize,
    /// Number of `RegisterOutput` nodes following inputs that represent payment obligations
    /// (not change). Used to separate payment outputs from change outputs positionally.
    pub(crate) n_payment_outputs: usize,
}

/// Standard denomination amounts used when decomposing change.
#[derive(Debug, Clone)]
pub(crate) struct DenominationMenu {
    /// Sorted descending.
    pub(crate) denominations: Vec<Amount>,
}

/// Scored cost for a leaf path.
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
    pub(crate) fn new(roots: Vec<PlanNode>, n_inputs: usize, n_payment_outputs: usize) -> Self {
        Self {
            roots,
            depth: 0,
            n_inputs,
            n_payment_outputs,
        }
    }

    /// Current live choices: roots before first commit, otherwise the children of the
    /// committed node at depth.
    pub(crate) fn next_actions(&self) -> Vec<&StepAction> {
        self.current_level().iter().map(|n| &n.action).collect()
    }

    /// All root-to-leaf action paths reachable from the current cursor position.
    pub(crate) fn reachable_leaves(&self) -> Vec<Vec<&StepAction>> {
        let mut leaves = Vec::new();
        for node in self.current_level() {
            collect_leaves(node, &mut vec![], &mut leaves);
        }
        leaves
    }

    /// Advance the cursor to the choice matching `action`, pruning all siblings.
    pub(crate) fn commit(&mut self, action: &StepAction) -> Result<(), CommitError> {
        let choices = self.current_level_mut();

        if choices.is_empty() {
            return Err(CommitError::AlreadyAtLeaf);
        }

        let idx = choices
            .iter()
            .position(|n| &n.action == action)
            .ok_or(CommitError::ActionNotFound)?;

        let chosen = choices.swap_remove(idx);
        choices.clear();
        choices.push(chosen);

        self.depth += 1;
        Ok(())
    }

    /// Outpoints appearing in every reachable leaf path — safe to advertise unconditionally.
    pub(crate) fn committed_inputs(&self) -> Vec<Outpoint> {
        use std::collections::HashSet;
        let leaves = self.reachable_leaves();
        if leaves.is_empty() {
            return vec![];
        }
        let extract = |leaf: &Vec<&StepAction>| -> HashSet<Outpoint> {
            leaf.iter()
                .filter_map(|a| {
                    if let StepAction::RegisterInput(op) = a {
                        Some(*op)
                    } else {
                        None
                    }
                })
                .collect()
        };
        let mut common = extract(&leaves[0]);
        for leaf in &leaves[1..] {
            let s = extract(leaf);
            common = common.intersection(&s).copied().collect();
        }
        common.into_iter().collect()
    }

    /// Re-scores all reachable leaves. Use `EXTERNAL_PENALTIES_OFF` before peer info is
    /// available, `EXTERNAL_PENALTIES_ON` once the bulletin board has peer outputs.
    ///
    /// Each returned path is a full root-to-leaf path: the already-committed prefix is
    /// prepended to each reachable tail so that a mid-session rescore still accounts for
    /// this wallet's own committed inputs, not just the remaining change choices.
    pub(crate) fn score_leaves(
        &self,
        peer: &PeerState,
        scorer: &CompositeScorer,
        wallet: &WalletHandle,
        mode: CostMode,
    ) -> Vec<(Vec<&StepAction>, LeafScore)> {
        let prefix = self.committed_path();
        self.reachable_leaves()
            .into_iter()
            .map(|tail| {
                let path: Vec<&StepAction> = prefix.iter().copied().chain(tail).collect();
                let plan = plan_from_path(&path, peer, wallet);
                let cost = scorer.score(&plan, wallet, mode);
                (path, LeafScore(cost.0))
            })
            .collect()
    }

    /// The actions already committed: the single-node chain from the root up to `depth`.
    /// Empty before the first commit.
    fn committed_path(&self) -> Vec<&StepAction> {
        let mut path = Vec::with_capacity(self.depth);
        let mut level: &[PlanNode] = &self.roots;
        for _ in 0..self.depth {
            // Invariant: after each commit siblings are pruned to exactly one node.
            let node = &level[0];
            path.push(&node.action);
            level = &node.children;
        }
        path
    }

    fn current_level(&self) -> &[PlanNode] {
        // SAFETY: after each commit siblings are pruned to exactly one element.
        // The path roots[0].children[0]... is valid up to `depth`.
        unsafe {
            let mut ptr: *const Vec<PlanNode> = &self.roots;
            for _ in 0..self.depth {
                ptr = &(&(*ptr))[0].children;
            }
            &*ptr
        }
    }

    fn current_level_mut(&mut self) -> &mut Vec<PlanNode> {
        // SAFETY: same invariant as current_level.
        unsafe {
            let mut ptr: *mut Vec<PlanNode> = &mut self.roots;
            for _ in 0..self.depth {
                ptr = &mut (&mut (*ptr))[0].children;
            }
            &mut *ptr
        }
    }
}

fn collect_leaves<'a>(
    node: &'a PlanNode,
    path: &mut Vec<&'a StepAction>,
    out: &mut Vec<Vec<&'a StepAction>>,
) {
    path.push(&node.action);
    if node.children.is_empty() {
        out.push(path.clone());
    } else {
        for child in &node.children {
            collect_leaves(child, path, out);
        }
    }
    path.pop();
}

impl DenominationMenu {
    /// Powers of two (1_024–8_192) and powers of ten (1_000–10_000), deduped descending.
    /// Capped at 10_000 sat — change outputs above this are emitted as a single residual.
    pub(crate) fn standard() -> Self {
        let mut denoms: Vec<u64> = Vec::new();

        let mut p2: u64 = 1 << 10;
        while p2 <= 10_000 {
            denoms.push(p2);
            p2 *= 2;
        }

        let mut p10: u64 = 1_000;
        while p10 <= 10_000 {
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

    pub(crate) fn min(&self) -> Option<Amount> {
        self.denominations.last().copied()
    }
}

/// Maximum number of change outputs a single decomposition path may produce.
const MAX_DECOMPOSE_DEPTH: usize = 4;

/// Recursively decomposes `remaining` into branching `RegisterOutput` nodes. Every
/// root-to-leaf path sums to exactly `remaining`. At the depth cap or below the
/// smallest denomination, a single residual leaf is emitted.
pub(crate) fn decompose(remaining: Amount, menu: &DenominationMenu) -> Vec<PlanNode> {
    decompose_inner(remaining, menu, 0)
}

fn decompose_inner(remaining: Amount, menu: &DenominationMenu, depth: usize) -> Vec<PlanNode> {
    if remaining == Amount::ZERO {
        return vec![];
    }

    if depth >= MAX_DECOMPOSE_DEPTH || menu.min().map_or(true, |min| remaining < min) {
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
            children: decompose_inner(remaining - d, menu, depth + 1),
        })
        .collect()
}

/// Generates up to `k` disjoint input sets via iterative BNB exclusion.
/// Falls back to spend-all if BNB never finds a solution.
pub(crate) fn generate_input_candidates(
    candidates: &[CoinCandidate],
    target: Target,
    k: usize,
) -> Vec<Vec<Outpoint>> {
    let mut results: Vec<Vec<Outpoint>> = Vec::new();
    let mut excluded: std::collections::HashSet<Outpoint> = std::collections::HashSet::new();

    while results.len() < k {
        let remaining: Vec<&CoinCandidate> = candidates
            .iter()
            .filter(|c| !excluded.contains(&c.outpoint))
            .collect();

        if remaining.is_empty() {
            break;
        }

        let owned: Vec<CoinCandidate> = remaining
            .iter()
            .map(|c| CoinCandidate {
                outpoint: c.outpoint,
                amount_sats: c.amount_sats,
                weight_wu: c.weight_wu,
                is_segwit: c.is_segwit,
            })
            .collect();

        if let Some((selected, _)) = select_bnb(&owned, target) {
            for op in &selected {
                excluded.insert(*op);
            }
            results.push(selected);
        } else {
            break;
        }
    }

    if results.is_empty() {
        let (selected, _) = select_all(candidates, target);
        if !selected.is_empty() {
            results.push(selected);
        }
    }

    results
}

/// Assembles a `PlanTree` from pre-computed input candidates.
///
/// For each input set: sorts inputs by denomination fit, builds a linear
/// `RegisterInput` chain, appends fixed payment outputs, then attaches the
/// branching change decomposition.
pub(crate) fn build_plan_tree(
    input_candidates: Vec<Vec<Outpoint>>,
    payment_amounts: Vec<Amount>,
    input_amounts: impl Fn(&Outpoint) -> Amount,
    menu: &DenominationMenu,
    fee: Amount,
) -> PlanTree {
    let n_payment_outputs = payment_amounts.len();
    let payment_total: Amount = payment_amounts.iter().copied().sum();

    let roots = input_candidates
        .into_iter()
        .map(|mut outpoints| {
            outpoints.sort_by_key(|op| denomination_fit(input_amounts(op), menu));

            let total_in: Amount = outpoints.iter().map(|op| input_amounts(op)).sum();
            let remaining = total_in
                .checked_sub(payment_total + fee)
                .unwrap_or(Amount::ZERO);

            let mut output_children = decompose(remaining, menu);
            for &amt in payment_amounts.iter().rev() {
                output_children = vec![PlanNode {
                    action: StepAction::RegisterOutput(amt),
                    children: output_children,
                }];
            }

            let mut node_children = output_children;
            for op in outpoints.into_iter().rev() {
                node_children = vec![PlanNode {
                    action: StepAction::RegisterInput(op),
                    children: node_children,
                }];
            }

            node_children.remove(0)
        })
        .collect();

    PlanTree::new(roots, 0, n_payment_outputs)
}

/// Builds a `Plan` from a leaf action path and current peer state.
/// Input amounts are looked up from the live UTXO pool. Residue is empty —
/// all payment obligations are treated as handled for leaf ranking purposes.
fn plan_from_path(path: &[&StepAction], peer: &PeerState, wallet: &WalletHandle) -> Plan {
    let mut my_inputs = Vec::new();
    let mut my_outputs = Vec::new();

    for action in path {
        match action {
            StepAction::RegisterInput(op) => {
                let amount = op.with(wallet.sim).data().amount;
                my_inputs.push((*op, amount));
            }
            StepAction::RegisterOutput(amt) => my_outputs.push(*amt),
        }
    }

    Plan {
        my_inputs,
        my_outputs,
        their_inputs: peer.their_inputs.clone(),
        their_outputs: peer.their_outputs.clone(),
        wallet_residue: WalletResidue {
            utxos: Vec::<UtxoWithMetadata>::new(),
            payment_obligations: Vec::<PaymentObligationData>::new(),
        },
    }
}

/// Distance from `amount` to the nearest denomination in `menu` (in satoshis).
fn denomination_fit(amount: Amount, menu: &DenominationMenu) -> u64 {
    menu.denominations
        .iter()
        .map(|&d| amount.to_sat().abs_diff(d.to_sat()))
        .min()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn leaf_paths(nodes: &[PlanNode], prefix: Vec<Amount>) -> Vec<Vec<Amount>> {
        let mut paths = Vec::new();
        for node in nodes {
            let StepAction::RegisterOutput(amt) = node.action else {
                continue;
            };
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
        assert!(!nodes.is_empty());
        for path in leaf_paths(&nodes, vec![]) {
            let total: Amount = path.iter().copied().sum();
            assert_eq!(
                total, remaining,
                "path {path:?} does not sum to {remaining}"
            );
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
        let nodes = decompose(Amount::from_sat(999), &menu);
        assert_eq!(nodes.len(), 1);
        assert!(nodes[0].children.is_empty());
        assert_eq!(
            nodes[0].action,
            StepAction::RegisterOutput(Amount::from_sat(999))
        );
    }

    #[test]
    fn standard_menu_is_sorted_descending() {
        let menu = DenominationMenu::standard();
        assert!(!menu.denominations.is_empty());
        for window in menu.denominations.windows(2) {
            assert!(window[0] >= window[1]);
        }
    }

    fn simple_tree() -> PlanTree {
        let x = PlanNode {
            action: StepAction::RegisterOutput(Amount::from_sat(1_000)),
            children: vec![],
        };
        let y = PlanNode {
            action: StepAction::RegisterOutput(Amount::from_sat(2_000)),
            children: vec![],
        };
        let a = PlanNode {
            action: StepAction::RegisterOutput(Amount::from_sat(10_000)),
            children: vec![x, y],
        };
        let b = PlanNode {
            action: StepAction::RegisterOutput(Amount::from_sat(20_000)),
            children: vec![],
        };
        PlanTree::new(vec![a, b], 0, 0)
    }

    #[test]
    fn next_actions_at_root_returns_all_roots() {
        let tree = simple_tree();
        let actions = tree.next_actions();
        assert_eq!(actions.len(), 2);
        assert!(actions.contains(&&StepAction::RegisterOutput(Amount::from_sat(10_000))));
        assert!(actions.contains(&&StepAction::RegisterOutput(Amount::from_sat(20_000))));
    }

    #[test]
    fn commit_prunes_siblings_and_advances_cursor() {
        let mut tree = simple_tree();
        tree.commit(&StepAction::RegisterOutput(Amount::from_sat(10_000)))
            .unwrap();
        let actions = tree.next_actions();
        assert_eq!(actions.len(), 2);
        assert!(actions.contains(&&StepAction::RegisterOutput(Amount::from_sat(1_000))));
        assert!(actions.contains(&&StepAction::RegisterOutput(Amount::from_sat(2_000))));
    }

    #[test]
    fn commit_unknown_action_returns_error() {
        let mut tree = simple_tree();
        let result = tree.commit(&StepAction::RegisterOutput(Amount::from_sat(99_999)));
        assert!(matches!(result, Err(CommitError::ActionNotFound)));
    }

    #[test]
    fn commit_at_leaf_returns_error() {
        let mut tree = simple_tree();
        tree.commit(&StepAction::RegisterOutput(Amount::from_sat(20_000)))
            .unwrap();
        let result = tree.commit(&StepAction::RegisterOutput(Amount::from_sat(20_000)));
        assert!(matches!(result, Err(CommitError::AlreadyAtLeaf)));
    }

    #[test]
    fn reachable_leaves_returns_all_leaf_paths() {
        let tree = simple_tree();
        let leaves = tree.reachable_leaves();
        assert_eq!(leaves.len(), 3);
    }

    #[test]
    fn reachable_leaves_shrinks_after_commit() {
        let mut tree = simple_tree();
        tree.commit(&StepAction::RegisterOutput(Amount::from_sat(10_000)))
            .unwrap();
        let leaves = tree.reachable_leaves();
        assert_eq!(leaves.len(), 2);
    }
}
