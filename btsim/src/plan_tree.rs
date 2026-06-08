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
    pub(crate) fn standard() -> Self {
        todo!()
    }
}

pub(crate) fn decompose(_remaining: Amount, _menu: &DenominationMenu) -> Vec<PlanNode> {
    todo!()
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
