use std::{iter::Sum, ops::Add};

use bdk_coin_select::{Target, TargetFee, TargetOutputs};
use bitcoin::Amount;
use log::debug;

use crate::{
    bulletin_board::BulletinBoardId,
    coin_selection::{select_all, select_bnb},
    cospend::{CospendInterest, UtxoWithMetadata},
    message::MessageId,
    metrics::PrivacyBundle,
    transaction::Outpoint,
    tx_contruction::TxConstructionState,
    wallet::{AddressId, PaymentObligationData, PaymentObligationId, WalletHandle},
};

fn piecewise_linear(x: f64, points: &[(f64, f64)]) -> f64 {
    assert!(points.len() >= 2, "need at least two points");

    // Clamp on either end of the points
    if x <= points[0].0 {
        return points[0].1;
    }

    let last = points.len() - 1;
    if x >= points[last].0 {
        return points[last].1;
    }

    // Find segment [x_i, x_{i+1}] containing x
    for window in points.windows(2) {
        let (x0, y0) = window[0];
        let (x1, y1) = window[1];

        if x >= x0 && x <= x1 {
            let t = (x - x0) / (x1 - x0);
            return y0 + t * (y1 - y0);
        }
    }

    unreachable!("x did not fall into any segment; are points sorted?");
}

/// An Action a wallet can perform
#[derive(Debug)]
pub(crate) enum Action {
    /// Spend a payment obligation unilaterally with pre-selected inputs and pre-computed change
    UnilateralPayments(Vec<PaymentObligationId>, Vec<Outpoint>, Vec<Amount>),
    /// Accept a cospend invitation
    AcceptCospendProposal((MessageId, BulletinBoardId)),
    /// Contribute outputs to a cospend session that is waiting for them, with pre-computed change
    ContributeOutputsToSession(BulletinBoardId, Vec<PaymentObligationId>, Vec<Amount>),
    /// Continue to participate in a multi-party payjoin
    ContinueParticipateInCospend(BulletinBoardId),
    /// Taker records non-committal interest in cospending with each orderbook UTXO
    ProposeCospend(Vec<CospendInterest>),
    /// Aggregator creates an aggregate session from pending interests
    CreateAggregateProposal(Vec<CospendInterest>),
    /// Register a single UTXO in the order book (maker action)
    RegisterInput(Vec<Outpoint>),
    /// Do nothing. There may be better oppurtunities to spend a payment obligation or participate in a payjoin.
    Wait,
}

/// The portion of wallet state not consumed by a given plan.
#[derive(Debug, Clone)]
pub(crate) struct WalletResidue {
    /// UTXOs not spent by a plan
    #[allow(dead_code)]
    pub(crate) utxos: Vec<UtxoWithMetadata>,
    /// Payment obligations not handled by a plan
    pub(crate) payment_obligations: Vec<PaymentObligationData>,
}

/// Transaction-level view of a candidate action from this wallet's perspective.
#[derive(Debug, Clone)]
pub(crate) struct Plan {
    #[allow(dead_code)]
    pub(crate) my_inputs: Vec<(Outpoint, Amount)>,
    #[allow(dead_code)]
    pub(crate) my_outputs: Vec<Amount>,
    #[allow(dead_code)]
    pub(crate) their_inputs: Vec<(Outpoint, Amount)>,
    #[allow(dead_code)]
    pub(crate) their_outputs: Vec<Amount>,
    /// Unspent UTXOs and unhandled POs after this plan executes
    pub(crate) wallet_residue: WalletResidue,
}

/// Scaling factors controlling which privacy penalties are active when scoring a plan.
///
/// Privacy costs decompose into two orthogonal categories:
/// - External: risks from counterparty behavior - what they can infer, leak, or do
///   with the information they receive by participating (e.g. they see my input UTXOs,
///   they can correlate outputs, they may be chain-analysis firms).
/// - Internal: costs from my own transaction structure choices - change linkability,
///   UTXO fragmentation, address reuse. These are always in my control regardless of
///   who the counterparty is.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CostMode {
    /// Multiplier for privacy costs caused by counterparty behavior.
    /// 0.0 = trust counterparties fully; 1.0 = assume adversarial.
    pub(crate) external: f64,
    /// Multiplier for privacy costs from this wallet's own tx structure choices. i.e my own inputs and outputs
    #[allow(dead_code)]
    pub(crate) internal: f64,
}

impl CostMode {
    /// Counterparties are assumed honest: external penalty zeroed, internal penalties retained.
    pub(crate) const EXTERNAL_PENALTIES_OFF: Self = Self {
        external: 0.0,
        internal: 1.0,
    };
    /// Counterparties are assumed adversarial: full external penalty applied.
    pub(crate) const EXTERNAL_PENALTIES_ON: Self = Self {
        external: 1.0,
        internal: 1.0,
    };
}

/// Cost interval for a plan. Best and worse case senarios are based on the cost mode.
#[derive(Debug, Clone)]
pub(crate) struct CostBracket {
    pub(crate) worst: ActionCost,
    #[allow(dead_code)]
    pub(crate) best: ActionCost,
}

/// Build a `Plan` from any `Action`, deriving inputs, outputs, and wallet residue
/// from the action's payload and current wallet state.
/// For actions with no direct cost impact (Wait, RegisterInput, etc.) no POs are handled
/// and the full wallet state is the residue.
fn plan_from_action(action: &Action, wallet: &WalletHandle) -> Plan {
    use crate::bulletin_board::BroadcastMessageType;
    let all_pos = wallet.unhandled_payment_obligations();

    match action {
        Action::UnilateralPayments(po_ids, inputs, change) => {
            let handled: std::collections::HashSet<_> = po_ids.iter().cloned().collect();
            Plan {
                my_inputs: inputs
                    .iter()
                    .map(|op| (*op, op.with(wallet.sim).data().amount))
                    .collect(),
                my_outputs: change.clone(),
                their_inputs: vec![],
                their_outputs: all_pos
                    .iter()
                    .filter(|po| handled.contains(&po.id))
                    .map(|po| po.amount)
                    .collect(),
                wallet_residue: WalletResidue {
                    utxos: wallet.spendable_utxos(),
                    payment_obligations: all_pos
                        .into_iter()
                        .filter(|po| !handled.contains(&po.id))
                        .collect(),
                },
            }
        }

        Action::ContributeOutputsToSession(bb_id, po_ids, change) => {
            let session = wallet
                .info()
                .active_multi_party_payjoins
                .get(bb_id)
                .expect("session must exist when contributing outputs");
            let my_outpoints: Vec<Outpoint> = session.inputs.iter().map(|i| i.outpoint).collect();
            let my_inputs: Vec<(Outpoint, Amount)> = my_outpoints
                .iter()
                .map(|op| (*op, op.with(wallet.sim).data().amount))
                .collect();
            let messages = wallet.sim.bulletin_boards[bb_id.0].messages.clone();
            let handled: std::collections::HashSet<_> = po_ids.iter().cloned().collect();
            Plan {
                their_inputs: messages
                    .iter()
                    .filter_map(|m| match m {
                        BroadcastMessageType::ContributeInputs(op)
                            if !my_outpoints.contains(op) =>
                        {
                            Some((*op, op.with(wallet.sim).data().amount))
                        }
                        _ => None,
                    })
                    .collect(),
                my_inputs,
                my_outputs: change.clone(),
                their_outputs: all_pos
                    .iter()
                    .filter(|po| handled.contains(&po.id))
                    .map(|po| po.amount)
                    .collect(),
                wallet_residue: WalletResidue {
                    utxos: wallet.spendable_utxos(),
                    payment_obligations: all_pos
                        .into_iter()
                        .filter(|po| !handled.contains(&po.id))
                        .collect(),
                },
            }
        }

        Action::AcceptCospendProposal((_, bb_id)) => {
            let messages = wallet.sim.bulletin_boards[bb_id.0].messages.clone();
            let my_confirmed = &wallet.info().confirmed_utxos;
            let my_addresses: std::collections::HashSet<AddressId> =
                wallet.data().addresses.iter().cloned().collect();
            let handled: std::collections::HashSet<PaymentObligationId> = wallet
                .info()
                .active_multi_party_payjoins
                .get(bb_id)
                .map(|s| s.payment_obligation_ids.iter().cloned().collect())
                .unwrap_or_default();
            Plan {
                my_inputs: messages
                    .iter()
                    .filter_map(|m| match m {
                        BroadcastMessageType::ContributeInputs(op) if my_confirmed.contains(op) => {
                            Some((*op, op.with(wallet.sim).data().amount))
                        }
                        _ => None,
                    })
                    .collect(),
                my_outputs: messages
                    .iter()
                    .filter_map(|m| match m {
                        BroadcastMessageType::ContributeOutputs(o)
                            if my_addresses.contains(&o.address_id) =>
                        {
                            Some(o.amount)
                        }
                        _ => None,
                    })
                    .collect(),
                their_inputs: messages
                    .iter()
                    .filter_map(|m| match m {
                        BroadcastMessageType::ContributeInputs(op)
                            if !my_confirmed.contains(op) =>
                        {
                            Some((*op, op.with(wallet.sim).data().amount))
                        }
                        _ => None,
                    })
                    .collect(),
                their_outputs: messages
                    .iter()
                    .filter_map(|m| match m {
                        BroadcastMessageType::ContributeOutputs(o)
                            if !my_addresses.contains(&o.address_id) =>
                        {
                            Some(o.amount)
                        }
                        _ => None,
                    })
                    .collect(),
                wallet_residue: WalletResidue {
                    utxos: wallet.spendable_utxos(),
                    payment_obligations: all_pos
                        .into_iter()
                        .filter(|po| !handled.contains(&po.id))
                        .collect(),
                },
            }
        }
        // Actions with no direct PO handling or tx contribution: residue = entire wallet state.
        _ => Plan {
            my_inputs: vec![],
            my_outputs: vec![],
            their_inputs: vec![],
            their_outputs: vec![],
            wallet_residue: WalletResidue {
                utxos: wallet.spendable_utxos(),
                payment_obligations: all_pos,
            },
        },
    }
}

/// Strategies will pick one action to minimize their cost
/// TODO: Strategies should be composible. They should enform the action decision space scoring and doing actions should be handling by something else that has composed multiple strategies.
pub(crate) trait Strategy: std::fmt::Debug {
    fn enumerate_candidate_actions(&self, wallet: &WalletHandle) -> Vec<Action>;
    fn clone_box(&self) -> Box<dyn Strategy>;
}

#[derive(Debug, Clone, PartialEq)]
// TODO: this should just be bitcoin::Amount
pub(crate) struct ActionCost(pub(crate) f64);

// Flat base cost applied to any action, including waiting.
const INHERENT_ACTION_COST: f64 = 0.0;

impl Sum for ActionCost {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        Self(iter.map(|s| s.0).sum())
    }
}

impl Eq for ActionCost {}

impl Ord for ActionCost {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        assert!(!self.0.is_nan() && !other.0.is_nan());
        self.0.partial_cmp(&other.0).expect("Checked for NaNs")
    }
}

impl PartialOrd for ActionCost {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Add for ActionCost {
    type Output = Self;
    fn add(self, other: Self) -> Self {
        Self(self.0 + other.0)
    }
}

/// Build a BDK Target for a slice of payment obligations, estimating output weight
/// from each recipient wallet's script type.
fn target_for_obligations(pos: &[PaymentObligationData], wallet: &WalletHandle) -> Target {
    let value_sum: u64 = pos.iter().map(|po| po.amount.to_sat()).sum();
    let weight_sum: u32 = pos
        .iter()
        .map(|po| po.to.with(wallet.sim).data().script_type.output_weight_wu())
        .sum();
    Target {
        fee: TargetFee {
            rate: bdk_coin_select::FeeRate::from_sat_per_vb(1.0),
            replace: None,
        },
        outputs: TargetOutputs {
            value_sum,
            weight_sum,
            n_outputs: pos.len(),
        },
    }
}

/// Compute pre-selected change outputs for a `ContributeOutputsToSession` action.
/// If the session has pre-selected inputs (from the aggregator), uses those exactly.
/// Otherwise falls back to full BNB / spend-all selection over all wallet UTXOs.
fn change_for_session_contribution(
    bb_id: &BulletinBoardId,
    pos: &[PaymentObligationData],
    wallet: &WalletHandle,
) -> Vec<Amount> {
    let session = wallet
        .info()
        .active_multi_party_payjoins
        .get(bb_id)
        .unwrap();
    let session_input_outpoints: Vec<Outpoint> =
        session.inputs.iter().map(|i| i.outpoint).collect();
    let target = target_for_obligations(pos, wallet);
    if session_input_outpoints.is_empty() {
        let candidates = wallet.coin_candidates();
        if let Some((_, change)) = select_bnb(&candidates, target) {
            return change;
        }
        select_all(&candidates, target).1
    } else {
        let candidates = wallet.coin_candidates_for(&session_input_outpoints);
        select_all(&candidates, target).1
    }
}

/// Enumerate every `Action::UnilateralPayments` that a wallet could perform unilaterally,
/// covering the full powerset of pending payment obligations and both BNB and SpendAll
/// coin selection strategies for each subset.
///
/// This is used to establish the true best unilateral fallback cost when deciding whether
/// to accept or propose a cospend. Considering only per-obligation actions (as
/// `UnilateralSpender` does) understates the fallback because batching can be cheaper.
///
/// The powerset has 2^n - 1 non-empty subsets. This is feasible for small n (typical in
/// simulation where a wallet holds a handful of pending POs at a time).
fn enumerate_unilateral_actions(wallet: &WalletHandle) -> Vec<Action> {
    let all_pos = wallet.unhandled_payment_obligations();
    let n = all_pos.len();
    if n == 0 {
        return vec![];
    }
    let candidates = wallet.coin_candidates();
    let mut actions = vec![];
    for mask in 1u64..(1u64 << n) {
        let subset: Vec<&PaymentObligationData> = (0..n)
            .filter(|i| mask & (1 << i) != 0)
            .map(|i| &all_pos[i])
            .collect();
        let po_ids: Vec<PaymentObligationId> = subset.iter().map(|po| po.id).collect();
        let subset_owned: Vec<PaymentObligationData> = subset.into_iter().cloned().collect();
        let target = target_for_obligations(&subset_owned, wallet);
        if let Some((inputs, change)) = select_bnb(&candidates, target) {
            actions.push(Action::UnilateralPayments(po_ids.clone(), inputs, change));
        }
        let (all_inputs, change) = select_all(&candidates, target);
        if !all_inputs.is_empty() {
            actions.push(Action::UnilateralPayments(po_ids, all_inputs, change));
        }
    }
    actions
}

#[derive(Debug, Clone)]
pub(crate) struct UnilateralSpender;

impl Strategy for UnilateralSpender {
    /// The decision space of the unilateral spender is the set of all payment obligations.
    /// For each obligation, enumerate both BNB and spend-all coin selections so the cost
    /// function can pick the cheaper input set.
    fn enumerate_candidate_actions(&self, wallet: &WalletHandle) -> Vec<Action> {
        let payment_obligations = wallet.unhandled_payment_obligations();
        if payment_obligations.is_empty() {
            return vec![Action::Wait];
        }
        let candidates = wallet.coin_candidates();
        let mut actions = vec![];
        for po in payment_obligations.iter() {
            let target = target_for_obligations(std::slice::from_ref(po), wallet);
            if let Some((inputs, change)) = select_bnb(&candidates, target) {
                actions.push(Action::UnilateralPayments(vec![po.id], inputs, change));
            }
            let (all_inputs, change) = select_all(&candidates, target);
            if !all_inputs.is_empty() {
                actions.push(Action::UnilateralPayments(vec![po.id], all_inputs, change));
            }
        }
        if actions.is_empty() {
            actions.push(Action::Wait);
        }
        actions
    }

    fn clone_box(&self) -> Box<dyn Strategy> {
        Box::new(self.clone())
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Consolidator;

impl Strategy for Consolidator {
    /// Always uses spend-all when paying — forces consolidation regardless of fee efficiency.
    fn enumerate_candidate_actions(&self, wallet: &WalletHandle) -> Vec<Action> {
        let candidates = wallet.coin_candidates();
        let mut actions = Vec::new();
        for po in wallet.unhandled_payment_obligations().iter() {
            let target = target_for_obligations(std::slice::from_ref(po), wallet);
            let (all_inputs, change) = select_all(&candidates, target);
            if !all_inputs.is_empty() {
                actions.push(Action::UnilateralPayments(vec![po.id], all_inputs, change));
            }
        }
        actions.push(Action::Wait);
        actions
    }

    fn clone_box(&self) -> Box<dyn Strategy> {
        Box::new(self.clone())
    }
}

#[derive(Debug, Clone)]
pub(crate) struct BatchSpender;

impl Strategy for BatchSpender {
    fn enumerate_candidate_actions(&self, wallet: &WalletHandle) -> Vec<Action> {
        let payment_obligations = wallet.unhandled_payment_obligations();
        if payment_obligations.is_empty() {
            return vec![Action::Wait];
        }
        // TODO: we may need to consider different partitioning strategies for the batch spend
        let po_ids: Vec<PaymentObligationId> = payment_obligations.iter().map(|po| po.id).collect();
        let target = target_for_obligations(&payment_obligations, wallet);
        let candidates = wallet.coin_candidates();
        let mut actions = vec![];
        if let Some((inputs, change)) = select_bnb(&candidates, target) {
            actions.push(Action::UnilateralPayments(po_ids.clone(), inputs, change));
        }
        let (all_inputs, change) = select_all(&candidates, target);
        if !all_inputs.is_empty() {
            actions.push(Action::UnilateralPayments(po_ids, all_inputs, change));
        }
        if actions.is_empty() {
            actions.push(Action::Wait);
        }
        actions
    }

    fn clone_box(&self) -> Box<dyn Strategy> {
        Box::new(self.clone())
    }
}

#[derive(Debug, Clone)]
pub(crate) struct MultipartyStrategy;

impl Strategy for MultipartyStrategy {
    fn enumerate_candidate_actions(&self, wallet: &WalletHandle) -> Vec<Action> {
        let mut actions = vec![];

        let active_cospends = wallet.active_cospend_sessions();
        let cospend_proposals = wallet.pending_cospend_proposals();
        let payment_obligations = wallet.unhandled_payment_obligations();
        let registered_inputs = wallet.registered_input_outpoints();
        let scorer = &wallet.data().scorer;

        // Continue active sessions
        for bulletin_board_id in active_cospends.iter() {
            actions.push(Action::ContinueParticipateInCospend(*bulletin_board_id));
        }

        // Accept incoming proposals if no active session and enough unilateral fallbacks exist.
        // No cost comparison here — acceptance is cheap (no funds committed yet) and the real
        // cost gate is in ContributeOutputsToSession where POs are actually committed.
        if let Some((bulletin_board_id, message_id)) = cospend_proposals.first() {
            if active_cospends.is_empty() {
                let fallback_count = enumerate_unilateral_actions(wallet).len();
                if fallback_count >= scorer.min_fallback_plans {
                    actions.push(Action::AcceptCospendProposal((
                        *message_id,
                        *bulletin_board_id,
                    )));
                }
            }
        }

        // Contribute outputs to sessions in AcceptedProposal state.
        // No cost gate here: acceptance is the commitment decision; once accepted, the
        // wallet fulfills by contributing all its pending POs to the session.
        for (bb_id, session) in wallet.info().active_multi_party_payjoins.iter() {
            if session.state == TxConstructionState::AcceptedProposal
                && !payment_obligations.is_empty()
            {
                let po_ids: Vec<PaymentObligationId> =
                    payment_obligations.iter().map(|po| po.id).collect();
                let change = change_for_session_contribution(bb_id, &payment_obligations, wallet);
                actions.push(Action::ContributeOutputsToSession(*bb_id, po_ids, change));
            }
        }

        // When truly idle (no pending proposals, no non-completed sessions):
        // register inputs in the orderbook and propose cospend if possible.
        let has_active_sessions = !active_cospends.is_empty()
            || wallet
                .info()
                .active_multi_party_payjoins
                .values()
                .any(|s| !matches!(s.state, TxConstructionState::Success(_)));
        if cospend_proposals.is_empty() && !has_active_sessions {
            // Register inputs — use the intersection of inputs across all unilateral actions
            // so we only advertise coins that are committed to regardless of which PO we pay.
            let unilateral_actions = UnilateralSpender.enumerate_candidate_actions(wallet);
            let per_action_inputs: Vec<std::collections::HashSet<Outpoint>> = unilateral_actions
                .iter()
                .filter_map(|a| match a {
                    Action::UnilateralPayments(_, inputs, _) => {
                        Some(inputs.iter().copied().collect())
                    }
                    _ => None,
                })
                .collect();
            let common_inputs: Vec<Outpoint> = per_action_inputs
                .iter()
                .skip(1)
                .fold(
                    per_action_inputs.first().cloned().unwrap_or_default(),
                    |acc, s| acc.intersection(s).copied().collect(),
                )
                .iter()
                .filter(|o| !registered_inputs.contains(o))
                .copied()
                .collect();
            if !common_inputs.is_empty() {
                actions.push(Action::RegisterInput(common_inputs));
            }

            // Propose cospend to each orderbook peer whose worst-case cost is competitive,
            // provided there are enough unilateral fallback plans.
            if !payment_obligations.is_empty() {
                if let Some(own_utxo) = wallet.spendable_utxos().into_iter().next() {
                    let po_ids: Vec<PaymentObligationId> =
                        payment_obligations.iter().map(|po| po.id).collect();
                    let unilateral_fallbacks = enumerate_unilateral_actions(wallet);
                    if unilateral_fallbacks.len() < scorer.min_fallback_plans {
                        // Not enough fallback options. Don't propose anything dont make any commitments.
                    } else {
                        let best_unilateral_worst = unilateral_fallbacks
                            .iter()
                            .map(|a| scorer.bracket(&plan_from_action(a, wallet), wallet).worst)
                            .min()
                            .unwrap_or(ActionCost(f64::MAX));
                        let residue_pos: Vec<PaymentObligationData> = wallet
                            .unhandled_payment_obligations()
                            .into_iter()
                            .filter(|po| !po_ids.contains(&po.id))
                            .collect();
                        let interests: Vec<CospendInterest> = wallet
                            .orderbook_utxos()
                            .into_iter()
                            .filter(|peer_utxo| peer_utxo.owner != wallet.id)
                            .filter(|peer_utxo| {
                                let plan = Plan {
                                    my_inputs: vec![(own_utxo.outpoint, own_utxo.amount)],
                                    my_outputs: vec![],
                                    their_inputs: vec![(peer_utxo.outpoint, peer_utxo.amount)],
                                    their_outputs: vec![],
                                    wallet_residue: WalletResidue {
                                        utxos: wallet.spendable_utxos(),
                                        payment_obligations: residue_pos.clone(),
                                    },
                                };
                                scorer.bracket(&plan, wallet).worst <= best_unilateral_worst
                            })
                            .map(|peer_utxo| CospendInterest {
                                utxos: vec![own_utxo.clone(), peer_utxo],
                            })
                            .collect();
                        if !interests.is_empty() {
                            actions.push(Action::ProposeCospend(interests));
                        }
                    }
                }
            }
        }

        if actions.is_empty() {
            actions.push(Action::Wait);
        }
        actions
    }

    fn clone_box(&self) -> Box<dyn Strategy> {
        Box::new(self.clone())
    }
}

const MIN_AGGREGATE_INTERESTS: usize = 2;

#[derive(Debug, Clone)]
pub(crate) struct AggregatorStrategy;

impl Strategy for AggregatorStrategy {
    fn enumerate_candidate_actions(&self, wallet: &WalletHandle) -> Vec<Action> {
        let pending_interests = wallet.pending_interests();
        if pending_interests.len() < MIN_AGGREGATE_INTERESTS {
            return vec![Action::Wait];
        }
        vec![Action::CreateAggregateProposal(pending_interests)]
    }

    fn clone_box(&self) -> Box<dyn Strategy> {
        Box::new(self.clone())
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CompositeStrategy {
    pub(crate) strategies: Vec<Box<dyn Strategy>>,
}

impl Strategy for CompositeStrategy {
    fn enumerate_candidate_actions(&self, wallet: &WalletHandle) -> Vec<Action> {
        let mut actions = vec![];
        for strategy in self.strategies.iter() {
            actions.extend(strategy.enumerate_candidate_actions(wallet));
        }
        actions
    }

    fn clone_box(&self) -> Box<dyn Strategy> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn Strategy> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

// TODO: this should be a trait once we have different scoring strategies
#[derive(Debug, Clone)]
pub(crate) struct CompositeScorer {
    /// Privacy metric bundle evaluated against a shared budget
    pub(crate) privacy_bundle: PrivacyBundle,
    /// Weight applied to deadline urgency for payment obligations
    pub(crate) payment_obligation_weight: f64,
    /// Minimum number of viable unilateral fallback plans required before committing to a
    /// multiparty session. 0 = no restriction.
    pub(crate) min_fallback_plans: usize,
}

impl CompositeScorer {
    /// Score a plan under a given cost mode.
    /// Handled POs are derived by diffing wallet state against the plan's residue.
    pub(crate) fn score(&self, plan: &Plan, wallet: &WalletHandle, mode: CostMode) -> ActionCost {
        let ts = wallet.sim.current_timestep;

        let residue_po_ids: std::collections::HashSet<PaymentObligationId> = plan
            .wallet_residue
            .payment_obligations
            .iter()
            .map(|po| po.id)
            .collect();

        let mut cost = ActionCost(INHERENT_ACTION_COST);
        for po in wallet
            .unhandled_payment_obligations()
            .into_iter()
            .filter(|po| !residue_po_ids.contains(&po.id))
        {
            let time_left = po.deadline.0 as i32 - ts.0 as i32;
            // Utility of 2*weight at deadline easily exceeds the base cost,
            // making near-deadline payments cheaper than waiting.
            let base_cost = po.amount.to_float_in(bitcoin::Denomination::Bitcoin);
            let points = [
                (0.0, 2.0 * self.payment_obligation_weight),
                (2.0, self.payment_obligation_weight),
                (5.0, 0.0),
            ];
            let utility = piecewise_linear(time_left as f64, &points);
            debug!(
                "PaymentObligationHandled cost: base={} utility={}",
                base_cost, utility
            );
            cost = cost + ActionCost(base_cost - utility);
        }

        cost = cost + self.privacy_bundle.evaluate(plan, mode);
        // TODO: internal privacy penalty (change linkability, UTXO fragmentation, address reuse)
        // cost = cost + ActionCost(self.privacy_weight * mode.internal * internal_delta);

        cost
    }

    /// Convenience wrapper: score the action under EXTERNAL_PENALTIES_ON.
    pub(crate) fn action_cost(&self, action: &Action, wallet: &WalletHandle) -> ActionCost {
        self.score(
            &plan_from_action(action, wallet),
            wallet,
            CostMode::EXTERNAL_PENALTIES_ON,
        )
    }

    /// Compute a `[best, worst]` cost bracket for a plan.
    pub(crate) fn bracket(&self, plan: &Plan, wallet: &WalletHandle) -> CostBracket {
        CostBracket {
            worst: self.score(plan, wallet, CostMode::EXTERNAL_PENALTIES_ON),
            best: self.score(plan, wallet, CostMode::EXTERNAL_PENALTIES_OFF),
        }
    }
}

/// Creates a strategy instance from its name string
pub(crate) fn create_strategy(name: &str) -> Result<Box<dyn Strategy>, String> {
    match name {
        "UnilateralSpender" => Ok(Box::new(UnilateralSpender)),
        "Consolidator" => Ok(Box::new(Consolidator)),
        "BatchSpender" => Ok(Box::new(BatchSpender)),
        "MultipartyStrategy" => Ok(Box::new(MultipartyStrategy)),
        "AggregatorStrategy" => Ok(Box::new(AggregatorStrategy)),
        _ => Err(format!("Unknown strategy: {}", name)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        bulletin_board::BulletinBoardId,
        message::{MessageData, MessageId, MessageType},
        tx_contruction::{MultiPartyPayjoinSession, TxConstructionState},
        wallet::{PaymentObligationData, WalletId},
        TimeStep,
    };
    use bitcoin::Amount;

    fn test_sim() -> crate::Simulation {
        use crate::{
            config::{ScorerConfig, WalletTypeConfig},
            script_type::ScriptType,
            SimulationBuilder,
        };
        SimulationBuilder::new(
            42,
            vec![WalletTypeConfig {
                name: "test".to_string(),
                count: 2,
                strategies: vec!["UnilateralSpender".to_string()],
                scorer: ScorerConfig {
                    privacy_weight: 0.0,
                    payment_obligation_weight: 0.0,
                    min_fallback_plans: 0,
                },
                script_type: ScriptType::P2tr,
            }],
            10,
            1,
            0,
        )
        .build()
    }

    fn add_payment_obligation(sim: &mut crate::Simulation, po: PaymentObligationData) {
        let id = po.id;
        let from = po.from;
        sim.payment_data.push(po);
        let last_id = sim.wallet_data[from.0].last_wallet_info_id;
        sim.wallet_info[last_id.0].payment_obligations.insert(id);
    }

    fn inject_active_session(
        sim: &mut crate::Simulation,
        wallet_id: WalletId,
        bb_id: BulletinBoardId,
        state: TxConstructionState,
    ) {
        let mut wallet = wallet_id.with_mut(sim);
        let mut info = wallet.info().clone();
        info.active_multi_party_payjoins.insert(
            bb_id,
            MultiPartyPayjoinSession {
                state,
                inputs: vec![],
                payment_obligation_ids: vec![],
            },
        );
        wallet.update_info(info);
    }

    fn add_cospend_proposal_message(
        sim: &mut crate::Simulation,
        to: WalletId,
        from: WalletId,
        bb_id: BulletinBoardId,
    ) {
        let id = MessageId(sim.messages.len());
        sim.messages.push(MessageData {
            id,
            message: MessageType::ProposeCoSpend(bb_id),
            from,
            to,
        });
    }

    #[test]
    fn test_unilateral_spender_no_utxos() {
        let mut sim = test_sim();
        let po = PaymentObligationData {
            id: PaymentObligationId(0),
            deadline: TimeStep(100),
            reveal_time: TimeStep(0),
            amount: Amount::from_sat(1000),
            from: WalletId(0),
            to: WalletId(1),
        };
        add_payment_obligation(&mut sim, po);
        let wallet = WalletId(0).with_mut(&mut sim);
        let strategy = UnilateralSpender;

        let actions = strategy.enumerate_candidate_actions(&wallet);

        // Wallet has no UTXOs, coin selection produces nothing falls back to Wait.
        assert_eq!(actions.len(), 1);
        assert!(matches!(actions[0], Action::Wait));
    }

    #[test]
    fn test_unilateral_consolidate_spender_no_utxos() {
        let mut sim = test_sim();
        let po = PaymentObligationData {
            id: PaymentObligationId(0),
            deadline: TimeStep(100),
            reveal_time: TimeStep(0),
            amount: Amount::from_sat(1000),
            from: WalletId(0),
            to: WalletId(1),
        };
        add_payment_obligation(&mut sim, po);
        let wallet = WalletId(0).with_mut(&mut sim);
        let strategy = Consolidator;

        let actions = strategy.enumerate_candidate_actions(&wallet);

        // Consolidator always emits Wait, and skips UnilateralPayments when no UTXOs exist.
        assert!(actions.iter().any(|a| matches!(a, Action::Wait)));
        assert!(!actions
            .iter()
            .any(|a| matches!(a, Action::UnilateralPayments(_, _, _))));
    }

    #[test]
    fn test_batch_spender_no_utxos() {
        let mut sim = test_sim();
        let po1 = PaymentObligationData {
            id: PaymentObligationId(0),
            deadline: TimeStep(100),
            reveal_time: TimeStep(0),
            amount: Amount::from_sat(1000),
            from: WalletId(0),
            to: WalletId(1),
        };
        let po2 = PaymentObligationData {
            id: PaymentObligationId(1),
            deadline: TimeStep(100),
            reveal_time: TimeStep(0),
            amount: Amount::from_sat(2000),
            from: WalletId(0),
            to: WalletId(1),
        };
        add_payment_obligation(&mut sim, po1);
        add_payment_obligation(&mut sim, po2);
        let wallet = WalletId(0).with_mut(&mut sim);
        let strategy = BatchSpender;

        let actions = strategy.enumerate_candidate_actions(&wallet);

        // No UTXOs coin selection produces nothing, falls back to Wait.
        assert_eq!(actions.len(), 1);
        assert!(matches!(actions[0], Action::Wait));
    }

    #[test]
    fn test_composite_strategy_combines_actions() {
        // TODO: this test is kinda useless, we need to add UTXOs to the sim and test the composite strategy.
        // Otherwise we are just testing that both strategies fall back to Wait when there are no UTXOs.
        // This is bc coin selection uses `wallet.handle().coin_candidates();` not `state.utxos`.
        let mut sim = test_sim();
        let po1 = PaymentObligationData {
            id: PaymentObligationId(0),
            deadline: TimeStep(100),
            reveal_time: TimeStep(0),
            amount: Amount::from_sat(1000),
            from: WalletId(0),
            to: WalletId(1),
        };
        let po2 = PaymentObligationData {
            id: PaymentObligationId(1),
            deadline: TimeStep(100),
            reveal_time: TimeStep(0),
            amount: Amount::from_sat(2000),
            from: WalletId(0),
            to: WalletId(1),
        };
        add_payment_obligation(&mut sim, po1);
        add_payment_obligation(&mut sim, po2);
        let wallet = WalletId(0).with_mut(&mut sim);
        let composite = CompositeStrategy {
            strategies: vec![Box::new(UnilateralSpender), Box::new(BatchSpender)],
        };

        let actions = composite.enumerate_candidate_actions(&wallet);

        // Wallet has no UTXOs in the sim, both strategies fall back to Wait.
        // Composite collects one Wait from each strategy.
        assert_eq!(actions.len(), 2);
        assert!(actions.iter().all(|a| matches!(a, Action::Wait)));
    }

    #[test]
    fn test_multiparty_waits_with_no_orderbook_utxos() {
        let mut sim = test_sim();
        let po = PaymentObligationData {
            id: PaymentObligationId(0),
            deadline: TimeStep(100),
            reveal_time: TimeStep(0),
            amount: Amount::from_sat(1000),
            from: WalletId(0),
            to: WalletId(1),
        };
        add_payment_obligation(&mut sim, po);
        // No UTXOs or orderbook entries — taker has nothing to propose with or to
        let wallet = WalletId(0).with_mut(&mut sim);
        let strategy = MultipartyStrategy;

        let actions = strategy.enumerate_candidate_actions(&wallet);

        assert_eq!(actions.len(), 1);
        assert!(matches!(actions[0], Action::Wait));
    }

    #[test]
    fn test_multiparty_proposes_cospend_with_orderbook_utxos() {
        let mut sim = test_sim();
        // Give wallets real UTXOs via build_universe
        sim.build_universe();

        let po = PaymentObligationData {
            id: PaymentObligationId(sim.payment_data.len()),
            deadline: TimeStep(100),
            reveal_time: TimeStep(0),
            amount: Amount::from_sat(1000),
            from: WalletId(0),
            to: WalletId(1),
        };
        add_payment_obligation(&mut sim, po);

        // Register exactly one of wallet 1's UTXOs in the orderbook
        let first_peer_utxo = WalletId(1)
            .with_mut(&mut sim)
            .info()
            .confirmed_utxos
            .iter()
            .next()
            .cloned()
            .unwrap();
        WalletId(1)
            .with_mut(&mut sim)
            .do_action(&Action::RegisterInput(vec![first_peer_utxo]));

        let strategy = MultipartyStrategy;
        let wallet = WalletId(0).with_mut(&mut sim);

        let actions = strategy.enumerate_candidate_actions(&wallet);

        // Strategy also registers inputs when idle, so there may be more than one action.
        assert!(actions
            .iter()
            .any(|a| matches!(a, Action::ProposeCospend(interests) if interests.len() == 1)));
        assert!(!actions.iter().any(|a| matches!(a, Action::Wait)));
    }

    #[test]
    fn test_multiparty_continues_active_session() {
        let mut sim = test_sim();
        let po = PaymentObligationData {
            id: PaymentObligationId(0),
            deadline: TimeStep(100),
            reveal_time: TimeStep(0),
            amount: Amount::from_sat(1000),
            from: WalletId(0),
            to: WalletId(1),
        };
        add_payment_obligation(&mut sim, po);
        let bb_id = sim.create_bulletin_board();
        inject_active_session(
            &mut sim,
            WalletId(0),
            bb_id,
            TxConstructionState::SentOutputs,
        );

        let wallet = WalletId(0).with_mut(&mut sim);
        let strategy = MultipartyStrategy;

        let actions = strategy.enumerate_candidate_actions(&wallet);

        assert_eq!(actions.len(), 1);
        assert!(actions
            .iter()
            .any(|a| matches!(a, Action::ContinueParticipateInCospend(_))));
    }

    #[test]
    fn test_multiparty_accepts_new_invitation() {
        let mut sim = test_sim();
        let po = PaymentObligationData {
            id: PaymentObligationId(0),
            deadline: TimeStep(100),
            reveal_time: TimeStep(0),
            amount: Amount::from_sat(1000),
            from: WalletId(0),
            to: WalletId(1),
        };
        add_payment_obligation(&mut sim, po);
        let bb_id = sim.create_bulletin_board();
        // Send a ProposeCoSpend message from wallet 1 to wallet 0
        add_cospend_proposal_message(&mut sim, WalletId(0), WalletId(1), bb_id);

        let wallet = WalletId(0).with_mut(&mut sim);
        let strategy = MultipartyStrategy;

        let actions = strategy.enumerate_candidate_actions(&wallet);

        assert_eq!(actions.len(), 1);
        assert!(actions
            .iter()
            .any(|a| matches!(a, Action::AcceptCospendProposal((_, id)) if *id == bb_id)));
    }

    #[test]
    fn test_multiparty_contributes_outputs_when_session_awaiting() {
        let mut sim = test_sim();

        // Create a bulletin board and accept an invitation, advancing session to AcceptedProposal
        let bb_id = sim.create_bulletin_board();
        let msg_id = MessageId(0);
        WalletId(0)
            .with_mut(&mut sim)
            .do_action(&Action::AcceptCospendProposal((msg_id, bb_id)));

        let po = PaymentObligationData {
            id: PaymentObligationId(0),
            deadline: TimeStep(100),
            reveal_time: TimeStep(0),
            amount: Amount::from_sat(1000),
            from: WalletId(0),
            to: WalletId(1),
        };
        add_payment_obligation(&mut sim, po);

        let strategy = MultipartyStrategy;
        let wallet = WalletId(0).with_mut(&mut sim);

        // Verify the session is in AcceptedProposal state
        let session = wallet
            .info()
            .active_multi_party_payjoins
            .get(&bb_id)
            .unwrap();
        assert_eq!(session.state, TxConstructionState::AcceptedProposal);

        let actions = strategy.enumerate_candidate_actions(&wallet);

        assert!(actions
            .iter()
            .any(|a| matches!(a, Action::ContributeOutputsToSession(id, ids, _) if *id == bb_id && ids.len() == 1)));
        // Should NOT emit ContinueParticipateInCospend for this session
        assert!(!actions
            .iter()
            .any(|a| matches!(a, Action::ContinueParticipateInCospend(id) if *id == bb_id)));
    }

    #[test]
    fn test_multiparty_with_active_session() {
        let mut sim = test_sim();
        let po = PaymentObligationData {
            id: PaymentObligationId(0),
            deadline: TimeStep(100),
            reveal_time: TimeStep(0),
            amount: Amount::from_sat(1000),
            from: WalletId(0),
            to: WalletId(1),
        };
        add_payment_obligation(&mut sim, po);
        let bb_id = sim.create_bulletin_board();
        inject_active_session(
            &mut sim,
            WalletId(0),
            bb_id,
            TxConstructionState::SentOutputs,
        );

        let wallet = WalletId(0).with_mut(&mut sim);
        let strategy = MultipartyStrategy;

        let actions = strategy.enumerate_candidate_actions(&wallet);

        assert_eq!(actions.len(), 1);
        assert!(actions
            .iter()
            .any(|a| matches!(a, Action::ContinueParticipateInCospend(_))));
    }

    #[test]
    fn test_multiparty_prefers_continue_when_invite_and_active() {
        let mut sim = test_sim();
        let po = PaymentObligationData {
            id: PaymentObligationId(0),
            deadline: TimeStep(100),
            reveal_time: TimeStep(0),
            amount: Amount::from_sat(1000),
            from: WalletId(0),
            to: WalletId(1),
        };
        add_payment_obligation(&mut sim, po);
        let bb_invite = sim.create_bulletin_board();
        let bb_active = sim.create_bulletin_board();
        // Pending invitation for bb_invite
        add_cospend_proposal_message(&mut sim, WalletId(0), WalletId(1), bb_invite);
        // Active session for bb_active (SentOutputs state)
        inject_active_session(
            &mut sim,
            WalletId(0),
            bb_active,
            TxConstructionState::SentOutputs,
        );

        let wallet = WalletId(0).with_mut(&mut sim);
        let strategy = MultipartyStrategy;

        let actions = strategy.enumerate_candidate_actions(&wallet);

        // With both a new invitation and active session, strategy should continue active session.
        assert_eq!(actions.len(), 1);
        assert!(matches!(
            actions[0],
            Action::ContinueParticipateInCospend(id) if id == bb_active
        ));
    }
}
