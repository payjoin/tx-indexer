use crate::{
    actions::{Action, CompositeScorer, CompositeStrategy},
    blocks::BroadcastSetId,
    bulletin_board::BulletinBoardId,
    coin_selection::CoinCandidate,
    cospend::{CospendInterest, UtxoWithMetadata},
    message::{MessageId, MessageType},
    script_type::ScriptType,
    tx_contruction::{MultiPartyPayjoinSession, SentOutputs, SentReadyToSign, TxConstructionState},
    Simulation, TimeStep,
};
use bitcoin::{transaction::InputWeightPrediction, Amount};
use im::{HashMap, OrdSet, Vector};
use log::info;

use crate::transaction::*;

define_entity_id_and_handle!(Wallet);
define_entity_handle_mut!(Wallet);
define_entity_info_id!(Wallet);
define_entity_data!(Wallet, {
    pub(crate) id: WalletId,
    pub(crate) addresses: Vec<AddressId>,         // TODO split into internal/external?
    pub(crate) own_transactions: Vec<TxId>,       // transactions originating from this wallet
    pub(crate) last_wallet_info_id: WalletInfoId, // Monotone
    // Monotone index of the last message that was processed by this wallet
    pub(crate) messages_processed: OrdSet<MessageId>,
    pub(crate) strategies: CompositeStrategy,
    pub(crate) scorer: CompositeScorer,
    pub(crate) script_type: ScriptType,
}, skip_eq_clone);
define_entity_info!(Wallet, {
        pub(crate) broadcast_set_id: BroadcastSetId,
        pub(crate) payment_obligations: OrdSet<PaymentObligationId>,
        pub(crate) expected_payments: OrdSet<PaymentObligationId>,
        pub(crate) broadcast_transactions: Vector<TxId>,
        pub(crate) received_transactions: Vector<TxId>,
        pub(crate) confirmed_utxos: OrdSet<Outpoint>,    // TODO locktimes
        pub(crate) unconfirmed_transactions: OrdSet<TxId>,
        pub(crate) unconfirmed_txos: OrdSet<Outpoint>,  // compute CPFP cost
        pub(crate) unconfirmed_spends: OrdSet<Outpoint>, // RBFable
        /// Map of txids to the payment obligations that they are associated with
        /// Sim state should refrence this when updating wallet states after confirmation
        pub(crate) txid_to_payment_obligation_ids: HashMap<TxId, Vec<PaymentObligationId>>,

        /// Set of payment obligations that have been handled
        pub(crate) handled_payment_obligations: OrdSet<PaymentObligationId>,
        /// Set of multi-party payjoin sessions that this wallet is participating in
        pub(crate) active_multi_party_payjoins: HashMap<BulletinBoardId, MultiPartyPayjoinSession>,
        /// UTXOs registered in the order book by this wallet
        pub(crate) registered_inputs: OrdSet<Outpoint>,
    }
);

impl<'a> WalletHandle<'a> {
    pub(crate) fn data(&self) -> &'a WalletData {
        &self.sim.wallet_data[self.id.0]
    }

    pub(crate) fn info(&self) -> &'a WalletInfo {
        &self.sim.wallet_info[self.data().last_wallet_info_id.0]
    }

    // TODO: this should take into account liabilties spending unconfirmed UTXOs. For which a CPFP cost model is needed
    // In the future in needs to take as arg the current mempool and somethign to predict the state of the mempool overtime
    #[allow(dead_code)]
    pub(crate) fn effective_balance(&self) -> Amount {
        let utxos: Vec<OutputHandle<'a>> = self.unspent_coins().collect();
        let outputs_amounts = utxos.iter().map(|output| output.data().amount).sum();

        outputs_amounts
    }

    /// Build coin selection candidates from this wallet's unspent coins.
    pub(crate) fn coin_candidates(&self) -> Vec<CoinCandidate> {
        self.unspent_coins()
            .map(|o| CoinCandidate {
                outpoint: o.outpoint(),
                amount_sats: o.data().amount.to_sat(),
                weight_wu: o.address().data().script_type.input_weight_wu(),
                is_segwit: o.address().data().script_type.is_segwit(),
            })
            .collect()
    }

    /// Build coin selection candidates for a specific set of outpoints.
    pub(crate) fn coin_candidates_for(&self, outpoints: &[Outpoint]) -> Vec<CoinCandidate> {
        outpoints
            .iter()
            .map(|op| {
                let o = op.with(self.sim);
                CoinCandidate {
                    outpoint: *op,
                    amount_sats: o.data().amount.to_sat(),
                    weight_wu: o.address().data().script_type.input_weight_wu(),
                    is_segwit: o.address().data().script_type.is_segwit(),
                }
            })
            .collect()
    }

    fn potentially_spendable_txos(&self) -> impl Iterator<Item = OutputHandle<'a>> + '_ {
        self.info()
            .confirmed_utxos
            .iter()
            .chain(self.info().unconfirmed_txos.iter())
            .map(|outpoint| OutputHandle::new(self.sim, *outpoint))
    }

    fn unspent_coins(&self) -> impl Iterator<Item = OutputHandle<'a>> + '_ {
        self.potentially_spendable_txos()
            .filter(|o| !self.info().unconfirmed_spends.contains(&o.outpoint()))
    }
    #[allow(dead_code)]
    fn double_spendable_coins(&self) -> impl Iterator<Item = OutputHandle<'a>> + '_ {
        self.potentially_spendable_txos()
            .filter(|o| self.info().unconfirmed_spends.contains(&o.outpoint()))
    }

    pub(crate) fn unhandled_payment_obligations(&self) -> Vec<PaymentObligationData> {
        let wallet_info = self.info();
        wallet_info
            .payment_obligations
            .clone()
            .iter()
            .filter(|po_id| !wallet_info.handled_payment_obligations.contains(po_id))
            // Do not offer paying again while a tx for this PO is already in the mempool;
            // handled_payment_obligations only updates on confirm, so without this the wallet
            // could build another tx reusing the same inputs (double-spend in `spends`).
            .filter(|po_id| {
                !wallet_info
                    .txid_to_payment_obligation_ids
                    .iter()
                    .any(|(txid, po_ids)| {
                        wallet_info.unconfirmed_transactions.contains(txid)
                            && po_ids.contains(po_id)
                    })
            })
            // Filter out POs that are not revealed yet
            .filter(|po| po.with(self.sim).data().reveal_time <= self.sim.current_timestep)
            .map(|po| po.with(self.sim).data().clone())
            .collect()
    }

    pub(crate) fn pending_cospend_proposals(&self) -> Vec<(BulletinBoardId, MessageId)> {
        self.sim
            .messages
            .iter()
            .filter(|message| !self.data().messages_processed.contains(&message.id))
            .filter(|message| message.from != self.id && message.to == self.id)
            .filter_map(|message| match &message.message {
                MessageType::ProposeCoSpend(bulletin_board_id) => {
                    Some((*bulletin_board_id, message.id))
                }
                MessageType::RegisterWalletInput(_) => None,
            })
            .collect()
    }

    pub(crate) fn active_cospend_sessions(&self) -> Vec<BulletinBoardId> {
        self.info()
            .active_multi_party_payjoins
            .iter()
            .filter_map(|(bulletin_board_id, session)| match &session.state {
                TxConstructionState::SentOutputs | TxConstructionState::SentReadyToSign => {
                    Some(*bulletin_board_id)
                }
                _ => None,
            })
            .collect()
    }

    pub(crate) fn spendable_utxos(&self) -> Vec<UtxoWithMetadata> {
        self.unspent_coins()
            .map(|o| UtxoWithMetadata {
                outpoint: o.outpoint(),
                amount: o.data().amount,
                owner: self.id,
            })
            .collect()
    }

    pub(crate) fn registered_input_outpoints(&self) -> Vec<Outpoint> {
        self.info().registered_inputs.iter().cloned().collect()
    }

    pub(crate) fn orderbook_utxos(&self) -> Vec<UtxoWithMetadata> {
        self.sim.get_orderbook_utxos()
    }

    pub(crate) fn pending_interests(&self) -> Vec<CospendInterest> {
        self.sim.cospend_interests.clone()
    }
}

impl<'a> WalletHandleMut<'a> {
    pub(crate) fn data_mut(&mut self) -> &mut WalletData {
        &mut self.sim.wallet_data[self.id.0]
    }

    fn info_mut(&mut self) -> &mut WalletInfo {
        let last_wallet_info_id = self.data().last_wallet_info_id;
        &mut self.sim.wallet_info[last_wallet_info_id.0]
    }

    #[allow(dead_code)]
    pub(crate) fn handle(&self) -> WalletHandle<'_> {
        WalletHandle {
            sim: self.sim,
            id: self.data().id,
        }
    }

    pub(crate) fn new_address(&mut self) -> AddressId {
        let id = AddressId(self.sim.address_data.len());
        self.sim.wallet_data[self.id.0].addresses.push(id);
        self.sim.address_data.push(AddressData {
            wallet_id: self.id,
            script_type: self.data().script_type,
        });
        id
    }

    fn participate_in_multi_party_payjoin(&mut self, bulletin_board_id: &BulletinBoardId) {
        let session = self
            .info()
            .active_multi_party_payjoins
            .get(bulletin_board_id)
            .unwrap();
        // TODO: construct tx template and contribute inputs / locking the po's and utxos to this session
        let state = session.state.clone();
        log::info!(
            "wallet id: {:?} participating in multi party payjoin session with state: {:?}",
            self.id,
            state
        );
        match state {
            TxConstructionState::AcceptedProposal => {
                // Outputs are contributed via ContributeOutputsToSession; nothing to do here.
                log::info!(
                    "wallet id: {:?} in AcceptedProposal state for bb {:?}, waiting for ContributeOutputsToSession",
                    self.id,
                    bulletin_board_id
                );
            }
            TxConstructionState::SentOutputs => {
                let inputs = session.inputs.clone();
                let t = SentOutputs::new(
                    self.sim,
                    *bulletin_board_id,
                    TxData {
                        inputs,
                        outputs: vec![],
                    },
                );
                let res = t.have_enough_outputs();
                if res.is_some() {
                    let mut updated_session = session.clone();
                    updated_session.state = TxConstructionState::SentReadyToSign;
                    self.info_mut()
                        .active_multi_party_payjoins
                        .insert(*bulletin_board_id, updated_session);
                    log::info!(
                        "Sent ready to sign for multi party payjoin session with bulletin board id: {:?}",
                        bulletin_board_id
                    );
                }
            }
            TxConstructionState::SentReadyToSign => {
                let t = SentReadyToSign::new(self.sim, *bulletin_board_id);
                let res = t.have_enough_ready_to_sign();
                if let Some(tx) = res {
                    // Only the participant with the lowest wallet ID broadcasts to avoid
                    // duplicate transactions (new_tx deduplicates by content, so all
                    // participants would get the same TxId, violating broadcast/received invariants).
                    let min_participant_id = self.sim.bulletin_boards[bulletin_board_id.0]
                        .messages
                        .iter()
                        .filter_map(|msg| match msg {
                            crate::bulletin_board::BroadcastMessageType::ContributeInputs(op) => {
                                Some(op.with(self.sim).wallet().id)
                            }
                            _ => None,
                        })
                        .min();
                    let is_broadcaster = min_participant_id == Some(self.id);

                    let tx_id = if is_broadcaster {
                        let id = self.spend_tx(tx);
                        self.broadcast(std::iter::once(id));
                        let po_ids = session.payment_obligation_ids.clone();
                        self.info_mut()
                            .txid_to_payment_obligation_ids
                            .insert(id, po_ids);
                        Some(id)
                    } else {
                        None
                    };
                    let mut updated_session = session.clone();
                    updated_session.state = TxConstructionState::Success(tx_id);
                    self.info_mut()
                        .active_multi_party_payjoins
                        .insert(*bulletin_board_id, updated_session);
                    log::info!(
                        "Multi party payjoin session successful with bulletin board id: {:?}",
                        bulletin_board_id
                    );
                }
            }
            TxConstructionState::Success(tx_id) => {
                log::info!("Multi party payjoin session successful: {:?}", tx_id);
            }
        }
    }

    fn register_input(&mut self, outpoint: &Outpoint) {
        if self.info().registered_inputs.contains(outpoint) {
            return;
        }
        let mut latest_info = self.info().clone();
        latest_info.registered_inputs.insert(*outpoint);
        self.update_info(latest_info);
        info!(
            "Wallet {:?} registered input {:?} in order book",
            self.id, outpoint
        );
    }

    pub(crate) fn update_info(&mut self, info: WalletInfo) {
        let id = WalletInfoId(self.sim.wallet_info.len());
        self.sim.wallet_info.push(info);
        self.data_mut().last_wallet_info_id = id;
    }

    pub(crate) fn do_action(&'a mut self, action: &Action) {
        match action {
            Action::Wait => {}
            Action::UnilateralPayments(po_ids, selected_inputs, change_amounts) => {
                self.handle_payment_obligations(po_ids, selected_inputs, change_amounts);
            }
            Action::AcceptCospendProposal((message_id, bulletin_board_id)) => {
                // Aggregator already pre-filled all inputs on the bulletin board.
                // Find our own inputs from the bulletin board's ContributeInputs messages.
                use crate::bulletin_board::BroadcastMessageType;
                let my_inputs: Vec<Input> = self.sim.bulletin_boards[bulletin_board_id.0]
                    .messages
                    .iter()
                    .filter_map(|msg| match msg {
                        BroadcastMessageType::ContributeInputs(op) => Some(op),
                        _ => None,
                    })
                    .filter(|op| {
                        self.info().confirmed_utxos.contains(op)
                            && !self.info().unconfirmed_spends.contains(op)
                    })
                    .map(|op| Input { outpoint: *op })
                    .collect();
                self.info_mut().active_multi_party_payjoins.insert(
                    *bulletin_board_id,
                    MultiPartyPayjoinSession {
                        payment_obligation_ids: vec![],
                        inputs: my_inputs,
                        state: TxConstructionState::AcceptedProposal,
                    },
                );
                self.data_mut().messages_processed.insert(*message_id);
            }
            Action::ProposeCospend(interests) => {
                for interest in interests {
                    self.sim.cospend_interests.push(interest.clone());
                }
            }
            Action::CreateAggregateProposal(interests) => {
                let bb_id = self.sim.create_bulletin_board();
                // Collect unique (outpoint, owner) pairs to avoid double-spending
                // when multiple interests share the same UTXO (e.g. taker proposes
                // the same UTXO against multiple makers).
                let mut seen_outpoints = std::collections::HashSet::new();
                let unique_utxos: Vec<_> = interests
                    .iter()
                    .flat_map(|i| i.utxos.iter())
                    .filter(|u| seen_outpoints.insert(u.outpoint))
                    // Skip UTXOs that have been spent since the interest was recorded.
                    // Interests are non-committal and may go stale between proposal and
                    // aggregation (e.g. the owner spent the coin unilaterally in the same
                    // tick before the aggregator ran).
                    .filter(|u| {
                        let info = &self.sim.wallet_info
                            [self.sim.wallet_data[u.owner.0].last_wallet_info_id.0];
                        info.confirmed_utxos.contains(&u.outpoint)
                            && !info.unconfirmed_spends.contains(&u.outpoint)
                    })
                    .collect();
                // Pre-fill all unique inputs on the bulletin board
                for u in &unique_utxos {
                    self.sim.add_message_to_bulletin_board(
                        bb_id,
                        crate::bulletin_board::BroadcastMessageType::ContributeInputs(u.outpoint),
                    );
                }
                // Invite each unique participant once
                let mut invited = std::collections::HashSet::new();
                for u in &unique_utxos {
                    if invited.insert(u.owner) {
                        self.sim.broadcast_message(
                            u.owner,
                            self.id,
                            MessageType::ProposeCoSpend(bb_id),
                        );
                    }
                }
                // Clear the handled interests
                self.sim
                    .cospend_interests
                    .retain(|i| !interests.contains(i));
            }
            Action::ContributeOutputsToSession(bulletin_board_id, po_ids, change_amounts) => {
                use crate::bulletin_board::BroadcastMessageType;
                let mut outputs = vec![];
                for po_id in po_ids {
                    let po = po_id.with(self.sim).data().clone();
                    let to_addr = po.to.with_mut(self.sim).new_address();
                    outputs.push(Output {
                        amount: po.amount,
                        address_id: to_addr,
                    });
                }
                for &change_amount in change_amounts {
                    let change_addr = self.new_address();
                    outputs.push(Output {
                        amount: change_amount,
                        address_id: change_addr,
                    });
                }
                for output in &outputs {
                    self.sim.add_message_to_bulletin_board(
                        *bulletin_board_id,
                        BroadcastMessageType::ContributeOutputs(*output),
                    );
                }
                let session = self
                    .info_mut()
                    .active_multi_party_payjoins
                    .get_mut(bulletin_board_id)
                    .unwrap();
                session.payment_obligation_ids = po_ids.clone();
                session.state = TxConstructionState::SentOutputs;
            }
            Action::ContinueParticipateInCospend(bulletin_board_id) => {
                self.participate_in_multi_party_payjoin(bulletin_board_id);
            }
            Action::RegisterInput(outpoints) => {
                for outpoint in outpoints {
                    self.register_input(outpoint);
                }
            }
        }
    }

    pub(crate) fn wake_up(&'a mut self) {
        let scorer = self.data().scorer.clone();
        // Clone strategies to allow passing &self to enumerate_candidate_actions
        // without conflicting with the borrow on strategies.strategies
        let strategies = self.data().strategies.clone();
        let mut all_actions = Vec::new();
        for strategy in strategies.strategies.iter() {
            all_actions.extend(strategy.enumerate_candidate_actions(self));
        }

        let action = all_actions
            .into_iter()
            .min_by_key(|action| scorer.action_cost(action, self))
            .unwrap_or(Action::Wait);
        info!("Wallet id: {:?} chose action: {:?}", self.id, action);
        self.do_action(&action);
    }

    fn handle_payment_obligations(
        &'a mut self,
        payment_obligation_ids: &[PaymentObligationId],
        selected_inputs: &[Outpoint],
        change_amounts: &[Amount],
    ) {
        // Build recipient outputs.
        let mut outputs = vec![];
        for po_id in payment_obligation_ids.iter() {
            let po = po_id.with(self.sim).data().clone();
            let to_addr = po.to.with_mut(self.sim).new_address();
            outputs.push(Output {
                amount: po.amount,
                address_id: to_addr,
            });
        }
        // Add pre-computed change outputs.
        for &change_amount in change_amounts.iter() {
            let change_addr = self.new_address();
            outputs.push(Output {
                amount: change_amount,
                address_id: change_addr,
            });
        }
        let tx_id = self.spend_tx(TxData {
            inputs: selected_inputs
                .iter()
                .map(|op| Input { outpoint: *op })
                .collect(),
            outputs,
        });
        self.info_mut()
            .txid_to_payment_obligation_ids
            .insert(tx_id, payment_obligation_ids.to_vec());
        self.broadcast(vec![tx_id]);
    }

    // TODO: refactor this? Do we event need this?
    fn spend_tx(&mut self, txdata: TxData) -> TxId {
        // TODO: assert this is my obligation
        let spend = self
            .new_tx(|tx, _| {
                tx.inputs = txdata.inputs;
                tx.outputs = txdata.outputs;
            })
            .id;

        spend
    }

    pub(crate) fn new_tx<F>(&mut self, build: F) -> TxHandle<'_>
    where
        F: FnOnce(&mut TxData, &Simulation),
    {
        let id = self.sim.new_tx(build);
        self.data_mut().own_transactions.push(id);
        TxHandle::new(self.sim, id)
    }

    pub(crate) fn broadcast(&mut self, txs: impl IntoIterator<Item = TxId>) -> BroadcastSetId {
        let mut wallet_info = self.info().clone();

        let txs = Vector::from_iter(txs);

        wallet_info.broadcast_transactions.append(txs.clone());

        // TODO refactor boilerplate for updating wallet ID
        let id = WalletInfoId(self.sim.wallet_info.len());
        self.sim.wallet_info.push(wallet_info);
        let data = self.data_mut();
        data.last_wallet_info_id = id;

        let res = self.sim.broadcast(txs);
        res.id
    }
}

define_entity!(
    PaymentObligation,
    {
        pub(crate) id: PaymentObligationId,
        pub(crate) deadline: TimeStep,
        pub(crate) reveal_time: TimeStep,
        pub(crate) amount: Amount,
        pub(crate) from: WalletId,
        pub(crate) to: WalletId,
    },
    {
        // TODO coin selection strategy agnostic (pessimal?) spendable balance lower
        // bound
    }
);

impl<'a> PaymentObligationHandle<'a> {
    pub(crate) fn data(&self) -> &'a PaymentObligationData {
        &self.sim.payment_data[self.id.0]
    }
}

define_entity!(Address, {
    pub(crate) wallet_id: WalletId,
    pub(crate) script_type: ScriptType,
    // TODO internal
    // TODO silent payments
}, {});

impl From<AddressData> for InputWeightPrediction {
    fn from(data: AddressData) -> Self {
        data.script_type.input_weight_prediction()
    }
}

impl From<AddressHandle<'_>> for InputWeightPrediction {
    fn from(address: AddressHandle<'_>) -> Self {
        Self::from(address.data().clone())
    }
}

// TODO traits?
impl<'a> AddressHandle<'a> {
    pub(crate) fn data(&'a self) -> &'a AddressData {
        &self.sim.address_data[self.id.0]
    }

    pub(crate) fn wallet(&self) -> WalletHandle<'a> {
        self.data().wallet_id.with(self.sim)
    }
}
