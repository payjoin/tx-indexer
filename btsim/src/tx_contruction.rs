// The aggregator pre-fills all inputs on the bulletin board and sends invitations.
// Each participant accepts the invitation (AcceptedProposal state), then contributes their outputs.
// After sending outputs (SentOutputs), each participant signals ready-to-sign.
// Any participant who observes enough ready-to-sign messages may broadcast the tx.

use crate::{
    bulletin_board::{BroadcastMessageType, BulletinBoardId},
    transaction::{Input, Output, TxData, TxId},
    wallet::PaymentObligationId,
    Simulation,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MultiPartyPayjoinSession {
    /// The payment obligations that are being handled in this session. Specific for each wallet
    pub(crate) payment_obligation_ids: Vec<PaymentObligationId>,
    /// The inputs this wallet is committing to the session
    pub(crate) inputs: Vec<Input>,
    /// The state of the session
    pub(crate) state: TxConstructionState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TxConstructionState {
    /// Wallet has accepted the aggregator's invitation; inputs are already known
    AcceptedProposal,
    SentOutputs,
    SentReadyToSign,
    Success(Option<TxId>),
}

#[derive(Debug)]
pub(crate) struct SentOutputs<'a> {
    pub(crate) bulletin_board_id: BulletinBoardId,
    pub(crate) tx_template: TxData,
    pub(crate) sim: &'a mut Simulation,
}

impl<'a> SentOutputs<'a> {
    pub(crate) fn new(
        sim: &'a mut Simulation,
        bulletin_board_id: BulletinBoardId,
        tx_template: TxData,
    ) -> Self {
        Self {
            bulletin_board_id,
            tx_template,
            sim,
        }
    }

    #[allow(dead_code)]
    fn read_txout_messages(&self) -> Vec<Output> {
        let messages = self.sim.bulletin_boards[self.bulletin_board_id.0]
            .messages
            .iter()
            .filter_map(|message| match message {
                BroadcastMessageType::ContributeOutputs(output) => Some(*output),
                _ => None,
            })
            .collect::<Vec<_>>();

        messages
    }

    pub(crate) fn have_enough_outputs(self) -> Option<SentReadyToSign<'a>> {
        // Broadcast my ready to sign message for all the inputs I have contributed
        for _ in 0..self.tx_template.inputs.len() {
            self.sim.add_message_to_bulletin_board(
                self.bulletin_board_id,
                BroadcastMessageType::ReadyToSign(),
            );
        }

        Some(SentReadyToSign::new(self.sim, self.bulletin_board_id))
    }
}

#[derive(Debug)]
pub(crate) struct SentReadyToSign<'a> {
    pub(crate) bulletin_board_id: BulletinBoardId,
    pub(crate) sim: &'a mut Simulation,
}

impl<'a> SentReadyToSign<'a> {
    pub(crate) fn new(sim: &'a mut Simulation, bulletin_board_id: BulletinBoardId) -> Self {
        Self {
            bulletin_board_id,
            sim,
        }
    }

    fn read_ready_to_sign_messages(&self) -> usize {
        self.sim.bulletin_boards[self.bulletin_board_id.0]
            .messages
            .iter()
            .filter(|message| matches!(message, BroadcastMessageType::ReadyToSign()))
            .count()
    }

    fn get_all_input_messages(&self) -> usize {
        self.sim.bulletin_boards[self.bulletin_board_id.0]
            .messages
            .iter()
            .filter(|message| matches!(message, BroadcastMessageType::ContributeInputs(_)))
            .count()
    }

    pub(crate) fn have_enough_ready_to_sign(self) -> Option<TxData> {
        let ready_to_sign_messages = self.read_ready_to_sign_messages();
        let n = self.get_all_input_messages();
        if ready_to_sign_messages < n {
            return None;
        }
        // Signatures are abstracted away; identical TxData is deduped when recorded on-chain.
        let messages = self.sim.bulletin_boards[self.bulletin_board_id.0]
            .messages
            .clone();
        let mut tx = TxData::default();
        for message in messages {
            match message {
                BroadcastMessageType::ContributeInputs(outpoint) => {
                    tx.inputs.push(crate::transaction::Input { outpoint });
                }
                BroadcastMessageType::ContributeOutputs(output) => {
                    tx.outputs.push(output);
                }
                _ => continue,
            }
        }

        Some(tx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        actions::{create_strategy, CompositeScorer, CompositeStrategy},
        transaction::{Input, Outpoint},
        SimulationBuilder,
    };
    use bitcoin::Amount;

    // Test harness helpers
    mod test_harness {
        use super::*;

        /// Creates a minimal simulation with a specified number of wallets
        pub fn create_minimal_simulation(num_wallets: usize) -> crate::Simulation {
            use crate::config::{ScorerConfig, WalletTypeConfig};
            let wallet_types = vec![WalletTypeConfig {
                name: "test".to_string(),
                count: num_wallets,
                strategies: vec!["UnilateralSpender".to_string()],
                scorer: ScorerConfig {
                    privacy_weight: 0.0,
                    payment_obligation_weight: 0.0,
                    min_fallback_plans: 0,
                },
                script_type: crate::script_type::ScriptType::P2tr,
            }];
            SimulationBuilder::new(42, wallet_types, 10, 1, 0).build()
        }

        /// Creates a mock output template for a wallet (inputs are not placed on the BB —
        /// the aggregator handles that separately)
        pub fn create_mock_tx_template(
            sim: &mut crate::Simulation,
            num_inputs: usize,
            num_outputs: usize,
        ) -> TxData {
            let default_scorer = CompositeScorer {
                privacy_bundle: crate::metrics::PrivacyBundle::default(),
                payment_obligation_weight: 0.0,
                min_fallback_plans: 0,
            };
            let wallet = sim.new_wallet(
                CompositeStrategy {
                    strategies: vec![create_strategy("UnilateralSpender").unwrap()],
                },
                default_scorer,
                crate::script_type::ScriptType::P2tr,
            );
            let address = wallet.with_mut(sim).new_address();

            // Dummy inputs (used only to determine how many ready-to-sign messages to broadcast)
            let inputs = (0..num_inputs)
                .map(|i| Input {
                    outpoint: Outpoint {
                        txid: TxId(i),
                        index: 0,
                    },
                })
                .collect();

            let outputs = (0..num_outputs)
                .map(|_| Output {
                    amount: Amount::from_sat(1000),
                    address_id: address,
                })
                .collect();

            TxData { inputs, outputs }
        }

        /// Simulates the aggregator pre-filling inputs on the bulletin board
        pub fn add_other_inputs(
            sim: &mut crate::Simulation,
            bulletin_board_id: BulletinBoardId,
            num_inputs: usize,
        ) {
            for i in 0..num_inputs {
                sim.add_message_to_bulletin_board(
                    bulletin_board_id,
                    BroadcastMessageType::ContributeInputs(Outpoint {
                        txid: TxId(100 + i),
                        index: 0,
                    }),
                );
            }
        }

        /// Adds output contributions from other participants to the bulletin board
        pub fn add_other_outputs(
            sim: &mut crate::Simulation,
            bulletin_board_id: BulletinBoardId,
            num_outputs: usize,
        ) {
            let default_scorer = CompositeScorer {
                privacy_bundle: crate::metrics::PrivacyBundle::default(),
                payment_obligation_weight: 0.0,
                min_fallback_plans: 0,
            };
            let wallet = sim.new_wallet(
                CompositeStrategy {
                    strategies: vec![create_strategy("UnilateralSpender").unwrap()],
                },
                default_scorer,
                crate::script_type::ScriptType::P2tr,
            );
            let address = wallet.with_mut(sim).new_address();

            for _ in 0..num_outputs {
                sim.add_message_to_bulletin_board(
                    bulletin_board_id,
                    BroadcastMessageType::ContributeOutputs(Output {
                        amount: Amount::from_sat(2000),
                        address_id: address,
                    }),
                );
            }
        }

        /// Adds ready-to-sign messages from other participants
        pub fn add_other_ready_to_sign(
            sim: &mut crate::Simulation,
            bulletin_board_id: BulletinBoardId,
            num_messages: usize,
        ) {
            for _ in 0..num_messages {
                sim.add_message_to_bulletin_board(
                    bulletin_board_id,
                    BroadcastMessageType::ReadyToSign(),
                );
            }
        }
    }

    #[test]
    fn test_state_machine() {
        let mut sim = test_harness::create_minimal_simulation(3);

        let bulletin_board_id = sim.create_bulletin_board();

        // Aggregator pre-fills all inputs (4 total: 2 "ours" + 2 from other participant)
        test_harness::add_other_inputs(&mut sim, bulletin_board_id, 4);
        // Other participant has contributed 2 outputs and 2 ready-to-sign messages
        test_harness::add_other_outputs(&mut sim, bulletin_board_id, 2);
        test_harness::add_other_ready_to_sign(&mut sim, bulletin_board_id, 2);

        // This wallet's tx template: 2 dummy inputs (length determines ready-to-sign count),
        // 1 output to contribute
        let tx_template = test_harness::create_mock_tx_template(&mut sim, 2, 1);

        // ContributeOutputsToSession broadcasts our output to the BB
        for output in tx_template.outputs.iter() {
            sim.add_message_to_bulletin_board(
                bulletin_board_id,
                BroadcastMessageType::ContributeOutputs(*output),
            );
        }

        // SentOutputs: broadcast ready-to-sign for our 2 inputs
        let sent_outputs = SentOutputs::new(&mut sim, bulletin_board_id, tx_template);
        let sent_ready = sent_outputs
            .have_enough_outputs()
            .expect("should proceed to SentReadyToSign");

        // SentReadyToSign: 2 (others) + 2 (ours) = 4 ready-to-sign >= 4 inputs → success
        let txdata = sent_ready
            .have_enough_ready_to_sign()
            .expect("should have enough ready to sign");

        // 4 inputs pre-filled by aggregator
        assert_eq!(txdata.inputs.len(), 4);
        // 3 outputs: 2 from others (2000 sats) + 1 from this wallet (1000 sats)
        assert_eq!(txdata.outputs.len(), 3);

        let output_1000_count = txdata
            .outputs
            .iter()
            .filter(|o| o.amount == Amount::from_sat(1000))
            .count();
        let output_2000_count = txdata
            .outputs
            .iter()
            .filter(|o| o.amount == Amount::from_sat(2000))
            .count();
        assert_eq!(
            output_1000_count, 1,
            "1 output at 1000 sats from this wallet"
        );
        assert_eq!(
            output_2000_count, 2,
            "2 outputs at 2000 sats from other participant"
        );
    }
}
