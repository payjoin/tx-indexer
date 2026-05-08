use std::path::Path;

use bitcoin::{Amount, Weight};
use graphviz_rust::{cmd::Format, dot_structures, printer::PrinterContext};
use im::{OrdMap, OrdSet, Vector};
use log::info;
use rand::Rng;
use rand_distr::Distribution;
use rand_distr::Geometric;
use rand_pcg::rand_core::{RngCore, SeedableRng};
use rand_pcg::Pcg64;
use serde::Serialize;

use crate::bulletin_board::BroadcastMessageType;
use crate::bulletin_board::BulletinBoardData;
use crate::bulletin_board::BulletinBoardId;
use crate::cospend::{CospendInterest, UtxoWithMetadata};
use crate::message::MessageType;
use crate::tx_contruction::MultiPartyPayjoinSession;
use crate::{
    actions::{create_strategy, CompositeScorer, CompositeStrategy},
    blocks::{
        BlockData, BlockHandle, BlockId, BlockInfo, BroadcastSetData, BroadcastSetHandleMut,
        BroadcastSetId, BroadcastSetInfo, BroadcastSetType,
    },
    config::WalletTypeConfig,
    economic_graph::EconomicGraph,
    message::{MessageData, MessageId},
    script_type::ScriptType,
    transaction::{InputId, Outpoint, TxData, TxId, TxInfo},
    wallet::{
        AddressData, AddressId, PaymentObligationData, PaymentObligationId, WalletData,
        WalletHandle, WalletId, WalletInfo, WalletInfoId,
    },
};

#[macro_use]
mod macros;
mod actions;
mod blocks;
mod bulletin_board;
mod coin_selection;
pub mod config;
mod cospend;
mod economic_graph;
mod graphviz;
mod message;
pub mod metrics;
pub mod script_type;
mod transaction;
mod tx_contruction;
mod wallet;

#[derive(Debug, Clone)]
struct PrngFactory(Pcg64);

impl PrngFactory {
    fn new(seed: u64) -> Self {
        Self(Pcg64::seed_from_u64(seed))
    }

    fn generate_prng(&mut self) -> Pcg64 {
        let seed = self.0.next_u64();
        Pcg64::seed_from_u64(seed)
    }
}

// total fee budget
//   - cap average over entire history, to work within estimated budget overall
//     - this is a soft fail, resulting in missed payments
//     - failure mode is broadcasting highest possible feerate at deadline, miss by time it takes to confirm

// cost function evaluates:
// - do nothings vs. unilateral build txn vs. build multiparty txn
// - if payment nearing deadline, sign tx discharging it
//   - immediately broadcast min relay fee txn based on deadline anxiety
//     - ... if it were not for privacy loss terms:
//       - desire not to unilaterally link inputs
//       - desire to minimize RBF / double spending / failed coinjoin sessions
//       - agent time preference and fallback strategy dictate balance between these
//   - batching strategy: powerset over payment obligations, loss diminishes with set size, and avoid evaluating below threshold
//   - link aversive strategy:
//     - never in unilateral txs
//     - simulate independent clients in multiparty txs
//
// experience measured loss as well based on how much the deadline was missed by? or just measure error objectively?

// Clock iterations:
// - new block
// - coinjoin opportunity alternating with new block. all agents available at all times.
// - listening for coinjoins
// - agents sleeping, simulated wallclock time
//   - parameterize with zef results?

// Lookup tables, generate in O(1) space without repetition as joins (datalog in rust thing?)
// - denomination combinations by size, ordered set of tiny vectors indicating denomination combinations
// - coin selection table, combinations of coins (how to limit? random ordering?)
// - other wallets' input combinations (up to size 2-3), cardinality estimation by counting quotient filter?

// Decomposition
// - given predefined outputs (payment targets)
//   - later, account for address reuse likelyhood in failed txn context?
//   - what about breakdown to unilateral spend on timeout?
// - take power set of candidate output set
// - sum density over a window, until some saturation limit
// - loss is (limit - value)/limit ?

// Peer coin selection
// - randomized score based on chain tip, xor metric over pairs
// - hypotehtical input combinations -> hypothetical decomposition evaluation
//
//

// https://ishaana.com/blog/wallet_fingerprinting/fingerprints_final.png

// petgraph tx graph
// rustworkx economic graph
// cost function for payments
// timestep abstraction, track txn broadcast
// mining, mempool
// vector clock of (top level) entity IDs? with nested spans?

// agent has state, re-evaluates at every timestep
// enumerate payment obligations
// check fees
// calculate cost for meeting obligation
// RBF or create txs estimated to meet deadline
// coin selection:
// - min size
// - core
// - privacy

// TODO AddressType extend with sizes
//
// OrdMap, OrdSet -> HashMap HashSet - where?
// just accept randomization and test that simulation is replicable even with

// TODO data() and info() fetchers from handle, deref into touple?
// TODO break down into define_id, define_handle, define_handle_mut, define_data, define_info, define_info_id
// TODO define_sequenced_entity (broadcast set, monad-ish) vs. define_mut_entity (wallet, append only updates)
// TODO handle enum for broadcastset data?
//

// TODO: unsued do we need this?
// #[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
// struct TxByFeerate(FeeRate, TxId);

// Wrapper type for timestep index
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Hash, Default)]
pub(crate) struct TimeStep(u64);

#[derive(Debug)]
pub struct SimulationBuilder {
    seed: u64,
    /// Wallet type configurations
    wallet_types: Vec<WalletTypeConfig>,
    /// Total number of timesteps for the simulation
    max_timestep: TimeStep,
    /// How many blocks are mined between timesteps
    block_interval: u64,
    /// Number of payment obligations to create
    num_payment_obligations: usize,
}

impl SimulationBuilder {
    pub fn new(
        seed: u64,
        wallet_types: Vec<WalletTypeConfig>,
        max_timestep: u64,
        block_interval: u64,
        num_payment_obligations: usize,
    ) -> Self {
        let total_wallets: usize = wallet_types.iter().map(|wt| wt.count).sum();
        debug_assert!(total_wallets >= 2, "Must have at least 2 wallets total");
        Self {
            seed,
            wallet_types,
            max_timestep: TimeStep(max_timestep),
            block_interval,
            num_payment_obligations,
        }
    }

    pub fn build(self) -> Simulation {
        let mut prng_factory = PrngFactory::new(self.seed);
        let economic_graph_prng = prng_factory.generate_prng();
        let mut sim = Simulation {
            wallet_data: Vec::new(),
            payment_data: Vec::new(),
            address_data: vec![AddressData {
                // First address is the "miner" address
                wallet_id: WalletId(0),
                script_type: ScriptType::P2tr,
            }],
            tx_data: Vec::new(),
            broadcast_set_data: Vec::new(),
            block_data: Vec::new(),
            current_timestep: TimeStep(0),
            prng_factory,
            spends: OrdMap::new(),
            wallet_info: Vec::new(),
            block_info: Vec::new(),
            tx_info: Vec::new(),
            broadcast_set_info: Vec::new(),
            messages: Vec::new(),
            bulletin_boards: Vec::new(),
            cospend_interests: Vec::new(),
            economic_graph: EconomicGraph::new(3, economic_graph_prng),
            config: SimulationConfig {
                max_timestep: self.max_timestep,
                block_interval: self.block_interval,
                num_payment_obligations: self.num_payment_obligations,
            },
        };

        // genesis block has empty coinbase
        sim.tx_data.push(TxData::default());
        sim.tx_info.push(TxInfo {
            fee: Amount::from_sat(0),
            weight: Weight::from_wu(0),
        });
        sim.block_data.push(BlockData {
            parent: None,
            coinbase_tx: TxId(0),
            confirmed_txs: vec![],
        });
        sim.block_info.push(BlockInfo {
            height: 0,
            spent: OrdSet::default(),
            created: OrdSet::default(),
            utxos: OrdSet::default(),
            all_confirmed_txs: OrdSet::default(),
            confirmed_txs: OrdSet::default(),
        });

        // empty initial broadcast set
        sim.broadcast_set_data.push(BroadcastSetData {
            data: BroadcastSetType::Block(sim.genesis_block()),
        });
        sim.broadcast_set_info.push(BroadcastSetInfo {
            parent_id: None,
            chain_tip_id: sim.genesis_block(),
            unconfirmed_txs: OrdSet::default(),
            invalidated_txs: OrdSet::default(),
        });

        // Create wallets according to their type configurations
        for wallet_type in &self.wallet_types {
            let scorer = CompositeScorer {
                privacy_bundle: crate::metrics::PrivacyBundle::default(),
                payment_obligation_weight: wallet_type.scorer.payment_obligation_weight,
                min_fallback_plans: wallet_type.scorer.min_fallback_plans,
            };

            for _ in 0..wallet_type.count {
                // Create new strategy instances for each wallet (can't clone Box<dyn Strategy>)
                let mut strategies = Vec::new();
                for strategy_name in &wallet_type.strategies {
                    match create_strategy(strategy_name) {
                        Ok(s) => strategies.push(s),
                        Err(e) => {
                            panic!("Failed to create strategy {}: {}", strategy_name, e);
                        }
                    }
                }
                let wallet_id = sim.new_wallet(
                    CompositeStrategy { strategies },
                    scorer.clone(),
                    wallet_type.script_type,
                );
                sim.economic_graph.grow(wallet_id);
            }
        }

        sim
    }
}

#[derive(Debug, Clone)]
struct SimulationConfig {
    max_timestep: TimeStep,
    block_interval: u64,
    num_payment_obligations: usize,
}

/// all entities are numbered sequentially
#[derive(Debug, Clone)]
pub struct Simulation {
    // primary information
    wallet_data: Vec<WalletData>,
    payment_data: Vec<PaymentObligationData>,
    address_data: Vec<AddressData>,
    tx_data: Vec<TxData>, // all are implicitly broadcast for now
    broadcast_set_data: Vec<BroadcastSetData>,
    // TODO mempools, = orderings / replacements of broadcast_sets
    block_data: Vec<BlockData>,
    current_timestep: TimeStep,
    prng_factory: PrngFactory,
    economic_graph: EconomicGraph<Pcg64>,
    config: SimulationConfig,
    /// Append only vector of p2p messages
    messages: Vec<MessageData>,

    /// Broadcast bulletin boards
    bulletin_boards: Vec<BulletinBoardData>,

    /// Pending cospend interests from takers (non-committal proposals)
    pub(crate) cospend_interests: Vec<CospendInterest>,

    // secondary information (indexes)
    /// Map of outpoints to the set of (txid, input index) pairs that spend them
    spends: OrdMap<Outpoint, OrdSet<InputId>>,
    wallet_info: Vec<WalletInfo>,
    block_info: Vec<BlockInfo>,
    tx_info: Vec<TxInfo>,
    broadcast_set_info: Vec<BroadcastSetInfo>,
}

impl<'a> Simulation {
    pub fn build_universe(&mut self) {
        let mut prng = self.prng_factory.generate_prng();
        let wallet_ids: Vec<WalletId> = self.wallet_data.iter().map(|w| w.id).collect();
        let addresses = wallet_ids
            .iter()
            .map(|&id| id.with_mut(self).new_address())
            .collect::<Vec<_>>();

        // For now we just mine a coinbase transaction for each wallet
        let mut i = 0;
        for address in addresses.iter() {
            for _ in 0..prng.random_range(5..10) {
                let _ = BroadcastSetHandleMut {
                    id: BroadcastSetId(i),
                    sim: self,
                }
                .construct_block_template(Weight::MAX_BLOCK)
                .mine(*address, self);

                self.assert_invariants();
                i += 1;
            }
        }

        // We'll set up some payment obligations
        self.setup_initial_payment_schedule();

        self.assert_invariants();
    }

    pub fn run(&mut self) -> SimulationResult {
        let max_timesteps = self.config.max_timestep;
        while self.current_timestep < max_timesteps {
            info!("Timestep {}", self.current_timestep.0);
            self.tick();
            // TODO: call this only in debug / testmode?
            self.assert_invariants();
        }
        SimulationResult::new(self)
    }

    fn tick(&mut self) {
        let wallet_ids = self.wallet_data.iter().map(|w| w.id).collect::<Vec<_>>();
        for wallet_id in wallet_ids.iter() {
            wallet_id.with_mut(self).wake_up();
        }

        if self
            .current_timestep
            .0
            .is_multiple_of(self.config.block_interval)
        {
            info!("Mining block");
            let bx_id = BroadcastSetId(self.broadcast_set_data.len() - 1);
            let bx_set_handle = bx_id.with_mut(self);
            bx_set_handle
                .construct_block_template(Weight::MAX_BLOCK)
                .mine(self.miner_address(), self);
        }

        self.current_timestep = TimeStep(self.current_timestep.0 + 1);
    }

    fn genesis_block(&self) -> BlockId {
        BlockId(0)
    }

    fn miner_address(&mut self) -> AddressId {
        AddressId(0)
    }

    fn create_bulletin_board(&mut self) -> BulletinBoardId {
        let id = BulletinBoardId(self.bulletin_boards.len());
        self.bulletin_boards.push(BulletinBoardData {
            id,
            messages: Vec::new(),
        });
        id
    }

    fn add_message_to_bulletin_board(
        &mut self,
        bulletin_board_id: BulletinBoardId,
        message: BroadcastMessageType,
    ) {
        self.bulletin_boards[bulletin_board_id.0]
            .messages
            .push(message);
    }

    fn broadcast_message(&mut self, to: WalletId, from: WalletId, message: MessageType) {
        let id = MessageId(self.messages.len());
        self.messages.push(MessageData {
            id,
            message,
            from,
            to,
        });
    }

    /// Creates a random payment obligation between two wallets.
    fn setup_initial_payment_schedule(&mut self) {
        let mut prng = self.prng_factory.generate_prng();
        if self.config.max_timestep.0 - self.current_timestep.0 < 2 {
            // Not enough timesteps left to create a payment obligation
            return;
        }
        let current_timestep = 0;
        let mut i = 0;
        while i < self.config.num_payment_obligations {
            for (from, to) in self.economic_graph.next_ordered_payment_pairs() {
                debug_assert!(from != to, "circular payment obligation");
                // TODO: should be a configurable or dependent on the balance of each wallet?
                let reveal_time =
                    prng.random_range(current_timestep + 1..self.config.max_timestep.0 / 2); // Payments shouldnt be revealed too late. Aim to have them revealed within the first half of the simulation.
                let deadline = reveal_time
                    + std::cmp::min(
                        self.config.max_timestep.0,
                        Geometric::new(1.0 / (self.config.max_timestep.0 as f64 / 2.0))
                            .unwrap()
                            .sample(&mut prng),
                    );
                // TODO: instead of hardcoded average amount we should predict the balance of each wallet
                let amount = Geometric::new(1.0 / 10_000.0).unwrap().sample(&mut prng);
                // First insert payment obligation into simulation
                let payment_obligation_id = PaymentObligationId(self.payment_data.len());
                self.payment_data.push(PaymentObligationData {
                    id: payment_obligation_id,
                    amount: Amount::from_sat(amount),
                    from,
                    to,
                    deadline: TimeStep(deadline),
                    reveal_time: TimeStep(reveal_time),
                });

                // Then insert into to_wallet's expected payments
                let last_wallet_info_id = self.wallet_data[to.0].last_wallet_info_id;
                self.wallet_info[last_wallet_info_id.0]
                    .expected_payments
                    .insert(payment_obligation_id);

                // Then insert into from_wallet's payment obligations
                let last_wallet_info_id = self.wallet_data[from.0].last_wallet_info_id;
                self.wallet_info[last_wallet_info_id.0]
                    .payment_obligations
                    .insert(payment_obligation_id);
                i += 1;
            }
        }
    }

    fn new_wallet(
        &mut self,
        strategies: CompositeStrategy,
        scorer: CompositeScorer,
        script_type: ScriptType,
    ) -> WalletId {
        // TODO wallet_handle?
        let last_wallet_info_id = WalletInfoId(self.wallet_info.len());
        self.wallet_info.push(WalletInfo {
            broadcast_set_id: BroadcastSetId(self.broadcast_set_data.len() - 1), // FIXME refactor
            payment_obligations: OrdSet::<PaymentObligationId>::default(),
            expected_payments: OrdSet::<PaymentObligationId>::default(),
            broadcast_transactions: Vector::<TxId>::default(),
            received_transactions: Vector::<TxId>::default(),
            unconfirmed_transactions: OrdSet::<TxId>::default(),
            unconfirmed_txos: OrdSet::<Outpoint>::default(),
            confirmed_utxos: OrdSet::<Outpoint>::default(),
            unconfirmed_spends: OrdSet::<Outpoint>::default(),
            txid_to_payment_obligation_ids: im::HashMap::<TxId, Vec<PaymentObligationId>>::default(
            ),
            handled_payment_obligations: OrdSet::<PaymentObligationId>::default(),
            active_multi_party_payjoins:
                im::HashMap::<BulletinBoardId, MultiPartyPayjoinSession>::default(),
            registered_inputs: OrdSet::<Outpoint>::default(),
        });

        let id = WalletId(self.wallet_data.len());
        self.wallet_data.push(WalletData {
            id,
            last_wallet_info_id,
            addresses: Vec::default(),
            own_transactions: Vec::default(),
            messages_processed: OrdSet::<MessageId>::default(),
            strategies,
            scorer,
            script_type,
        });
        id
    }

    fn new_tx<F>(&mut self, build: F) -> TxId
    where
        F: FnOnce(&mut TxData, &Simulation),
    {
        let txid = TxId(self.tx_data.len());
        let mut tx = TxData::default();

        build(&mut tx, self);

        let tx_info = TxInfo::new(&tx, self);

        // TODO check all inputs unspent
        // TODO check transaction validity, calculate input values, feerate, weight

        for (i, input) in tx.inputs.iter().enumerate() {
            if !self.spends.contains_key(&input.outpoint) {
                self.spends
                    .insert(input.outpoint, OrdSet::<InputId>::default());
            }
            self.spends[&input.outpoint].insert(InputId { txid, index: i });
        }
        self.tx_data.push(tx);
        self.tx_info.push(tx_info);

        txid
    }

    fn get_wallet_handles(&'a self) -> impl Iterator<Item = WalletHandle<'a>> {
        let max_id = self.wallet_data.len();
        (0..max_id).map(|id| WalletId(id).with(self))
    }

    fn new_block(&'a mut self, data: BlockData, info: BlockInfo) -> BlockHandle<'a> {
        let id = BlockId(self.block_data.len());

        self.block_data.push(data);
        self.block_info.push(info); // TODO compute this here by accepting BlockTemplate and coinbase_tx?

        // TODO refactor, return mut handle from process_block, clean up IDs
        BroadcastSetId(self.broadcast_set_data.len() - 1)
            .with_mut(self)
            .process_block(id);

        id.with(self)
    }

    fn broadcast(&'a mut self, txs: impl IntoIterator<Item = TxId>) -> BroadcastSetHandleMut<'a> {
        // TODO BroadcastSetHandle
        let bx_id = BroadcastSetId(self.broadcast_set_data.len() - 1);
        bx_id.with_mut(self).broadcast(txs)
    }

    pub(crate) fn get_orderbook_utxos(&'a self) -> Vec<UtxoWithMetadata> {
        self.wallet_data
            .iter()
            .flat_map(|wallet| {
                let info = &self.wallet_info[wallet.last_wallet_info_id.0];
                info.registered_inputs
                    .iter()
                    .filter(|outpoint| {
                        info.confirmed_utxos.contains(outpoint)
                            && !info.unconfirmed_spends.contains(outpoint)
                    })
                    .map(|outpoint| UtxoWithMetadata {
                        outpoint: *outpoint,
                        amount: outpoint.with(self).data().amount,
                        owner: wallet.id,
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    // FIXME debug only code?
    fn assert_invariants(&self) {
        assert!(self
            .broadcast_set_info
            .last()
            .unwrap()
            .unconfirmed_txs
            .clone()
            .intersection(self.block_info.last().unwrap().all_confirmed_txs.clone())
            .is_empty());

        self.wallet_info.iter().for_each(|w| {
            assert!(w
                .confirmed_utxos
                .clone()
                .intersection(w.unconfirmed_txos.clone())
                .is_empty());
        });

        self.wallet_info.iter().for_each(|w| {
            assert!(
                OrdSet::<TxId>::from_iter(w.broadcast_transactions.clone().into_iter())
                    .intersection(OrdSet::from_iter(
                        w.received_transactions.clone().into_iter()
                    ))
                    .is_empty()
            );
        });

        self.spends.iter().for_each(|(outpoint, inputs)| {
            assert!(
                inputs.len() == 1,
                "Outpoint {:?} spent multiple times",
                outpoint
            );
        });

        // TODO: assert that expected payments and payment obligations met
        // TODO: assert that for each payment obligation, the from wallet has the expected payment

        // TODO for all wallets, ensure their confirmed and unconfirmed utxos form a partition (their intersections are empty and their union is describes the corresponding block info and broadcast state)
        // take union and compare size to sum of sizes, and check equality with global structures
    }
}

impl std::fmt::Display for Simulation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "=== Simulation State ===")?;
        writeln!(f, "Current Timestep: {}", self.current_timestep.0)?;
        writeln!(f, "Max Timesteps: {}", self.config.max_timestep.0)?;
        writeln!(f, "\nWallets: {}", self.wallet_data.len())?;

        for (i, wallet) in self.wallet_data.iter().enumerate() {
            writeln!(f, "\nWallet {}:", i)?;
            writeln!(f, "  Own Transactions: {:?}", wallet.own_transactions)?;
            writeln!(f, "  Addresses: {:?}", wallet.addresses)?;
            writeln!(
                f,
                "  Broadcast Transactions: {:?}",
                self.wallet_info[wallet.last_wallet_info_id.0].broadcast_transactions
            )?;
            writeln!(
                f,
                "  Received Transactions: {:?}",
                self.wallet_info[wallet.last_wallet_info_id.0].received_transactions
            )?;
            writeln!(
                f,
                "  Unconfirmed Transactions: {:?}",
                self.wallet_info[wallet.last_wallet_info_id.0].unconfirmed_transactions
            )?;
            writeln!(
                f,
                "  Confirmed UTXOs: {:?}",
                self.wallet_info[wallet.last_wallet_info_id.0].confirmed_utxos
            )?;
            writeln!(
                f,
                "  Unconfirmed UTXOs: {:?}",
                self.wallet_info[wallet.last_wallet_info_id.0].unconfirmed_txos
            )?;
            writeln!(
                f,
                "  Unconfirmed Spends: {:?}",
                self.wallet_info[wallet.last_wallet_info_id.0].unconfirmed_spends
            )?;
        }

        writeln!(f, "\nPayment Obligations: {}", self.payment_data.len())?;
        for (i, payment) in self.payment_data.iter().enumerate() {
            writeln!(
                f,
                "\nPayment {}: Amount: {}, From: Wallet {}, To: Address {}, Deadline: Timestep {}",
                i, payment.amount, payment.from.0, payment.to.0, payment.deadline.0
            )?;
        }

        writeln!(f, "\nPeer Messages: {}", self.messages.len())?;
        for (i, message) in self.messages.iter().enumerate() {
            writeln!(
                f,
                "\nMessage {}: From: Wallet {}, To: Wallet {:?}, Message Type: {:?}",
                i, message.from.0, message.to, message.message
            )?;
        }

        writeln!(f, "\nSpends: {}", self.spends.len())?;
        for (i, spend) in self.spends.iter().enumerate() {
            writeln!(f, "Spend {}: {:?}", i, spend)?;
        }

        writeln!(f, "\nBlocks: {}", self.block_data.len())?;
        writeln!(f, "Broadcast Sets: {}", self.broadcast_set_data.len())?;

        Ok(())
    }
}

// left of here: This should be a static container for sim results, and impl serialize / deserialize so we can save the results to ondisk
// Util methods dont seem too useful here.
pub struct SimulationResult {
    tx_graph: dot_structures::Graph,
    sim: Simulation,
}

pub struct WalletUtxoStats {
    pub wallet_id: usize,
    pub dust_count: usize,
    pub total_count: usize,
    pub p50: Option<Amount>,
    pub p90: Option<Amount>,
}

#[derive(Serialize)]
struct SimulationResultJson {
    total_payment_obligations: usize,
    percentage_payment_obligations_missed: f64,
    total_block_weight_wu: u64,
    average_fee_cost_sats: u64,
    dust_utxo_count: usize,
    utxo_size_distribution_sats: Vec<u64>,
    wallet_utxo_stats: Vec<WalletUtxoStatsJson>,
}

#[derive(Serialize)]
struct WalletUtxoStatsJson {
    wallet_id: usize,
    dust_count: usize,
    total_count: usize,
    p50_sats: Option<u64>,
    p90_sats: Option<u64>,
}

impl SimulationResult {
    pub fn new(sim: &Simulation) -> Self {
        Self {
            tx_graph: sim.draw_tx_graph(),
            sim: sim.clone(),
        }
    }

    /// Count of missed payment obligations per wallet, computed from current sim state.
    pub fn missed_payment_obligations(&self) -> Vec<(usize, usize)> {
        let mut out = Vec::new();
        for wallet in self.sim.get_wallet_handles() {
            let handled = wallet.info().handled_payment_obligations.clone();
            let obligations = wallet.info().payment_obligations.clone();
            let diff = handled.difference(obligations);
            out.push((wallet.data().id.0, diff.len()));
        }
        out
    }

    /// Block weights for each block, computed from current sim state.
    pub fn block_weights(&self) -> Vec<u64> {
        self.sim
            .block_data
            .iter()
            .map(|block| {
                let mut block_weight_wu = block.coinbase_tx.with(&self.sim).info().weight.to_wu();
                for txid in &block.confirmed_txs {
                    block_weight_wu += txid.with(&self.sim).info().weight.to_wu();
                }
                block_weight_wu
            })
            .collect()
    }

    /// Total number of payment obligations, computed from current sim state.
    pub fn total_payment_obligations(&self) -> usize {
        self.sim.payment_data.len()
    }

    /// Percentage of payment obligations missed, computed from current sim state.
    pub fn percentage_of_payment_obligations_missed(&self) -> f64 {
        let total = self.total_payment_obligations();
        if total == 0 {
            return 0.0;
        }
        self.missed_payment_obligations()
            .iter()
            .map(|(_, count)| count)
            .sum::<usize>() as f64
            / total as f64
    }

    /// Total block weight, computed from current sim state.
    pub fn total_block_weight(&self) -> u64 {
        self.block_weights().iter().sum::<u64>()
    }

    /// Average fee cost across non-coinbase transactions, computed from current sim state.
    pub fn average_fee_cost(&self) -> Amount {
        let mut total_fee_sat = 0u64;
        let mut counted = 0u64;
        for (tx, info) in self.sim.tx_data.iter().zip(self.sim.tx_info.iter()) {
            if tx.inputs.is_empty() {
                continue;
            }
            total_fee_sat = total_fee_sat.saturating_add(info.fee.to_sat());
            counted = counted.saturating_add(1);
        }
        if counted == 0 {
            return Amount::from_sat(0);
        }
        Amount::from_sat(total_fee_sat / counted)
    }

    /// Count of dust UTXOs (<= 546 sats), computed from confirmed UTXOs.
    pub fn dust_utxo_count(&self) -> usize {
        const DUST_THRESHOLD_SATS: u64 = 546;
        self.sim
            .wallet_data
            .iter()
            .flat_map(|wallet| {
                let info = &self.sim.wallet_info[wallet.last_wallet_info_id.0];
                info.confirmed_utxos.iter()
            })
            .filter(|outpoint| {
                outpoint.with(&self.sim).data().amount.to_sat() <= DUST_THRESHOLD_SATS
            })
            .count()
    }

    /// Sorted list of confirmed UTXO sizes, computed from current sim state.
    pub fn utxo_size_distribution(&self) -> Vec<Amount> {
        let mut amounts: Vec<Amount> = self
            .sim
            .wallet_data
            .iter()
            .flat_map(|wallet| {
                let info = &self.sim.wallet_info[wallet.last_wallet_info_id.0];
                info.confirmed_utxos.iter()
            })
            .map(|outpoint| outpoint.with(&self.sim).data().amount)
            .collect();
        amounts.sort_by_key(|amount| amount.to_sat());
        amounts
    }

    /// Per-wallet dust counts and size percentiles (p50, p90) for confirmed UTXOs.
    pub fn wallet_utxo_stats(&self) -> Vec<WalletUtxoStats> {
        const DUST_THRESHOLD_SATS: u64 = 546;
        self.sim
            .wallet_data
            .iter()
            .map(|wallet| {
                let info = &self.sim.wallet_info[wallet.last_wallet_info_id.0];
                let mut amounts: Vec<Amount> = info
                    .confirmed_utxos
                    .iter()
                    .map(|outpoint| outpoint.with(&self.sim).data().amount)
                    .collect();
                amounts.sort_by_key(|amount| amount.to_sat());
                let dust_count = amounts
                    .iter()
                    .filter(|amount| amount.to_sat() <= DUST_THRESHOLD_SATS)
                    .count();
                let total_count = amounts.len();
                WalletUtxoStats {
                    wallet_id: wallet.id.0,
                    dust_count,
                    total_count,
                    p50: percentile_amount(&amounts, 0.50),
                    p90: percentile_amount(&amounts, 0.90),
                }
            })
            .collect()
    }

    /// Save the transaction graph to a file.
    pub fn save_tx_graph(&self, path: impl AsRef<Path>) {
        let graph_svg = graphviz_rust::exec(
            self.tx_graph.clone(),
            &mut PrinterContext::default(),
            vec![Format::Svg.into()],
        )
        .unwrap();
        std::fs::write(path, graph_svg).unwrap();
    }

    /// Save simulation results as a JSON file.
    pub fn save_results_json(&self, path: impl AsRef<Path>) {
        let utxo_sizes = self
            .utxo_size_distribution()
            .into_iter()
            .map(|amount| amount.to_sat())
            .collect();
        let wallet_utxo_stats = self
            .wallet_utxo_stats()
            .into_iter()
            .map(|stats| WalletUtxoStatsJson {
                wallet_id: stats.wallet_id,
                dust_count: stats.dust_count,
                total_count: stats.total_count,
                p50_sats: stats.p50.map(|amount| amount.to_sat()),
                p90_sats: stats.p90.map(|amount| amount.to_sat()),
            })
            .collect();
        let result = SimulationResultJson {
            total_payment_obligations: self.total_payment_obligations(),
            percentage_payment_obligations_missed: self.percentage_of_payment_obligations_missed(),
            total_block_weight_wu: self.total_block_weight(),
            average_fee_cost_sats: self.average_fee_cost().to_sat(),
            dust_utxo_count: self.dust_utxo_count(),
            utxo_size_distribution_sats: utxo_sizes,
            wallet_utxo_stats,
        };
        let file = std::fs::File::create(path).unwrap();
        serde_json::to_writer_pretty(file, &result).unwrap();
    }
    // TODO: anon set metrics
}

fn percentile_amount(sorted: &[Amount], percentile: f64) -> Option<Amount> {
    if sorted.is_empty() {
        return None;
    }
    let n = sorted.len() as f64;
    let rank = (percentile * n).ceil().max(1.0);
    let idx = (rank as usize).saturating_sub(1);
    sorted.get(idx).copied()
}

#[cfg(test)]
mod tests {
    use bdk_coin_select::{Target, TargetFee, TargetOutputs};
    use im::{ordset, vector};

    use crate::transaction::{Input, Output};

    use super::*;

    #[test]
    fn test_universe() {
        use crate::config::{ScorerConfig, WalletTypeConfig};
        let wallet_types = vec![WalletTypeConfig {
            name: "unilateral_spender".to_string(),
            count: 5,
            strategies: vec!["UnilateralSpender".to_string()],
            scorer: ScorerConfig {
                privacy_weight: 2.0,
                payment_obligation_weight: 1.0,
                min_fallback_plans: 0,
            },
            script_type: ScriptType::P2tr,
        }];
        let mut sim = SimulationBuilder::new(42, wallet_types, 20, 1, 10).build();
        sim.assert_invariants();
        sim.build_universe();
        let result = sim.run();
        sim.assert_invariants();

        // Assert simulation completed successfully
        assert!(
            result.total_payment_obligations() > 0,
            "Simulation should create payment obligations"
        );
        assert!(
            result.percentage_of_payment_obligations_missed() < 1.0,
            "Not all obligations should be missed"
        );

        assert_eq!(
            result.percentage_of_payment_obligations_missed(),
            0.0,
            "With seed 42, missed percentage should be deterministic"
        );
        assert!(
            result.total_block_weight() > 0,
            "Simulation should consume some block space"
        );
    }

    #[test]
    fn test_cospend_universe() {
        use crate::config::{ScorerConfig, WalletTypeConfig};
        let wallet_types = vec![
            WalletTypeConfig {
                name: "participant".to_string(),
                count: 4,
                strategies: vec!["MultipartyStrategy".to_string()],
                scorer: ScorerConfig {
                    privacy_weight: 1.0,
                    payment_obligation_weight: 2.0,
                    min_fallback_plans: 0,
                },
                script_type: ScriptType::P2wpkh,
            },
            WalletTypeConfig {
                name: "aggregator".to_string(),
                count: 1,
                strategies: vec!["AggregatorStrategy".to_string()],
                scorer: ScorerConfig {
                    privacy_weight: 0.0,
                    payment_obligation_weight: 0.0,
                    min_fallback_plans: 0,
                },
                script_type: ScriptType::P2wpkh,
            },
        ];
        let mut sim = SimulationBuilder::new(42, wallet_types, 15, 1, 5).build();
        sim.assert_invariants();
        sim.build_universe();
        let result = sim.run();
        sim.assert_invariants();

        println!("result: {:?}", result.total_payment_obligations());

        assert!(
            result.total_payment_obligations() > 0,
            "Simulation should create payment obligations"
        );
    }

    #[test]
    fn it_works() {
        use crate::config::{ScorerConfig, WalletTypeConfig};
        let wallet_types = vec![WalletTypeConfig {
            name: "default".to_string(),
            count: 2,
            strategies: vec!["UnilateralSpender".to_string(), "BatchSpender".to_string()],
            scorer: ScorerConfig {
                privacy_weight: 2.0,
                payment_obligation_weight: 1.0,
                min_fallback_plans: 0,
            },
            script_type: ScriptType::P2tr,
        }];
        let mut sim = SimulationBuilder::new(42, wallet_types, 20, 1, 10).build();
        sim.assert_invariants();

        use crate::actions::{create_strategy, CompositeScorer};
        let default_scorer = CompositeScorer {
            privacy_bundle: crate::metrics::PrivacyBundle::default(),
            payment_obligation_weight: 1.0,
            min_fallback_plans: 0,
        };
        let alice_strategies = vec![
            create_strategy("UnilateralSpender").unwrap(),
            create_strategy("BatchSpender").unwrap(),
        ];
        let bob_strategies = vec![
            create_strategy("UnilateralSpender").unwrap(),
            create_strategy("BatchSpender").unwrap(),
        ];
        let alice = sim.new_wallet(
            CompositeStrategy {
                strategies: alice_strategies,
            },
            default_scorer.clone(),
            ScriptType::P2tr,
        );
        sim.assert_invariants();
        let bob = sim.new_wallet(
            CompositeStrategy {
                strategies: bob_strategies,
            },
            default_scorer,
            ScriptType::P2tr,
        );
        sim.assert_invariants();

        let alice_coinbase_addr = alice.with_mut(&mut sim).new_address();
        sim.assert_invariants();

        // TODO sim.current_broadcast_set()
        let initial_bx = BroadcastSetHandleMut {
            id: BroadcastSetId(0),
            sim: &mut sim,
        };

        let coinbase_tx = initial_bx
            .construct_block_template(Weight::MAX_BLOCK)
            .mine(alice_coinbase_addr, &mut sim)
            .coinbase_tx()
            .id;

        sim.assert_invariants();

        assert_eq!(alice.with(&sim).data().own_transactions, vec![coinbase_tx]);
        assert_eq!(
            alice.with(&sim).info().confirmed_utxos,
            OrdSet::from_iter(coinbase_tx.with(&sim).outpoints())
        );

        // TODO coinbase maturity

        let payment = PaymentObligationData {
            id: PaymentObligationId(0),
            amount: Amount::from_int_btc(20),
            from: WalletId(0),
            to: bob,
            deadline: TimeStep(2), // TODO 102
            reveal_time: TimeStep(1),
        };
        sim.assert_invariants();

        let bob_payment_addr = bob.with_mut(&mut sim).new_address();
        sim.assert_invariants();
        let alice_change_addr = alice.with_mut(&mut sim).new_address();
        sim.assert_invariants();

        let target = Target {
            fee: TargetFee {
                rate: bdk_coin_select::FeeRate::from_sat_per_vb(1.0),
                replace: None,
            },
            outputs: TargetOutputs {
                value_sum: payment.amount.to_sat(),
                weight_sum: ScriptType::P2tr.output_weight_wu(),
                n_outputs: 1,
            },
        };

        let candidates = alice.with(&sim).coin_candidates();
        let (selected_outpoints, change_amounts) =
            crate::coin_selection::select_bnb(&candidates, target)
                .unwrap_or_else(|| crate::coin_selection::select_all(&candidates, target));

        let spend = alice
            .with_mut(&mut sim)
            .new_tx(|tx, _sim| {
                tx.inputs = selected_outpoints
                    .iter()
                    .map(|op| Input { outpoint: *op })
                    .collect();

                tx.outputs = vec![Output {
                    amount: payment.amount,
                    address_id: bob_payment_addr,
                }];
                for &change_amount in &change_amounts {
                    tx.outputs.push(Output {
                        amount: change_amount,
                        address_id: alice_change_addr,
                    });
                }
            })
            .id;
        sim.assert_invariants();

        assert_eq!(spend, TxId(2));

        assert_eq!(spend.with(&sim).info().weight, Weight::from_wu(616));

        assert_eq!(
            alice.with(&sim).data().own_transactions,
            vec![coinbase_tx, spend]
        );

        assert_eq!(
            alice.with(&sim).info().broadcast_transactions,
            Vector::default()
        );

        // these fields are not updated until broadcast
        assert_eq!(
            alice.with(&sim).info().confirmed_utxos,
            OrdSet::from_iter(coinbase_tx.with(&sim).outpoints())
        );
        assert_eq!(alice.with(&sim).info().unconfirmed_spends, ordset![]);

        alice.with_mut(&mut sim).broadcast(std::iter::once(spend));

        assert_eq!(
            alice.with(&sim).info().unconfirmed_spends,
            OrdSet::from_iter(coinbase_tx.with(&sim).outpoints())
        );

        assert_eq!(
            alice.with(&sim).info().unconfirmed_txos,
            OrdSet::from_iter(spend.with(&sim).outpoints().skip(1))
        );

        assert_eq!(
            bob.with(&sim).info().unconfirmed_txos,
            OrdSet::from_iter(spend.with(&sim).outpoints().take(1))
        );

        assert_eq!(
            alice.with(&sim).info().broadcast_transactions,
            vector![spend]
        );

        assert!(bob.with(&sim).info().received_transactions.contains(&spend));

        // Mine another block to confirm the transaction
        let miner_addr = alice.with_mut(&mut sim).new_address();
        let block_bx = BroadcastSetHandleMut {
            id: BroadcastSetId(sim.broadcast_set_data.len() - 1),
            sim: &mut sim,
        };

        let _block = block_bx
            .construct_block_template(Weight::MAX_BLOCK)
            .mine(miner_addr, &mut sim);

        sim.assert_invariants();

        // Verify transaction is now confirmed and UTXOs are updated
        assert!(alice
            .with(&sim)
            .info()
            .confirmed_utxos
            .contains(&spend.with(&sim).outpoints().nth(1).unwrap()));
        assert!(bob
            .with(&sim)
            .info()
            .confirmed_utxos
            .contains(&spend.with(&sim).outpoints().next().unwrap()));

        // Verify the spend transaction is no longer unconfirmed
        assert!(alice.with(&sim).info().unconfirmed_txos.is_empty());
        assert!(bob.with(&sim).info().unconfirmed_txos.is_empty());
    }

    #[test]
    fn test_weight_prediction_by_script_type() {
        use crate::config::{ScorerConfig, WalletTypeConfig};
        use bitcoin::transaction::{predict_weight, InputWeightPrediction};

        let script_types = [ScriptType::P2tr, ScriptType::P2wpkh, ScriptType::P2pkh];

        for script_type in script_types {
            let wallet_types = vec![WalletTypeConfig {
                name: "weight_test".to_string(),
                count: 2,
                strategies: vec!["UnilateralSpender".to_string()],
                scorer: ScorerConfig {
                    privacy_weight: 2.0,
                    payment_obligation_weight: 1.0,
                    min_fallback_plans: 0,
                },
                script_type,
            }];

            let mut sim = SimulationBuilder::new(42, wallet_types, 5, 1, 0).build();
            let wallet_id = WalletId(0);
            let funding_addr = wallet_id.with_mut(&mut sim).new_address();
            let spend_addr = wallet_id.with_mut(&mut sim).new_address();

            let funding_tx = sim.new_tx(|tx, _| {
                tx.outputs.push(Output {
                    amount: Amount::from_sat(1_000),
                    address_id: funding_addr,
                });
            });

            let spend_tx = sim.new_tx(|tx, _| {
                tx.inputs.push(Input {
                    outpoint: Outpoint {
                        txid: funding_tx,
                        index: 0,
                    },
                });
                tx.outputs.push(Output {
                    amount: Amount::from_sat(900),
                    address_id: spend_addr,
                });
            });

            let expected_input = match script_type {
                ScriptType::P2tr => InputWeightPrediction::P2TR_KEY_DEFAULT_SIGHASH,
                ScriptType::P2wpkh => InputWeightPrediction::P2WPKH_MAX,
                ScriptType::P2pkh => InputWeightPrediction::P2PKH_COMPRESSED_MAX,
            };
            let expected_output_len = match script_type {
                ScriptType::P2tr => 34,
                ScriptType::P2wpkh => 22,
                ScriptType::P2pkh => 25,
            };
            let expected_weight = predict_weight([expected_input], [expected_output_len]);

            assert_eq!(spend_tx.with(&sim).info().weight, expected_weight);
        }
    }

    #[test]
    fn test_consolidation_strategy_pays_and_consolidates() {
        use crate::config::{ScorerConfig, WalletTypeConfig};

        let wallet_types = vec![
            WalletTypeConfig {
                name: "consolidator".to_string(),
                count: 1,
                strategies: vec!["Consolidator".to_string()],
                scorer: ScorerConfig {
                    privacy_weight: 0.0,
                    payment_obligation_weight: 1.0,
                    min_fallback_plans: 0,
                },
                script_type: ScriptType::P2tr,
            },
            WalletTypeConfig {
                name: "receiver".to_string(),
                count: 1,
                strategies: vec!["UnilateralSpender".to_string()],
                scorer: ScorerConfig {
                    privacy_weight: 0.0,
                    payment_obligation_weight: 1.0,
                    min_fallback_plans: 0,
                },
                script_type: ScriptType::P2tr,
            },
        ];

        let mut sim = SimulationBuilder::new(42, wallet_types, 8, 1, 4).build();
        sim.build_universe();
        sim.run();

        let consolidator = WalletId(0).with(&sim);

        assert!(
            !consolidator.info().payment_obligations.is_empty(),
            "Consolidator should have payment obligations"
        );
        assert!(
            !consolidator.info().handled_payment_obligations.is_empty(),
            "Consolidator should handle payment obligations"
        );

        let payment_receiver = consolidator
            .info()
            .payment_obligations
            .iter()
            .next()
            .map(|po_id| po_id.with(&sim).data().to)
            .expect("Consolidator should have a payment receiver");

        let mut saw_consolidation = false;
        for txid in consolidator.data().own_transactions.iter() {
            let tx = txid.with(&sim);
            if tx.data().inputs.len() <= 1 {
                continue;
            }
            if tx.data().outputs.len() != 2 {
                continue;
            }
            if !consolidator
                .info()
                .txid_to_payment_obligation_ids
                .contains_key(txid)
            {
                continue;
            }
            let output_wallets: Vec<_> = tx
                .data()
                .outputs
                .iter()
                .map(|output| output.address_id.with(&sim).data().wallet_id)
                .collect();
            if !output_wallets.contains(&consolidator.id) {
                continue;
            }
            if !output_wallets.contains(&payment_receiver) {
                continue;
            }
            saw_consolidation = true;
            break;
        }

        assert!(
            saw_consolidation,
            "Consolidator should broadcast a self-consolidation transaction"
        );
    }
}
