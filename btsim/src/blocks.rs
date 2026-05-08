use bitcoin::{Amount, Weight};
use im::OrdSet;

use crate::{
    transaction::{Outpoint, Output, TxHandle, TxId},
    wallet::{AddressId, WalletHandle, WalletId, WalletInfo, WalletInfoId},
    Simulation,
};

#[derive(Debug)]
pub(crate) struct ChainParams {
    initial_subsidy: Amount,
    halving_interval: usize,
    #[allow(dead_code)]
    max_block_weight: Weight,
}

impl ChainParams {
    fn subsidy(self, height: usize) -> Amount {
        let halvings = height / self.halving_interval;
        if halvings >= 64 {
            // BIP 42: After 64 halvings (or when shift would be >= 64), subsidy is permanently zero
            Amount::ZERO
        } else {
            Amount::from_sat(self.initial_subsidy.to_sat() >> halvings)
        }
    }
}

impl Default for ChainParams {
    fn default() -> Self {
        ChainParams {
            initial_subsidy: Amount::from_int_btc(50),
            halving_interval: 210_000,
            max_block_weight: Weight::from_wu(400000),
        }
    }
}

define_entity!(
    Block,
    {
        // block data
        pub(crate) parent: Option<BlockId>,
        pub(crate) coinbase_tx: TxId,
        pub(crate) confirmed_txs: Vec<TxId>,
    },
    {
        // TODO total size
        // TODO total fees
        pub(crate) height: usize,
        pub(crate) spent: OrdSet<Outpoint>,
        pub(crate) created: OrdSet<Outpoint>,
        pub(crate) utxos: OrdSet<Outpoint>,
        pub(crate) confirmed_txs: OrdSet<TxId>,
        pub(crate) all_confirmed_txs: OrdSet<TxId>,
    }
);

define_entity!(
    BroadcastSet,
    {
        pub(crate) data: BroadcastSetType,
    },
    {
        pub(crate) parent_id: Option<BroadcastSetId>,
        pub(crate) chain_tip_id: BlockId,
        pub(crate) unconfirmed_txs: OrdSet<TxId>,
        pub(crate) invalidated_txs: OrdSet<TxId>,
    }
);

define_entity_handle_mut!(BroadcastSet);

#[derive(Debug, PartialEq, Eq, Clone)]
/// Represents a broadcast set in the simulation, which can be a confirmed block
/// or a set of transactions awaiting confirmation.
pub(crate) enum BroadcastSetType {
    Block(BlockId),
    Transactions(Vec<TxId>),
}

impl<'a> BroadcastSetHandle<'a> {
    pub(crate) fn data(&'a self) -> &'a BroadcastSetType {
        &self.sim.broadcast_set_data[self.id.0].data
    }

    pub(crate) fn info(&'a self) -> &'a BroadcastSetInfo {
        &self.sim.broadcast_set_info[self.id.0]
    }
}

impl<'a> BroadcastSetHandleMut<'a> {
    pub(crate) fn process_block(self, block_id: BlockId) -> Self {
        let block = block_id.with(self.sim);

        let mut unconfirmed_txs = self.info().unconfirmed_txs.clone();
        let mut invalidated_txs = self.info().invalidated_txs.clone();

        for tx in block.data().txs() {
            unconfirmed_txs.remove(tx);

            let tx = tx.with(self.sim);
            // also remove conflicting transactions
            // TODO refactor tx.with(self.sim).spent_coins() impl Iterator Outppoint?
            for input in tx.inputs() {
                let spends = self.sim.spends[&input.data().outpoint]
                    .iter()
                    .map(|input_id| input_id.txid)
                    .collect::<OrdSet<TxId>>();
                for conflicting_tx in spends.without(&tx.id).intersection(unconfirmed_txs.clone())
                // FIXME pr to im, unnecessary clone
                {
                    unconfirmed_txs.remove(&conflicting_tx);
                    invalidated_txs.insert(conflicting_tx);
                }
            }
        }

        self.sim.broadcast_set_data.push(BroadcastSetData {
            data: BroadcastSetType::Block(block_id),
        });

        self.sim.broadcast_set_info.push(BroadcastSetInfo {
            parent_id: Some(self.id),
            chain_tip_id: block_id,
            unconfirmed_txs,
            invalidated_txs,
        });

        let id = BroadcastSetId(self.sim.broadcast_set_data.len() - 1);

        Self { sim: self.sim, id }.update_wallets()
    }

    // TODO non Mut BroadcastSetHandle fn parent(&'a self) -> Self
    pub(crate) fn chain_tip(&'a self) -> BlockHandle<'a> {
        self.info().chain_tip_id.with(self.sim)
    }

    pub(crate) fn update_wallets(self) -> Self {
        let mut wallet_infos = std::collections::HashMap::<WalletId, WalletInfo>::default();

        let id = self.id;

        let mut update_wallet_info = |wallet: &WalletHandle, update: &dyn Fn(&mut WalletInfo)| {
            // impl not allowed in closure but rustc suggests adding it?
            wallet_infos.entry(wallet.id).or_insert_with(|| {
                let mut new_info = wallet.info().clone();
                new_info.broadcast_set_id = id;
                new_info
            });

            update(wallet_infos.get_mut(&wallet.id).unwrap())
        };

        match self.data() {
            BroadcastSetType::Transactions(new_txs) => {
                for tx in new_txs {
                    for input in tx.with(self.sim).inputs() {
                        let prevout = input.prevout();
                        let wallet = prevout.wallet();

                        update_wallet_info(&wallet, &|i: &mut WalletInfo| {
                            i.unconfirmed_spends.insert(input.data().outpoint);
                        });
                    }

                    for output in tx.with(self.sim).outputs() {
                        let wallet = output.wallet();

                        update_wallet_info(&wallet, &|info: &mut WalletInfo| {
                            // don't treat wallet generated transactions as received transactions
                            // FIXME O(n) contains()
                            if !wallet.data().own_transactions.contains(tx) {
                                info.received_transactions.push_back(*tx);
                            }

                            info.unconfirmed_transactions.insert(*tx);
                            info.unconfirmed_txos.insert(output.outpoint);
                        });
                    }
                }
            }
            BroadcastSetType::Block(_) => {
                for tx in &self.chain_tip().info().confirmed_txs {
                    let tx_handle = tx.with(self.sim);
                    for input in tx_handle.inputs() {
                        let prevout = input.prevout();
                        let wallet = prevout.wallet();

                        update_wallet_info(&wallet, &|info: &mut WalletInfo| {
                            info.broadcast_set_id = self.id;
                            info.confirmed_utxos.remove(&input.data().outpoint);
                            info.unconfirmed_txos.remove(&input.data().outpoint);
                            info.unconfirmed_spends.remove(&input.data().outpoint);
                            info.registered_inputs.remove(&input.data().outpoint);
                            info.unconfirmed_transactions.remove(tx);

                            // First check if this transaction directly handles a payment obligation
                            // This covers both unilateral spends and payjoins where we participated
                            if let Some(payment_obligation_ids) =
                                info.txid_to_payment_obligation_ids.get(tx)
                            {
                                info.handled_payment_obligations
                                    .extend(payment_obligation_ids.iter().copied());

                                for (_, mppj_session) in info.active_multi_party_payjoins.iter() {
                                    if mppj_session
                                        .inputs
                                        .iter()
                                        .any(|i| i.outpoint == input.data().outpoint)
                                    {
                                        info.handled_payment_obligations.extend(
                                            mppj_session.payment_obligation_ids.iter().copied(),
                                        );
                                        break;
                                    }
                                }
                            }
                        })
                    }

                    for output in tx.with(self.sim).outputs() {
                        let wallet = output.wallet();

                        update_wallet_info(&wallet, &|info: &mut WalletInfo| {
                            // TODO no .contains() check needed if checking self.all_txs?
                            // FIXME O(n) + O(n) contains()
                            if !wallet.data().own_transactions.contains(tx)
                                && !info.received_transactions.contains(tx)
                            {
                                info.received_transactions.push_back(*tx);
                            }
                            info.confirmed_utxos.insert(output.outpoint);
                            info.unconfirmed_txos.remove(&output.outpoint);
                            info.unconfirmed_transactions.remove(tx);
                        })
                    }
                }
            }
        };

        for (wallet_id, info) in wallet_infos {
            // TODO assert consistency of wallet info with global index? confirmed & unconfirmed intersections are empty

            let id = WalletInfoId(self.sim.wallet_info.len());
            self.sim.wallet_info.push(info); // TODO append?
            wallet_id.with_mut(self.sim).data_mut().last_wallet_info_id = id;
        }

        self
    }

    // TODO Deref, move to non mut
    pub(crate) fn unconfirmed_txs(&'a self) -> impl IntoIterator<Item = TxHandle<'a>> {
        self.info()
            .unconfirmed_txs
            .iter()
            .map(|tx| tx.with(self.sim))
    }

    pub(crate) fn broadcast(self, txs: impl IntoIterator<Item = TxId>) -> Self {
        let previously_unconfirmed_txs = &self.info().unconfirmed_txs;

        let all_confirmed_txs = &self.chain_tip().info().all_confirmed_txs;

        let new_txs: Vec<TxId> = txs
            .into_iter()
            .filter(|tx| !previously_unconfirmed_txs.contains(tx))
            .filter(|tx| !all_confirmed_txs.contains(tx))
            .collect();

        let unconfirmed_txs = previously_unconfirmed_txs
            .clone()
            .union(OrdSet::from(&new_txs));

        let data = BroadcastSetData {
            data: BroadcastSetType::Transactions(new_txs),
        };

        let bxset = BroadcastSetInfo {
            parent_id: Some(self.id),
            chain_tip_id: self.chain_tip().id,
            unconfirmed_txs,
            invalidated_txs: self.info().invalidated_txs.clone(),
        };

        let id = BroadcastSetId(self.sim.broadcast_set_data.len());

        self.sim.broadcast_set_data.push(data);
        self.sim.broadcast_set_info.push(bxset);

        Self { sim: self.sim, id }.update_wallets()
    }

    // TODO move to its own MempoolState objects that implement tx ordering policy
    // for each tx, build unconfirmed transitive closure of parents
    // order these by feerate
    // prune double counted parents
    // solve for satisfiability WRT double spending
    // knaptime optzn
    pub(crate) fn construct_block_template(&self, max_weight: Weight) -> BlockTemplate {
        let last_block = self.chain_tip();

        let mut utxos = last_block.info().utxos.clone();
        let mut spent = OrdSet::<Outpoint>::default();
        let mut created = OrdSet::<Outpoint>::default();
        let mut confirmed_txs = Vec::<TxId>::default();

        let mut remaining_weight = max_weight;

        'tx: for tx in self.unconfirmed_txs() {
            // skip if too large
            if tx.info().weight >= remaining_weight {
                continue 'tx;
            }

            let mut tx_utxos = utxos.clone();
            let mut tx_spent = spent.clone();
            let mut tx_created = created.clone();

            for input in &tx.data().inputs {
                if tx_utxos.remove(&input.outpoint).is_none() {
                    // skip if spending a spent txo
                    continue 'tx;
                }

                tx_spent.insert(input.outpoint);
            }

            for outpoint in tx.outpoints() {
                tx_utxos.insert(outpoint);
                tx_created.insert(outpoint);
            }

            // Transaction is valid,
            confirmed_txs.push(tx.id);
            utxos = tx_utxos;
            spent = tx_spent;
            created = tx_created;
            remaining_weight -= tx.info().weight;
        }

        BlockTemplate {
            parent: last_block.id,
            txs: confirmed_txs,
            utxos,
            spent,
            created,
        }
    }
}

// Ephemeral data type, no entity ID and not retained just a helper for constructing blocks
pub(crate) struct BlockTemplate {
    parent: BlockId,
    txs: Vec<TxId>,
    spent: OrdSet<Outpoint>,
    created: OrdSet<Outpoint>,
    utxos: OrdSet<Outpoint>,
}

impl<'a> BlockTemplate {
    pub(crate) fn mine(self, rewards_to: AddressId, sim: &'a mut Simulation) -> BlockHandle<'a> {
        let parent_block = self.parent.with(sim);

        let height = 1 + parent_block.info().height;
        let subsidy = ChainParams::default().subsidy(height); // TODO make parameter of simulation

        let fees = self.txs.iter().map(|tx| tx.with(sim).info().fee).sum();

        let block_rewards = subsidy + fees;

        let mut confirmed_txs = OrdSet::from(&self.txs);

        let coinbase_tx = sim.new_tx(|tx, _| {
            tx.outputs.push(Output {
                address_id: rewards_to,
                amount: block_rewards,
            });
        });

        confirmed_txs.insert(coinbase_tx);

        let parent_block = self.parent.with(sim); // recreate since new_tx needs a mut borrow of sim
        let all_confirmed_txs = parent_block
            .info()
            .all_confirmed_txs
            .clone()
            .union(confirmed_txs.clone());

        // TODO refactor _mut()?
        let rewards_wallet = rewards_to.with(sim).wallet().id;
        sim.wallet_data[rewards_wallet.0]
            .own_transactions
            .push(coinbase_tx);

        let mut utxos = self.utxos;
        let mut created = self.created;

        let outpoint = Outpoint {
            txid: coinbase_tx,
            index: 0,
        };
        utxos.insert(outpoint);
        created.insert(outpoint);

        // TODO refactor, blockinfo shouldn't be created here
        sim.new_block(
            BlockData {
                parent: Some(self.parent),
                coinbase_tx,
                confirmed_txs: self.txs,
            },
            BlockInfo {
                height,
                utxos,
                created,
                spent: self.spent,
                confirmed_txs,
                all_confirmed_txs,
            },
        )
    }
}

impl BlockData {
    fn txs(&self) -> impl Iterator<Item = &TxId> {
        // TODO why is confirmed_txs by ref?
        std::iter::once(&self.coinbase_tx).chain(self.confirmed_txs.iter())
    }
}

#[allow(dead_code)]
impl<'a> BlockHandle<'a> {
    pub(crate) fn data(&self) -> &'a BlockData {
        &self.sim.block_data[self.id.0]
    }

    // TODO TxHandle
    pub(crate) fn txs(&self) -> impl Iterator<Item = &'a TxId> {
        self.data().txs()
    }

    pub(crate) fn info(&self) -> &'a BlockInfo {
        &self.sim.block_info[self.id.0]
    }

    pub(crate) fn parent(&self) -> Option<Self> {
        self.data().parent.map(|id| Self { sim: self.sim, id })
    }

    pub(crate) fn coinbase_tx(&self) -> TxHandle<'_> {
        self.data().coinbase_tx.with(self.sim)
    }
}
