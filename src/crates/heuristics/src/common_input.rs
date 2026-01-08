use std::collections::HashMap;

use bitcoin::consensus::Encodable;
use bitcoin::hashes::Hash;
use bitcoin::{Transaction, TxOut, Txid};
use tx_indexer_primitives::disjoint_set::{DisJointSet, SparseDisjointSet};

pub trait PrevOutIndex {
    // TODO: this should take an input id and return an id
    // TODO: consider handle wrappers converting ids to the actual types.
    // justification: the heuristics may not care about the content of the data. Only access that thru the handler.
    fn prev_txout(&self, ot: &TxInId) -> TxOutId;
}

pub trait TxInIndex {
    fn spending_txin(&self, tx: &TxOutId) -> Option<TxInId>;
}

pub trait TxOutIndex {
    fn get_txout(&self, id: TxOutId) -> Option<TxOutHandler>;
}

pub trait FullIndex: PrevOutIndex + TxInIndex {}

pub struct InMemoryIndex {
    prev_txouts: HashMap<TxInId, TxOutId>,
    spending_txins: HashMap<TxOutId, TxInId>,
}

impl InMemoryIndex {
    fn new() -> Self {
        Self {
            prev_txouts: HashMap::new(),
            spending_txins: HashMap::new(),
        }
    }
    // TODO: data ingestion
}

impl PrevOutIndex for InMemoryIndex {
    fn prev_txout(&self, id: &TxInId) -> TxOutId {
        self.prev_txouts
            .get(id)
            .expect("Previous output should always be present if index is build correctly")
            .clone()
    }
}
impl TxInIndex for InMemoryIndex {
    fn spending_txin(&self, tx_out: &TxOutId) -> Option<TxInId> {
        self.spending_txins.get(tx_out).cloned()
    }
}

// TODO(armins): come back to this later. when we need to access the txout data.
// impl TxOutIndex for InMemoryIndex {
//     fn get_txout(&self, id: TxOutId) -> Option<TxOutHandler> {
//         self.tx
//             .get(&id)
//             .map(|txout| TxOutHandler { id, index: self })
//     }
// }

impl FullIndex for InMemoryIndex {}

trait ToShortId: bitcoin::consensus::Encodable {
    /// Produce 80 byte hash of the item.
    fn short_id(&self) -> u32;
}

// TBD whether this is a generic or u32 specifically
/// Sum of the short id of the txid and vout.
#[derive(PartialEq, Eq, Hash, Copy, Clone, Debug)]
pub struct TxOutId(u32);
impl TxOutId {
    fn new(txid: Txid, vout: u32) -> Self {
        let txid_short_id = txid.short_id();
        Self(txid_short_id + vout)
    }
}
pub struct TxOutHandler {
    id: TxOutId,
    index: Box<dyn TxOutIndex>,
}

impl TxOutHandler {
    fn new(id: TxOutId, index: Box<dyn TxOutIndex>) -> Self {
        Self { id, index }
    }
}

/// Sum of the short id of the txid and vin
#[derive(PartialEq, Eq, Hash, Copy, Clone, Debug)]
pub struct TxInId(u32);
impl TxInId {
    fn new(txid: Txid, vin: u32) -> Self {
        let txid_short_id = txid.short_id();
        Self(txid_short_id + vin)
    }
}

// TODO(armins): do later. Dont need this for now
// pub struct TxInHandler {
//     id: TxInId,
//     index: Box<dyn TxInIndex>,
// }

// impl TxInHandler {
//     fn new(id: TxInId, index: Box<dyn TxInIndex>) -> Self {
//         Self { id, index }
//     }
// }

macro_rules! impl_to_short_id {
    ($t:ty) => {
        impl ToShortId for $t {
            fn short_id(&self) -> u32 {
                let mut buf = Vec::new();
                self.consensus_encode(&mut buf).unwrap();
                let hash = bitcoin::hashes::sha256::Hash::hash(buf.as_slice());

                u32::from_le_bytes(
                    hash.to_byte_array()
                        .to_vec()
                        .into_iter()
                        .take(4)
                        .collect::<Vec<u8>>()
                        .try_into()
                        .unwrap(),
                )
            }
        }
    };
}

impl_to_short_id!(TxOut);
impl_to_short_id!(Txid);

pub struct MultiInputHeuristic {
    uf: SparseDisjointSet<TxOutId>,
    index: Box<dyn PrevOutIndex>,
}

struct AbstractTransaction {
    /// Short ids of txouts of the previous transactions
    spent_coins: Vec<TxOutId>,
}

// TODO: trait definition for heuristics?
impl MultiInputHeuristic {
    fn new(index: Box<dyn FullIndex>) -> Self {
        Self {
            uf: SparseDisjointSet::new(),
            index,
        }
    }

    fn visit_tx(&mut self, tx: &Transaction) {
        let txid = tx.compute_txid();
        // In the sparse representation we need to assign a unique short id to each txout
        tx.output
            .iter()
            .enumerate()
            .map(|(vout, _)| TxOutId::new(txid, vout as u32))
            .for_each(|output| {
                self.uf.find(output);
            });

        if tx.is_coinbase() {
            return;
        }
        self.merge_txouts(AbstractTransaction {
            spent_coins: tx
                .input
                .iter()
                .enumerate()
                .map(|(vin, _)| TxInId::new(txid, vin as u32))
                .map(|input| self.index.prev_txout(&input))
                .collect(),
        })
    }

    fn merge_txouts(&mut self, tx: AbstractTransaction) {
        tx.spent_coins.iter().reduce(|a, b| {
            self.uf.union(*a, *b);
            a
        });
    }
}

#[cfg(test)]
mod tests {
    use crate::common_input::{InMemoryIndex, MultiInputHeuristic, ToShortId, TxInId, TxOutId};
    use bitcoin::{
        Amount, OutPoint, ScriptBuf, Sequence, Transaction, TxIn, TxOut, Witness,
        absolute::LockTime,
        secp256k1::{self},
    };
    use secp256k1::Secp256k1;
    use secp256k1::rand::rngs::OsRng;
    use tx_indexer_primitives::disjoint_set::DisJointSet;

    #[test]
    fn multi_input_heuristic() {
        let coinbase1 = create_coinbase_with_many_outputs(1, 10); // 10 outputs
        let coinbase2 = create_coinbase_with_many_outputs(2, 15); // 15 outputs

        let coinbase3 = create_coinbase_with_many_outputs(3, 20); // 20 outputs

        // Get the txids
        let coinbase1_txid = coinbase1.compute_txid();
        let coinbase2_txid = coinbase2.compute_txid();

        // Create a spending transaction that spends the two outputs:
        let total_value = coinbase1.output[3]
            .value
            .checked_add(coinbase2.output[7].value)
            .expect("value overflow");
        let spending_tx = create_spending_transaction(
            OutPoint::new(coinbase1_txid, 3),
            OutPoint::new(coinbase2_txid, 7),
            total_value,
        );

        assert_eq!(spending_tx.input.len(), 2);

        let mut index = InMemoryIndex::new();
        // Add index for txouts spending inputs
        index.prev_txouts.insert(
            TxInId::new(spending_tx.compute_txid(), 0),
            TxOutId::new(coinbase1.compute_txid(), 3),
        );
        index.prev_txouts.insert(
            TxInId::new(spending_tx.compute_txid(), 1),
            TxOutId::new(coinbase2.compute_txid(), 7),
        );

        // Add index for txins spent by txouts
        // For this limited example there is two outs that are spet
        let mut heuristic = MultiInputHeuristic::new(Box::new(index));
        heuristic.visit_tx(&spending_tx);

        assert_eq!(
            heuristic.uf.find(TxOutId::new(coinbase1.compute_txid(), 3)),
            heuristic.uf.find(TxOutId::new(coinbase2.compute_txid(), 7))
        );
        // TODO: more assertions here
        assert_ne!(
            heuristic.uf.find(TxOutId::new(coinbase2.compute_txid(), 7)),
            heuristic.uf.find(TxOutId::new(coinbase3.compute_txid(), 1))
        );
    }

    fn create_coinbase_with_many_outputs(block_height: u32, num_outputs: usize) -> Transaction {
        // Create coinbase input (special input with no previous output)
        let mut coinbase_script_bytes = block_height.to_le_bytes().to_vec();
        coinbase_script_bytes.push(0x00); // Add a byte to make it a valid script
        let coinbase_script = ScriptBuf::from(coinbase_script_bytes);
        let coinbase_input = TxIn {
            previous_output: OutPoint::null(),
            script_sig: coinbase_script,
            sequence: Sequence::MAX,
            witness: Witness::new(),
        };

        // Create many outputs
        let mut outputs = Vec::new();
        for i in 0..num_outputs {
            // Each output has a different value (in satoshis)
            let value = Amount::from_sat(50_000_000 + (i as u64 * 1_000_000));
            // Use a unique keypair per output for the script_pubkey
            let secp = Secp256k1::new();
            let (_secret_key, public_key) = secp.generate_keypair(&mut OsRng);
            let script_pubkey = ScriptBuf::new_p2pk(&public_key.into());
            outputs.push(TxOut {
                value,
                script_pubkey,
            });
        }

        Transaction {
            version: bitcoin::transaction::Version::TWO,
            lock_time: LockTime::ZERO,
            input: vec![coinbase_input],
            output: outputs,
        }
    }

    fn create_spending_transaction(
        prev_outpoint1: OutPoint,
        prev_outpoint2: OutPoint,
        total_value: Amount,
    ) -> Transaction {
        let input1 = TxIn {
            previous_output: prev_outpoint1,
            script_sig: ScriptBuf::new(),
            sequence: Sequence::MAX,
            witness: Witness::new(),
        };
        let input2 = TxIn {
            previous_output: prev_outpoint2,
            script_sig: ScriptBuf::new(),
            sequence: Sequence::MAX,
            witness: Witness::new(),
        };

        let fee = Amount::from_sat(1_000);
        let output_value = total_value.checked_sub(fee).unwrap_or(total_value);
        let secp = Secp256k1::new();
        let (_, public_key) = secp.generate_keypair(&mut OsRng);
        let spk = ScriptBuf::new_p2pk(&public_key.into());

        let output = TxOut {
            value: output_value,
            script_pubkey: spk,
        };

        Transaction {
            version: bitcoin::transaction::Version::TWO,
            lock_time: LockTime::ZERO,
            input: vec![input1, input2],
            output: vec![output],
        }
    }
}
