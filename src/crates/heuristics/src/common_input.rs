use std::collections::HashMap;

use bitcoin::consensus::Encodable;
use bitcoin::hashes::Hash;
use bitcoin::{OutPoint, Transaction, TxIn, TxOut};
use tx_indexer_primitives::disjoint_set::SparseDisjointSet;


pub trait PrevOutIndex {
    fn prev_txout(&self, ot: &OutPoint) -> TxOut;
}

pub trait TxInIndex {
    fn spending_txin(&self, tx: &TxOut) -> Option<TxIn>;
}

pub trait FullIndex: PrevOutIndex + TxInIndex {}

pub struct InMemoryIndex {
    prev_txouts: HashMap<OutPoint, TxOut>,
    spending_txins: HashMap<TxOut, TxIn>,
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
    fn prev_txout(&self, ot: &OutPoint) -> TxOut {
        self.prev_txouts
            .get(ot)
            .expect("Previous output should always be present if index is build correctly")
            .clone()
    }
}

impl TxInIndex for InMemoryIndex {
    fn spending_txin(&self, tx_out: &TxOut) -> Option<TxIn> {
        self.spending_txins.get(tx_out).cloned()
    }
}

impl FullIndex for InMemoryIndex {}

// Txout short id is a hash of the txout
type ShortId = u32; // 4 Byte short id identifier

trait ToShortId: bitcoin::consensus::Encodable {
    /// Produce 80 byte hash of the item.
    fn short_id(&self) -> ShortId;
}

impl ToShortId for TxOut {
    fn short_id(&self) -> ShortId {
        let mut buf = Vec::new();
        self.consensus_encode(&mut buf).unwrap();
        let hash = bitcoin::hashes::sha256::Hash::hash(buf.as_slice());

        // TODO: This is super ugly. Refactor later
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

pub struct MultiInputHeuristic {
    uf: SparseDisjointSet<ShortId>,
    index: Box<dyn PrevOutIndex>,
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
        // In the sparse representation we need to assign a unique short id to each txout
        tx.output
            .iter()
            .map(|output| output.short_id())
            .for_each(|output| {
                self.uf.find(output);
            });

        if tx.is_coinbase() {
            return;
        }

        // Create a root from the first input
        let root = self.index.prev_txout(&tx.input[0].previous_output);
        // Should create the root if it doesn't exist
        self.uf.find(root.short_id());
        for input in tx.input.iter().skip(1).map(|input| input.previous_output) {
            let txout_index = self.index.prev_txout(&input);
            self.uf.union(root.short_id(), txout_index.short_id());
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::common_input::{InMemoryIndex, MultiInputHeuristic, ToShortId};
    use bitcoin::{
        Amount, OutPoint, ScriptBuf, Sequence, Transaction, TxIn, TxOut, Witness,
        absolute::LockTime,
        secp256k1::{self},
    };
    use secp256k1::Secp256k1;
    use secp256k1::rand::rngs::OsRng;


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
            spending_tx.input[0].previous_output,
            coinbase1.output[3].clone(),
        );
        index.prev_txouts.insert(
            spending_tx.input[1].previous_output,
            coinbase2.output[7].clone(),
        );

        // Add index for txins spent by txouts
        // For this limited example there is two outs that are spet
        let mut heuristic = MultiInputHeuristic::new(Box::new(index));
        heuristic.visit_tx(&spending_tx);

        assert_eq!(
            heuristic.uf.find(coinbase1.output[3].short_id()),
            heuristic.uf.find(coinbase2.output[7].short_id())
        );
        // TODO: more assertions here
        assert_ne!(
            heuristic.uf.find(coinbase2.output[7].short_id()),
            heuristic.uf.find(coinbase3.output[1].short_id())
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
