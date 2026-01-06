use std::collections::HashMap;

use bitcoin::consensus::Encodable;
use bitcoin::hashes::Hash;
use bitcoin::{OutPoint, Transaction, TxIn, TxOut};

// TODO: make a union find trait that requires get parent and impl find and union.
// open question: canonicalizing the parent and child requires the key to impl Ord -- probably fine. But we didnt do it.
// For the vec type we need conversion into usize. Should we create a trait bound for that?
// For "loose" transactions. No sequential order.
pub struct SparseDisjointSet<K: Eq + std::hash::Hash + Copy>(HashMap<K, K>);

impl<K: Eq + std::hash::Hash + Copy> SparseDisjointSet<K> {
    fn new() -> Self {
        Self(HashMap::new())
    }
    fn find(&mut self, x: K) -> K {
        let parent = *self.0.get(&x).unwrap_or(&x);
        if parent == x {
            return x;
        }
        let root = self.find(parent);
        self.0.insert(x, root);

        root
    }
    fn union(&mut self, x: K, y: K) {
        let x_root = self.find(x);
        let y_root = self.find(y);

        if x_root == y_root {
            return;
        }

        self.0.insert(y_root, x_root);
    }
}

/// For sequentially ordered keys. Keys here are global txout indices.
pub struct SequentialDisjointSet(Vec<usize>);

impl SequentialDisjointSet {
    fn new(n: usize) -> Self {
        Self(Vec::from_iter(0..n))
    }

    /// Finds the root of the subset that x is in.
    fn find(&mut self, x: usize) -> usize {
        let parent = self.0[x];
        if parent == x {
            return x;
        }
        let root = self.find(parent);
        self.0[x] = root;

        root
    }

    /// Declares that x and y are in the same subset. Merges the subsets of x and y.
    fn union(&mut self, x: usize, y: usize) {
        let x_root = self.find(x);
        let y_root = self.find(y);

        if x_root == y_root {
            return;
        }

        let (parent, child) = if x_root < y_root {
            (x_root, y_root)
        } else {
            (y_root, x_root)
        };

        self.0[child] = parent;
    }
}

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
type TxoutShortId = u32; // 4 Byte short id identifier

trait ShortId: bitcoin::consensus::Encodable {
    /// Produce 80 byte hash of the item.
    fn short_id(&self) -> TxoutShortId;
}

impl ShortId for TxOut {
    fn short_id(&self) -> TxoutShortId {
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
    uf: SparseDisjointSet<TxoutShortId>,
    index: Box<dyn FullIndex>,
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
    use crate::{
        InMemoryIndex, MultiInputHeuristic, SequentialDisjointSet, ShortId, SparseDisjointSet,
    };
    use bitcoin::{
        Amount, OutPoint, ScriptBuf, Sequence, Transaction, TxIn, TxOut, Witness,
        absolute::LockTime,
        secp256k1::{self},
    };
    use secp256k1::Secp256k1;
    use secp256k1::rand::rngs::OsRng;

    #[test]
    fn test_union_find() {
        // Singleton case
        assert_eq!(SequentialDisjointSet::new(1).find(0), 0);

        let mut uf = SequentialDisjointSet::new(5);
        uf.union(0, 2);
        assert_eq!(uf.find(0), 0);
        assert_eq!(uf.find(1), 1);
        assert_eq!(uf.find(2), 0);
        assert_eq!(uf.find(3), 3);
        assert_eq!(uf.find(4), 4);

        uf.union(4, 2);
        assert_eq!(uf.find(0), 0);
        assert_eq!(uf.find(1), 1);
        assert_eq!(uf.find(2), 0);
        assert_eq!(uf.find(3), 3);
        assert_eq!(uf.find(4), 0);

        uf.union(3, 1);
        assert_eq!(uf.find(0), 0);
        assert_eq!(uf.find(1), 1);
        assert_eq!(uf.find(2), 0);
        assert_eq!(uf.find(3), 1);
        assert_eq!(uf.find(4), 0);

        uf.union(3, 4);
        assert_eq!(uf.find(0), 0);
        assert_eq!(uf.find(1), 0);
        assert_eq!(uf.find(2), 0);
        assert_eq!(uf.find(3), 0);
        assert_eq!(uf.find(4), 0);

        let mut uf = SequentialDisjointSet::new(5);
        uf.union(0, 2);
        uf.union(4, 2);
        uf.union(3, 1);
        uf.union(3, 4);
        assert_eq!(uf.find(0), 0);
        assert_eq!(uf.find(1), 0);
        assert_eq!(uf.find(2), 0);
        assert_eq!(uf.find(3), 0);
        assert_eq!(uf.find(4), 0);
    }

    #[test]
    fn test_sparse_union_find() {
        // Singleton case
        assert_eq!(SparseDisjointSet::new().find(0), 0);

        let mut uf = SparseDisjointSet::new();
        uf.union(0, 2);
        assert_eq!(uf.find(0), uf.find(2));
        assert_eq!(uf.find(1), uf.find(1));
        assert_eq!(uf.find(2), uf.find(0));
        assert_eq!(uf.find(3), uf.find(3));
        assert_eq!(uf.find(4), uf.find(4));

        uf.union(4, 2);
        assert_eq!(uf.find(0), uf.find(0));
        assert_eq!(uf.find(1), uf.find(1));
        assert_eq!(uf.find(2), uf.find(0));
        assert_eq!(uf.find(3), uf.find(3));
        assert_eq!(uf.find(4), uf.find(0));

        uf.union(3, 1);
        assert_eq!(uf.find(0), uf.find(0));
        assert_eq!(uf.find(1), uf.find(1));
        assert_eq!(uf.find(2), uf.find(0));
        assert_eq!(uf.find(3), uf.find(1));
        assert_eq!(uf.find(4), uf.find(0));

        uf.union(3, 4);
        assert_eq!(uf.find(0), uf.find(0));
        assert_eq!(uf.find(1), uf.find(0));
        assert_eq!(uf.find(2), uf.find(0));
        assert_eq!(uf.find(3), uf.find(0));
        assert_eq!(uf.find(4), uf.find(0));

        let mut uf = SparseDisjointSet::new();
        uf.union(0, 2);
        uf.union(4, 2);
        uf.union(3, 1);
        uf.union(3, 4);
        assert_eq!(uf.find(0), uf.find(0));
        assert_eq!(uf.find(1), uf.find(0));
        assert_eq!(uf.find(2), uf.find(0));
        assert_eq!(uf.find(3), uf.find(0));
        assert_eq!(uf.find(4), uf.find(0));
    }

    #[test]
    fn test_union_find_no_unions() {
        // Test that all elements remain separate when no unions are performed
        let mut uf = SequentialDisjointSet::new(10);
        for i in 0..10 {
            assert_eq!(uf.find(i), i);
        }
    }

    #[test]
    fn test_union_find_sequential_chain() {
        // Test sequential unions forming a chain: 0-1-2-3-4
        let mut uf = SequentialDisjointSet::new(5);
        uf.union(0, 1);
        uf.union(1, 2);
        uf.union(2, 3);
        uf.union(3, 4);

        // All should have root 0
        for i in 0..5 {
            assert_eq!(uf.find(i), 0);
        }
    }

    #[test]
    fn test_union_find_idempotent_union() {
        // Test that unioning the same pair multiple times is idempotent
        let mut uf = SequentialDisjointSet::new(5);
        uf.union(1, 2);
        uf.union(1, 2);
        uf.union(2, 1);
        uf.union(1, 2);

        assert_eq!(uf.find(1), 1);
        assert_eq!(uf.find(2), 1);
    }

    #[test]
    fn test_union_find_union_with_self() {
        // Test that unioning an element with itself is idempotent
        let mut uf = SequentialDisjointSet::new(5);
        uf.union(2, 2);
        assert_eq!(uf.find(2), 2);

        uf.union(2, 2);
        assert_eq!(uf.find(2), 2);
    }

    #[test]
    fn test_union_find_path_compression() {
        // Test that path compression works correctly
        // Create a chain: 0 <- 1 <- 2 <- 3 <- 4
        let mut uf = SequentialDisjointSet::new(5);
        uf.union(0, 1);
        uf.union(1, 2);
        uf.union(2, 3);
        uf.union(3, 4);

        // First find should compress the path
        assert_eq!(uf.find(4), 0);
        // Subsequent finds should be fast (path already compressed)
        assert_eq!(uf.find(4), 0);
        assert_eq!(uf.find(3), 0);
        assert_eq!(uf.find(2), 0);
        assert_eq!(uf.find(1), 0);
    }

    #[test]
    fn test_union_find_star_pattern() {
        // Test star pattern: all elements connected to center
        let mut uf = SequentialDisjointSet::new(10);
        for i in 1..10 {
            uf.union(0, i);
        }

        // All should have root 0
        for i in 0..10 {
            assert_eq!(uf.find(i), 0);
        }
    }

    #[test]
    fn test_union_find_two_groups() {
        // Test two separate groups that never merge
        let mut uf = SequentialDisjointSet::new(10);
        // Group 1: 0, 1, 2, 3, 4
        for i in 1..5 {
            uf.union(0, i);
        }
        // Group 2: 5, 6, 7, 8, 9
        for i in 6..10 {
            uf.union(5, i);
        }

        // Group 1 should all have root 0
        for i in 0..5 {
            assert_eq!(uf.find(i), 0);
        }
        // Group 2 should all have root 5
        for i in 5..10 {
            assert_eq!(uf.find(i), 5);
        }
    }

    #[test]
    fn test_union_find_transitivity() {
        // Test transitivity: if A is connected to B and B to C, then A is connected to C
        let mut uf = SequentialDisjointSet::new(5);
        uf.union(0, 1);
        uf.union(1, 2);
        // Without directly unioning 0 and 2, they should be connected
        assert_eq!(uf.find(0), uf.find(2));

        uf.union(3, 4);
        // 3 and 4 should be connected, but not to 0, 1, 2
        assert_eq!(uf.find(3), uf.find(4));
        assert_ne!(uf.find(0), uf.find(3));
    }

    #[test]
    fn test_union_find_large_set() {
        // Test with a larger set
        let mut uf = SequentialDisjointSet::new(100);
        // Create groups of 10
        for group in 0..10 {
            let base = group * 10;
            for i in 1..10 {
                uf.union(base, base + i);
            }
        }

        // Verify each group is internally connected
        for group in 0..10 {
            let base = group * 10;
            let root = uf.find(base);
            for i in 1..10 {
                assert_eq!(uf.find(base + i), root);
            }
        }

        // Verify groups are separate
        assert_ne!(uf.find(0), uf.find(10));
        assert_ne!(uf.find(20), uf.find(30));
    }

    #[test]
    fn test_union_find_merge_groups() {
        // Test merging two existing groups
        let mut uf = SequentialDisjointSet::new(10);
        // Create group 1: 0-4
        for i in 1..5 {
            uf.union(0, i);
        }
        // Create group 2: 5-9
        for i in 6..10 {
            uf.union(5, i);
        }

        // Merge the two groups
        uf.union(2, 7);

        // Now all should be connected (root should be 0 since 0 < 5)
        for i in 0..10 {
            assert_eq!(uf.find(i), 0);
        }
    }

    #[test]
    fn test_union_find_reverse_order() {
        // Test that union order doesn't matter for final result
        let mut uf1 = SequentialDisjointSet::new(5);
        uf1.union(0, 1);
        uf1.union(2, 3);
        uf1.union(1, 2);
        uf1.union(3, 4);

        let mut uf2 = SequentialDisjointSet::new(5);
        uf2.union(4, 3);
        uf2.union(3, 2);
        uf2.union(2, 1);
        uf2.union(1, 0);

        // Both should result in all elements connected
        for i in 0..5 {
            assert_eq!(uf1.find(i), uf1.find(0));
            assert_eq!(uf2.find(i), uf2.find(0));
        }
    }

    #[test]
    fn multi_input_heuristic() {
        let coinbase1 = create_coinbase_with_many_outputs(1, 10); // 10 outputs
        let coinbase2 = create_coinbase_with_many_outputs(2, 15); // 15 outputs

        let coinbase3 = create_coinbase_with_many_outputs(3, 20); // 20 outputs

        // Get the txids
        let coinbase1_txid = coinbase1.compute_txid();
        let coinbase2_txid = coinbase2.compute_txid();
        let coinbase3_txid = coinbase3.compute_txid();

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
            coinbase1.output[7].clone(),
        );

        // Add index for txins spent by txouts
        // For this limited example there is two outs that are spet
        let mut heuristic = MultiInputHeuristic::new(Box::new(index));
        heuristic.visit_tx(&spending_tx);

        assert_eq!(
            heuristic.uf.find(coinbase1.output[3].short_id()),
            heuristic.uf.find(coinbase2.output[7].short_id())
        );
        assert_ne!(
            heuristic.uf.find(coinbase1.output[3].short_id()),
            heuristic.uf.find(coinbase3.output[20].short_id())
        );
        assert_ne!(
            heuristic.uf.find(coinbase2.output[7].short_id()),
            heuristic.uf.find(coinbase3.output[20].short_id())
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
