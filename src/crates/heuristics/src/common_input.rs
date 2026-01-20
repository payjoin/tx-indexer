use std::any::TypeId;

use tx_indexer_primitives::{
    abstract_types::EnumerateSpentTxOuts,
    datalog::{ClusterRel, CursorBook, IsCoinJoinRel, Rule, TxRel},
    disjoint_set::{DisJointSet, SparseDisjointSet},
    loose::TxOutId,
    storage::{FactStore, MemStore},
    test_utils::DummyTxData,
};

pub struct MultiInputHeuristic;

// TODO: trait definition for heuristics?
impl MultiInputHeuristic {
    pub fn merge_prevouts(&self, tx: &impl EnumerateSpentTxOuts) -> SparseDisjointSet<TxOutId> {
        if tx.spent_coins().count() == 0 {
            return SparseDisjointSet::default();
        }
        let mut set = SparseDisjointSet::default();
        tx.spent_coins().reduce(|a, b| {
            set.union(a, b);
            a
        });
        set
    }
}

pub fn common_input_map_pass_fn<T: EnumerateSpentTxOuts>(tx: T) -> Option<Vec<(TxOutId, TxOutId)>> {
    unimplemented!();
    // let heuristic = MultiInputHeuristic;
    // match heuristic.merge_prevouts(&tx) {
    //     MultiInputResult::Cluster(pairs) => Some(pairs),
    //     _ => None,
    // }
}

pub struct MihRule;

impl Rule for MihRule {
    fn name(&self) -> &'static str {
        "mih"
    }

    fn inputs(&self) -> &'static [TypeId] {
        // depends on Tx deltas; also reads IsCoinJoin for gating
        const INS: &[TypeId] = &[TypeId::of::<TxRel>(), TypeId::of::<IsCoinJoinRel>()];
        INS
    }

    fn step(&mut self, rid: usize, store: &mut MemStore, cursors: &mut CursorBook) -> usize {
        let delta_txs: Vec<DummyTxData> = cursors.read_delta::<TxRel>(rid, store);
        if delta_txs.is_empty() {
            return 0;
        }

        let mut out = 0;
        for tx in delta_txs {
            // gate: skip coinjoins (or change to score threshold if you store scores)
            if store.contains::<IsCoinJoinRel>(&(tx.id, true)) {
                continue;
            }

            let to_merge = MultiInputHeuristic.merge_prevouts(&tx);
            if store.insert::<ClusterRel>(to_merge) {
                out += 1;
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use crate::common_input::MultiInputHeuristic;
    use bitcoin::{
        Amount, OutPoint, ScriptBuf, Sequence, Transaction, TxIn, TxOut, Witness,
        absolute::LockTime,
        secp256k1::{self},
    };
    use secp256k1::Secp256k1;
    use secp256k1::rand::rngs::OsRng;
    use tx_indexer_primitives::{
        disjoint_set::SparseDisjointSet, loose::TxOutId, storage::InMemoryIndex,
    };

    // #[test]
    // fn multi_input_heuristic() {
    //     let coinbase1 = create_coinbase_with_many_outputs(1, 10); // 10 outputs
    //     let coinbase2 = create_coinbase_with_many_outputs(2, 15); // 15 outputs
    //     let coinbase3 = create_coinbase_with_many_outputs(3, 20); // 20 outputs

    //     let coinbase1_txid = coinbase1.compute_txid();
    //     let coinbase2_txid = coinbase2.compute_txid();

    //     let spending_vout_1 = 3u32;
    //     let spending_vout_2 = 7u32;

    //     // Create a spending transaction that spends the two outputs:
    //     let total_value = coinbase1.output[spending_vout_1 as usize]
    //         .value
    //         .checked_add(coinbase2.output[spending_vout_2 as usize].value)
    //         .expect("value overflow");
    //     let spending_tx = create_spending_transaction(
    //         OutPoint::new(coinbase1_txid, spending_vout_1),
    //         OutPoint::new(coinbase2_txid, spending_vout_2),
    //         total_value,
    //     );

    //     assert_eq!(spending_tx.input.len(), 2);

    //     let mut index = InMemoryIndex::new();
    //     let all_txs = vec![
    //         coinbase1.clone(),
    //         coinbase2.clone(),
    //         coinbase3.clone(),
    //         spending_tx.clone(),
    //     ];

    //     let mut clustering_index = InMemoryClusteringIndex::<SparseDisjointSet<TxOutId>>::new();
    //     for tx in all_txs.iter() {
    //         index.add_tx(Box::new(BitcoinTransactionWrapper { tx: tx.clone() }));
    //     }

    //     // Add index for txins spent by txouts
    //     // For this limited example there is two outs that are spet

    //     // FIXME: removed these assertions during a refactor. Add them back in using abstract types not bitcoin types
    //     // let heuristic = MultiInputHeuristic;
    //     // for tx in all_txs.iter() {
    //     //     let tx_handle = index.compute_txid(tx.compute_txid()).with(&index);
    //     //     let prevouts = heuristic.merge_prevouts(&tx_handle);
    //     //     for (a, b) in prevouts.iter() {
    //     //         clustering_index.execute(MutableOperation::Cluster(*a, *b));
    //     //     }
    //     // }

    //     assert_eq!(
    //         clustering_index.find_root(
    //                 InMemoryIndex::compute_txid(coinbase1.compute_txid())
    //                 .txout_id(spending_vout_1)
    //         ),
    //         clustering_index.find_root(
    //             InMemoryIndex::compute_txid(coinbase2.compute_txid())
    //                 .txout_id(spending_vout_2)
    //         )
    //     );
    //     // TODO: more assertions here
    //     assert_ne!(
    //         clustering_index.find_root(&index.compute_txid(coinbase2.compute_txid()).txout_id(7)),
    //         clustering_index.find_root(&index.compute_txid(coinbase3.compute_txid()).txout_id(1))
    //     );
    // }

    pub fn create_coinbase_with_many_outputs(block_height: u32, num_outputs: usize) -> Transaction {
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
