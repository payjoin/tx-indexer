use tx_indexer_primitives::{
    disjoint_set::{DisJointSet, SparseDisjointSet},
    loose::{EnumerateSpentTxOuts, TxOutId},
};

pub struct MultiInputHeuristic {
    uf: SparseDisjointSet<TxOutId>,
}

// TODO: trait definition for heuristics?
impl MultiInputHeuristic {
    fn new() -> Self {
        Self {
            uf: SparseDisjointSet::new(),
        }
    }

    fn merge_prevouts(&mut self, tx: &impl EnumerateSpentTxOuts) {
        tx.spent_coins().iter().reduce(|a, b| {
            self.uf.union(*a, *b);
            a
        });
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
    use tx_indexer_primitives::{disjoint_set::DisJointSet, loose::InMemoryIndex};

    #[test]
    fn multi_input_heuristic() {
        let coinbase1 = create_coinbase_with_many_outputs(1, 10); // 10 outputs
        let coinbase2 = create_coinbase_with_many_outputs(2, 15); // 15 outputs
        let coinbase3 = create_coinbase_with_many_outputs(3, 20); // 20 outputs

        let coinbase1_txid = coinbase1.compute_txid();
        let coinbase2_txid = coinbase2.compute_txid();

        let spending_vout_1 = 3u32;
        let spending_vout_2 = 7u32;

        // Create a spending transaction that spends the two outputs:
        let total_value = coinbase1.output[spending_vout_1 as usize]
            .value
            .checked_add(coinbase2.output[spending_vout_2 as usize].value)
            .expect("value overflow");
        let spending_tx = create_spending_transaction(
            OutPoint::new(coinbase1_txid, spending_vout_1),
            OutPoint::new(coinbase2_txid, spending_vout_2),
            total_value,
        );

        assert_eq!(spending_tx.input.len(), 2);

        let mut index = InMemoryIndex::new();
        let all_txs = vec![
            coinbase1.clone(),
            coinbase2.clone(),
            coinbase3.clone(),
            spending_tx.clone(),
        ];
        for tx in all_txs.iter() {
            index.add_tx(tx);
        }

        // Add index for txins spent by txouts
        // For this limited example there is two outs that are spet
        let mut heuristic = MultiInputHeuristic::new();

        for tx in all_txs.iter() {
            let tx_handle = index.compute_txid(tx.compute_txid()).with(&index);
            heuristic.merge_prevouts(&tx_handle);
        }

        assert_eq!(
            heuristic.uf.find(
                index
                    .compute_txid(coinbase1.compute_txid())
                    .txout_id(spending_vout_1)
            ),
            heuristic.uf.find(
                index
                    .compute_txid(coinbase2.compute_txid())
                    .txout_id(spending_vout_2)
            )
        );
        // TODO: more assertions here
        assert_ne!(
            heuristic
                .uf
                .find(index.compute_txid(coinbase2.compute_txid()).txout_id(7)),
            heuristic
                .uf
                .find(index.compute_txid(coinbase3.compute_txid()).txout_id(1))
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
