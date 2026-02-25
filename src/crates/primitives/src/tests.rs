#[cfg(test)]
mod tests {
    use anyhow::Result;
    use bitcoin::{Amount, hashes::Hash};
    use std::sync::{Arc, Mutex};

    use crate::dense::TxOutId;
    use crate::integration::{HarnessOut, run_harness};

    #[test]
    fn integration_mine_empty_block() -> Result<()> {
        run_harness(
            |harness| {
                let address = harness.client().new_address()?;
                harness.generate_blocks(1, &address)?;
                let count = harness.get_block_count()?;
                let best = harness.best_block_hash()?;
                let block = harness.get_block(best)?;
                let coinbase_txid = block.txdata[0].compute_txid();
                Ok(HarnessOut {
                    expected_txids: vec![coinbase_txid],
                    block_count_after: count,
                })
            },
            |_harness, storage, out, dense_txids| {
                assert_eq!(out.expected_txids.len(), 1, "one coinbase tx");
                let want = out.expected_txids[0];
                // We parsed block_count_after blocks; last block is the one we just mined
                assert!(
                    !dense_txids.is_empty(),
                    "expected at least one dense TxId (got {})",
                    dense_txids.len()
                );
                let dense_id = dense_txids
                    .get(&want)
                    .ok_or_else(|| anyhow::anyhow!("no dense TxId for coinbase {}", want))?;
                let tx = storage.get_tx(*dense_id);
                assert_eq!(tx.compute_txid(), want);
                // Coinbase has one input (null prevout) and at least one output
                assert_eq!(tx.input.len(), 1);
                assert!(tx.input[0].previous_output.is_null());
                assert!(!tx.output.is_empty());
                Ok(())
            },
        )
    }

    #[test]
    fn integration_mine_block_with_transactions() -> Result<()> {
        run_harness(
            |harness| {
                let addr1 = harness.client().new_address()?;
                let addr2 = harness.client().new_address()?;
                let amount = Amount::from_sat(50_000);
                let txid1 = harness.send_to_address(&addr1, amount)?;
                let txid2 = harness.send_to_address(&addr2, amount)?;
                harness.generate_blocks(1, &addr1)?;
                let count = harness.get_block_count()?;
                let best = harness.best_block_hash()?;
                let block = harness.get_block(best)?;
                let mut expected = vec![block.txdata[0].compute_txid()];
                for tx in &block.txdata[1..] {
                    expected.push(tx.compute_txid());
                }
                assert!(
                    expected.contains(&txid1) && expected.contains(&txid2),
                    "block should contain both sent txs"
                );
                Ok(HarnessOut {
                    expected_txids: expected,
                    block_count_after: count,
                })
            },
            |harness, storage, out, dense_txids| {
                for want in &out.expected_txids {
                    let dense_id = dense_txids
                        .get(want)
                        .ok_or_else(|| anyhow::anyhow!("no dense TxId for {}", want))?;
                    let tx = storage.get_tx(*dense_id);
                    assert_eq!(tx.compute_txid(), *want);
                    let rpc_tx = harness.get_raw_transaction(*want)?;
                    assert_eq!(tx.compute_txid(), rpc_tx.compute_txid());
                    assert_eq!(tx.input.len(), rpc_tx.input.len());
                    assert_eq!(tx.output.len(), rpc_tx.output.len());

                    let block = storage.block_of_tx(*dense_id);
                    assert_eq!(block, out.block_count_after);
                    let (start, end) = storage.tx_range_for_block(block);
                    assert!(dense_id.index() >= start);
                    assert!(dense_id.index() < end);
                }
                Ok(())
            },
        )
    }

    #[test]
    fn integration_dense_ids_roundtrip() -> Result<()> {
        run_harness(
            |harness| {
                let addr = harness.client().new_address()?;
                let _txid = harness.send_to_address(&addr, Amount::from_sat(30_000))?;
                harness.generate_blocks(1, &addr)?;
                let count = harness.get_block_count()?;
                let best = harness.best_block_hash()?;
                let block = harness.get_block(best)?;
                let mut expected = vec![block.txdata[0].compute_txid()];
                for tx in &block.txdata[1..] {
                    expected.push(tx.compute_txid());
                }
                Ok(HarnessOut {
                    expected_txids: expected,
                    block_count_after: count,
                })
            },
            |harness, storage, out, dense_txids| {
                // Pick a non-coinbase tx to test input/output ID round-trips
                let want = out
                    .expected_txids
                    .iter()
                    .find(|id| {
                        let tx = harness.get_raw_transaction(**id).unwrap();
                        !tx.input[0].previous_output.is_null()
                    })
                    .copied()
                    .ok_or_else(|| anyhow::anyhow!("no non-coinbase tx in block"))?;

                let dense_id = dense_txids
                    .get(&want)
                    .ok_or_else(|| anyhow::anyhow!("no dense TxId for {}", want))?;

                let tx = storage.get_tx(*dense_id);
                let txin_ids = storage.get_txin_ids(*dense_id);
                let txout_ids = storage.get_txout_ids(*dense_id);
                let (in_start, in_end) = storage.tx_in_range(*dense_id);
                let (out_start, out_end) = storage.tx_out_range(*dense_id);

                assert_eq!(txin_ids.len(), tx.input.len());
                assert_eq!(txout_ids.len(), tx.output.len());
                assert_eq!(in_end - in_start, txin_ids.len() as u64);
                assert_eq!(out_end - out_start, txout_ids.len() as u64);

                for (i, id) in txin_ids.iter().enumerate() {
                    assert_eq!(id.index(), in_start + i as u64);
                }
                for (i, id) in txout_ids.iter().enumerate() {
                    assert_eq!(id.index(), out_start + i as u64);
                }

                for (i, id) in txin_ids.iter().enumerate() {
                    let txin = storage.get_txin(*id);
                    assert_eq!(txin.previous_output, tx.input[i].previous_output);
                }
                for (i, id) in txout_ids.iter().enumerate() {
                    let txout = storage.get_txout(*id);
                    assert_eq!(txout.value, tx.output[i].value);
                    assert_eq!(txout.script_pubkey, tx.output[i].script_pubkey);
                }

                Ok(())
            },
        )
    }

    #[test]
    fn integration_prevout_spender_indexes() -> Result<()> {
        run_harness(
            |harness| {
                let addr = harness.client().new_address()?;
                let coinbase_addr = harness.client().new_address()?;
                harness.generate_blocks(1, &coinbase_addr)?;
                let _txid = harness.send_to_address(&addr, Amount::from_sat(30_000))?;
                harness.generate_blocks(1, &addr)?;
                let count = harness.get_block_count()?;
                let best = harness.best_block_hash()?;
                let block = harness.get_block(best)?;
                let mut expected = vec![block.txdata[0].compute_txid()];
                for tx in &block.txdata[1..] {
                    expected.push(tx.compute_txid());
                }
                Ok(HarnessOut {
                    expected_txids: expected,
                    block_count_after: count,
                })
            },
            |harness, storage, out, dense_txids| {
                let want = out
                    .expected_txids
                    .iter()
                    .find(|id| {
                        let tx = harness.get_raw_transaction(**id).unwrap();
                        !tx.input[0].previous_output.is_null()
                    })
                    .copied()
                    .ok_or_else(|| anyhow::anyhow!("no non-coinbase tx in block"))?;

                let dense_id = dense_txids
                    .get(&want)
                    .ok_or_else(|| anyhow::anyhow!("no dense TxId for {}", want))?;
                let tx = storage.get_tx(*dense_id);
                let txin_ids = storage.get_txin_ids(*dense_id);

                assert_eq!(txin_ids.len(), tx.input.len());

                for (i, in_id) in txin_ids.iter().enumerate() {
                    let txin = storage.get_txin(*in_id);
                    assert_eq!(txin.previous_output, tx.input[i].previous_output);
                    if txin.previous_output.is_null() {
                        assert_eq!(storage.prevout_for_in(*in_id), None);
                    } else {
                        let prev_txid = txin.previous_output.txid;
                        let prev_dense = dense_txids
                            .get(&prev_txid)
                            .ok_or_else(|| anyhow::anyhow!("no dense TxId for {}", prev_txid))?;
                        let (start, _end) = storage.tx_out_range(*prev_dense);
                        let out_id = TxOutId::new(start + txin.previous_output.vout as u64);
                        assert_eq!(storage.prevout_for_in(*in_id), Some(out_id));
                        assert_eq!(storage.spender_for_out(out_id), Some(*in_id));
                    }
                }

                Ok(())
            },
        )
    }

    #[test]
    fn integration_spk_index_lookup() -> Result<()> {
        let spk_hash = Arc::new(Mutex::new(None));

        run_harness(
            |harness| {
                let address = harness.client().new_address()?;
                let coinbase_addr = harness.client().new_address()?;
                let spk = address.script_pubkey();
                let hash = spk.script_hash();
                let mut bytes = [0u8; 20];
                bytes.copy_from_slice(&hash.to_raw_hash()[..]);
                *spk_hash.lock().expect("lock poisoned") = Some(bytes);

                harness.generate_blocks(1, &coinbase_addr)?;
                let txid = harness.send_to_address(&address, Amount::from_sat(30_000))?;
                harness.generate_blocks(1, &coinbase_addr)?;
                let count = harness.get_block_count()?;

                Ok(HarnessOut {
                    expected_txids: vec![txid],
                    block_count_after: count,
                })
            },
            |harness, storage, out, dense_txids| {
                let txid = out.expected_txids[0];
                let dense_id = dense_txids
                    .get(&txid)
                    .ok_or_else(|| anyhow::anyhow!("no dense TxId for {}", txid))?;
                let tx = harness.get_raw_transaction(txid)?;
                let (start, _end) = storage.tx_out_range(*dense_id);

                let target_spk = spk_hash
                    .lock()
                    .expect("lock poisoned")
                    .expect("spk hash set");
                let vout = tx
                    .output
                    .iter()
                    .position(|output| {
                        *output.script_pubkey.script_hash().as_byte_array() == target_spk
                    })
                    .ok_or_else(|| anyhow::anyhow!("no matching output for spk hash"))?;
                let expected = TxOutId::new(start + vout as u64);

                let got = storage
                    .script_pubkey_to_txout_id(&target_spk)
                    .map_err(|e| anyhow::anyhow!("spk lookup failed: {:?}", e))?;
                assert_eq!(got, Some(expected));

                Ok(())
            },
        )
    }
}
