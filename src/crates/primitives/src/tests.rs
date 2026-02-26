#[cfg(test)]
mod tests {
    use anyhow::Result;
    use bitcoin::{Amount, hashes::Hash};
    use std::sync::{Arc, Mutex};

    use crate::dense::{TxId, TxOutId};
    use crate::integration::run_harness;

    #[test]
    fn integration_mine_empty_block() -> Result<()> {
        run_harness(
            |harness| {
                let address = harness.client().new_address()?;
                harness.generate_blocks(1, &address)?;
                Ok(())
            },
            |_harness, storage, expected_txids| {
                let tx_counts = storage.tx_count();
                // Including the 101 the test harness creates
                assert_eq!(tx_counts, 103);
                assert_eq!(expected_txids.len(), 1);
                let want = expected_txids.iter().next().unwrap();
                let tx = storage.get_tx(*want.1);
                assert_eq!(tx.compute_txid(), *want.0);
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
                harness.send_to_address(&addr1, amount)?;
                harness.send_to_address(&addr2, amount)?;
                harness.generate_blocks(1, &addr1)?;
                Ok(())
            },
            |harness, storage, expected_txids| {
                for (want, dense_id) in expected_txids.iter() {
                    let tx = storage.get_tx(*dense_id);
                    assert_eq!(tx.compute_txid(), *want);
                    let rpc_tx = harness.get_raw_transaction(*want)?;
                    assert_eq!(tx.compute_txid(), rpc_tx.compute_txid());
                    assert_eq!(tx.input.len(), rpc_tx.input.len());
                    assert_eq!(tx.output.len(), rpc_tx.output.len());

                    let block = storage.block_of_tx(*dense_id);
                    let block_height = harness.get_block_count()?;
                    // Tx is in the tip block (get_block_count returns tip height)
                    assert_eq!(block, block_height);
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
                harness.send_to_address(&addr, Amount::from_sat(30_000))?;
                harness.generate_blocks(1, &addr)?;
                Ok(())
            },
            |_harness, storage, expected_txids| {
                // Pick a non-coinbase tx to test input/output ID round-trips
                let (_, dense_id) = expected_txids
                    .iter()
                    .find(|(_, id)| !storage.get_tx(**id).is_coinbase())
                    .expect("expected a non-coinbase tx");
                let dense_id = *dense_id;
                let tx = storage.get_tx(dense_id);
                assert!(!tx.is_coinbase());
                let txin_ids = storage.get_txin_ids(dense_id);
                let txout_ids = storage.get_txout_ids(dense_id);
                let (in_start, in_end) = storage.tx_in_range(dense_id);
                let (out_start, out_end) = storage.tx_out_range(dense_id);

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
                harness.send_to_address(&addr, Amount::from_sat(30_000))?;
                harness.generate_blocks(1, &addr)?;
                Ok(())
            },
            |_harness, storage, expected_txids| {
                let (_, dense_id) = expected_txids
                    .iter()
                    .find(|(_, id)| !storage.get_tx(**id).is_coinbase())
                    .expect("expected a non-coinbase tx");
                let dense_id = *dense_id;
                let tx = storage.get_tx(dense_id);
                let txin_ids = storage.get_txin_ids(dense_id);

                assert_eq!(txin_ids.len(), tx.input.len());
                assert_eq!(txin_ids.len(), 1);

                let in_id = txin_ids[0];
                let txin = storage.get_txin(in_id);
                assert_eq!(txin.previous_output, tx.input[0].previous_output);
                assert!(!txin.previous_output.is_null());
                let prev_txid = tx.input[0].previous_output.txid;
                let prev_dense = (0..storage.tx_count())
                    .find_map(|i| {
                        let id = TxId::new(i as u32);
                        if storage.get_tx(id).compute_txid() == prev_txid {
                            Some(id)
                        } else {
                            None
                        }
                    })
                    .expect("prevout tx should exist in storage");
                let (start, _end) = storage.tx_out_range(prev_dense);
                let out_id = TxOutId::new(start + txin.previous_output.vout as u64);
                assert_eq!(storage.prevout_for_in(in_id), Some(out_id));
                assert_eq!(storage.spender_for_out(out_id), Some(in_id));

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
                harness.send_to_address(&address, Amount::from_sat(30_000))?;
                harness.generate_blocks(1, &coinbase_addr)?;

                Ok(())
            },
            |_harness, storage, expected_txids| {
                let target_spk = spk_hash
                    .lock()
                    .expect("lock poisoned")
                    .expect("spk hash set");
                let (dense_id, vout) = expected_txids
                    .iter()
                    .find_map(|(_, dense_id)| {
                        let tx = storage.get_tx(*dense_id);
                        let vout = tx.output.iter().position(|output| {
                            *output.script_pubkey.script_hash().as_byte_array() == target_spk
                        })?;
                        Some((*dense_id, vout))
                    })
                    .ok_or_else(|| anyhow::anyhow!("no matching output for spk hash"))?;
                let (start, _end) = storage.tx_out_range(dense_id);
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
