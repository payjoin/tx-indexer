#[cfg(test)]
mod primitive_tests {
    use anyhow::Result;
    use bitcoin::{Amount, hashes::Hash};
    use std::{
        fs,
        path::PathBuf,
        sync::{Arc, Mutex},
    };

    use crate::integration::run_harness;
    use crate::parser::BlkFileHint;
    use crate::test_utils::temp_dir;
    use crate::{
        UnifiedStorage,
        dense::{DenseStorageBuilder, TxId, TxOutId},
    };

    /// Path to the multi-blk-file fixture (acts as a Bitcoin Core datadir).
    fn fixture_dir() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/multiple_block_files")
    }

    #[test]
    fn fixture_sync_from_tip_multiple_blk_files() -> Result<()> {
        use bitcoin_block_index::BlockIndex;

        let fixture = fixture_dir();
        let index_path = fixture.join("blocks/index");

        // Discover chain tip height and last blk file number from the block index.
        // Drop the BlockIndex before calling sync_from_tip so the LevelDB lock is released.
        let (tip_height, last_file) = {
            let mut index = BlockIndex::open(&index_path)?;
            let tip_hash = index.best_block()?;
            let tip_height = index.block_location(&tip_hash)?.height;
            let last_file = index.last_block_file()?;
            (tip_height, last_file)
        };

        // The fixture was created with a 1 KiB blk file limit, so the chain
        // must span at least 2 files.
        assert!(last_file >= 1, "fixture must have at least 2 blk files");

        // Index the whole chain forward using sync_from_tip.
        let tmp = temp_dir("fixture_multi_blk");
        let storage: UnifiedStorage = DenseStorageBuilder::sync_from_tip(fixture, tmp, tip_height)?
            .build()?
            .into();

        // This is a regtest chain with only coinbase transactions (one per block),
        // so total tx count == number of blocks == tip_height + 1.
        let total_txs = storage.dense_txids_len();
        assert_eq!(total_txs, (tip_height + 1) as usize);

        let txids = storage.dense_txids_from(0);
        assert_eq!(txids.len(), total_txs);

        // TODO: parse from genesis to tip and repeat assertions

        Ok(())
    }

    #[test]
    fn build_indices_stops_at_logical_blk_size() -> Result<()> {
        let fixture_blocks = fixture_dir().join("blocks");
        let plaintext = fs::read(fixture_blocks.join("blk00000.dat"))?;
        let logical_size = plaintext.len();

        let mut xor_key = [0u8; 8];
        let phase = logical_size % xor_key.len();
        // Make the zero-padded tail look like a plausible block header after XOR
        // decoding, so parsing past `data_len` would fail.
        let fake_tail_header = [0x11, 0x22, 0x33, 0x44, 0xff, 0xff, 0xff, 0xff];
        for (i, byte) in fake_tail_header.iter().enumerate() {
            xor_key[(phase + i) % xor_key.len()] = *byte;
        }

        let mut encrypted = plaintext.clone();
        for (i, byte) in encrypted.iter_mut().enumerate() {
            *byte ^= xor_key[i % xor_key.len()];
        }

        let tx_count_for = |dir_prefix: &str, blk_bytes: &[u8]| -> Result<u64> {
            let datadir = temp_dir(dir_prefix);
            let blocks_dir = datadir.join("blocks");
            fs::create_dir_all(&blocks_dir)?;
            fs::write(blocks_dir.join("xor.dat"), xor_key)?;
            fs::write(blocks_dir.join("blk00000.dat"), blk_bytes)?;

            let storage = DenseStorageBuilder::new(
                datadir,
                temp_dir("preallocated_blk_index"),
                0..100,
                vec![BlkFileHint {
                    file_no: 0,
                    height_first: 0,
                    height_last: u32::MAX,
                    data_len: Some(logical_size),
                }],
            )
            .build()?;

            Ok(storage.tx_count())
        };

        let baseline_tx_count = tx_count_for("preallocated_blk_fixture_baseline", &encrypted)?;

        let mut disk_bytes = encrypted.clone();
        // Appending zeros simulates the preallocated tail of the active blk file.
        disk_bytes.extend(std::iter::repeat_n(0u8, 4096));
        let tailed_tx_count = tx_count_for("preallocated_blk_fixture_tailed", &disk_bytes)?;

        assert!(
            baseline_tx_count > 0,
            "fixture should contribute at least one tx"
        );
        assert_eq!(
            tailed_tx_count, baseline_tx_count,
            "bytes past data_len must not change the parsed result"
        );
        Ok(())
    }

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
