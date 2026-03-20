use bitcoin::consensus::Decodable;
use bitcoin::{Amount, OutPoint, PublicKey, ScriptBuf, Transaction, TxIn, TxOut};
use std::str::FromStr;

use crate::fingerprints::input::{low_r_grinding, signals_rbf};
use crate::fingerprints::input_with_prevout::{has_uncompressed_pubkey, input_type};
use crate::fingerprints::output::output_type;
use crate::fingerprints::transaction::{
    address_reuse, anti_fee_snipe, input_order, mixed_input_types, output_structure, round_fee,
    tx_signals_rbf, tx_version,
};
use crate::types::{InputSortingType, OutputStructureType, OutputType};

fn get_tx_from_hex(hex_str: &str) -> Transaction {
    let bytes = hex::decode(hex_str).unwrap();
    Transaction::consensus_decode(&mut bytes.as_slice()).unwrap()
}

fn create_p2pk_script(compressed: bool) -> ScriptBuf {
    let secp = bitcoin::secp256k1::Secp256k1::new();
    let mut rng = rand::thread_rng();
    let (secret_key, _) = secp.generate_keypair(&mut rng);
    let pubkey = PublicKey::new(secret_key.public_key(&secp));

    let pubkey_bytes = if compressed {
        pubkey.to_bytes()
    } else {
        let mut uncompressed = vec![];
        uncompressed.extend_from_slice(&pubkey.inner.serialize_uncompressed());
        PublicKey::from_slice(&uncompressed).unwrap().to_bytes()
    };

    let pubkey = PublicKey::from_slice(&pubkey_bytes).unwrap();
    assert_eq!(pubkey.compressed, compressed);
    ScriptBuf::new_p2pk(&pubkey)
}

fn make_txout(script_pubkey: ScriptBuf) -> TxOut {
    TxOut {
        value: Amount::from_sat(1000),
        script_pubkey,
    }
}

fn dummy_txin() -> TxIn {
    TxIn {
        previous_output: OutPoint::from_str(
            "0000000000000000000000000000000000000000000000000000000000000000:0",
        )
        .unwrap(),
        script_sig: ScriptBuf::new(),
        sequence: bitcoin::Sequence::MAX,
        witness: bitcoin::Witness::new(),
    }
}

// --- classify tests ---

#[test]
fn test_classify_p2pkh() {
    let addr = bitcoin::Address::from_str("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa")
        .unwrap()
        .require_network(bitcoin::Network::Bitcoin)
        .unwrap();
    let spk = addr.script_pubkey();
    assert_eq!(
        crate::classify_script_pubkey(&spk.to_bytes()),
        OutputType::P2pkh
    );
}

#[test]
fn test_classify_p2tr() {
    // A valid P2TR scriptPubKey: OP_1 <32-byte-key>
    let spk = hex::decode("5120a60869f0dbcf1dc659c9cecbee8b89892408b072aba4c3bc5d42dba934a82e03")
        .unwrap();
    assert_eq!(crate::classify_script_pubkey(&spk), OutputType::P2tr);
}

#[test]
fn test_classify_op_return() {
    // OP_RETURN <data>
    let spk = hex::decode("6a0461746f6d").unwrap();
    assert_eq!(crate::classify_script_pubkey(&spk), OutputType::OpReturn);
}

// --- input fingerprint tests ---

#[test]
fn test_input_signals_rbf() {
    let mut txin = dummy_txin();
    assert!(!signals_rbf(&txin));

    txin.sequence = bitcoin::Sequence::ENABLE_RBF_NO_LOCKTIME;
    assert!(signals_rbf(&txin));
}

#[test]
fn test_low_r_grinding() {
    let tx = hex::decode("02000000000102fbc729acd99cee2d45267529dc5b350ea435fe213969fcb54fb3700f88c8184c0100000000fdffffff8d028c4c4836fb6cacb5c12bac2deb8388283360a9947fef734740cd77debbd00200000000fdffffff021826000000000000160014ef70029f96d2dec0d33b2e60838b5ae0db78b78d30a23d0000000000160014fa06516cb2ffdfd3497185660ce286b1aeebef5e0247304402202c0cc3d4d4d787dbbcfe039f12bb06b7bc3da6d52fa804d657b4d5f9c3c3260a0220171e6703016d8b1061bc82d83581045fae332093d76743118a255b5a6fc69e76012103416a98f79cd8f047cbf1c9f9e5c9848620043360cf606b35e6dd324b3896d78002483045022100fcc4e7d3805d54725ca6e539a1e04b576761e938e81e4c6f79db8c38aeecac91022030ae46783c7a5950f69302a2db443fa58ea2bba868f9ca61027d01578ad003d101210308e9e1ed1d187ce8ed6299989fdff6dfe566d116ac2bae0a3fc6a2c28610cd74e44e2700").unwrap();
    let tx = Transaction::consensus_decode(&mut tx.as_slice()).unwrap();
    assert!(low_r_grinding(&tx.input[0]));
    assert!(!low_r_grinding(&tx.input[1]));
}

// --- input-with-prevout tests ---

#[test]
fn test_uncompressed_pubkey_p2pk_compressed() {
    let prevout = make_txout(create_p2pk_script(true));
    assert!(!has_uncompressed_pubkey(&dummy_txin(), &prevout));
}

#[test]
fn test_uncompressed_pubkey_p2pk_uncompressed() {
    let prevout = make_txout(create_p2pk_script(false));
    assert!(has_uncompressed_pubkey(&dummy_txin(), &prevout));
}

#[test]
fn test_uncompressed_pubkey_non_p2pk_prevout() {
    let address = bitcoin::Address::from_str("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa")
        .unwrap()
        .require_network(bitcoin::Network::Bitcoin)
        .unwrap();
    let prevout = make_txout(address.script_pubkey());
    assert!(!has_uncompressed_pubkey(&dummy_txin(), &prevout));
}

#[test]
fn test_uncompressed_pubkey_p2pkh_with_uncompressed_key_in_scriptsig() {
    let p2pkh_spk = bitcoin::Address::from_str("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa")
        .unwrap()
        .require_network(bitcoin::Network::Bitcoin)
        .unwrap()
        .script_pubkey();

    let uncompressed_pubkey_bytes = {
        let secp = bitcoin::secp256k1::Secp256k1::new();
        let mut rng = rand::thread_rng();
        let (sk, _) = secp.generate_keypair(&mut rng);
        let pk = PublicKey::new(sk.public_key(&secp));
        pk.inner.serialize_uncompressed().to_vec()
    };
    assert_eq!(uncompressed_pubkey_bytes.len(), 65);
    assert_eq!(uncompressed_pubkey_bytes[0], 0x04);

    let mut builder = bitcoin::script::Builder::new();
    builder = builder
        .push_slice(bitcoin::script::PushBytesBuf::try_from(uncompressed_pubkey_bytes).unwrap());
    let script_sig = builder.into_script();
    let txin = TxIn {
        script_sig,
        ..dummy_txin()
    };
    let prevout = make_txout(p2pkh_spk);
    assert!(has_uncompressed_pubkey(&txin, &prevout));
}

#[test]
fn test_input_type_classification() {
    let addr = bitcoin::Address::from_str("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa")
        .unwrap()
        .require_network(bitcoin::Network::Bitcoin)
        .unwrap();
    let prevout = make_txout(addr.script_pubkey());
    assert_eq!(input_type(&prevout), OutputType::P2pkh);
}

// --- output tests ---

#[test]
fn test_output_type() {
    let addr = bitcoin::Address::from_str("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa")
        .unwrap()
        .require_network(bitcoin::Network::Bitcoin)
        .unwrap();
    let txout = make_txout(addr.script_pubkey());
    assert_eq!(output_type(&txout), OutputType::P2pkh);
}

// --- transaction-level tests ---

#[test]
fn test_tx_signals_rbf() {
    let mut txin1 = dummy_txin();
    let txin2 = dummy_txin();

    // No RBF
    assert!(!tx_signals_rbf(&[txin1.clone(), txin2.clone()]));

    // One input signals RBF
    txin1.sequence = bitcoin::Sequence::ENABLE_RBF_NO_LOCKTIME;
    assert!(tx_signals_rbf(&[txin1, txin2]));
}

#[test]
fn test_anti_fee_snipe() {
    assert!(!anti_fee_snipe(0));
    assert!(anti_fee_snipe(800000));
}

#[test]
fn test_address_reuse() {
    let addr1 = bitcoin::Address::from_str("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa")
        .unwrap()
        .require_network(bitcoin::Network::Bitcoin)
        .unwrap();
    let addr2 = bitcoin::Address::from_str("1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2")
        .unwrap()
        .require_network(bitcoin::Network::Bitcoin)
        .unwrap();

    let prevout1 = make_txout(addr1.script_pubkey());
    let output1 = make_txout(addr1.script_pubkey());
    let output2 = make_txout(addr2.script_pubkey());

    // Reuse detected
    assert!(address_reuse(&[output1], &[prevout1.clone()]));

    // No reuse
    assert!(!address_reuse(&[output2], &[prevout1]));
}

#[test]
fn test_mixed_input_types() {
    let p2pkh_addr = bitcoin::Address::from_str("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa")
        .unwrap()
        .require_network(bitcoin::Network::Bitcoin)
        .unwrap();

    let prevout1 = make_txout(p2pkh_addr.script_pubkey());
    let prevout2 = make_txout(p2pkh_addr.script_pubkey());

    // Same types
    assert!(!mixed_input_types(&[prevout1.clone(), prevout2]));

    // Mixed: P2PKH + P2TR
    let p2tr_spk =
        hex::decode("5120a60869f0dbcf1dc659c9cecbee8b89892408b072aba4c3bc5d42dba934a82e03")
            .unwrap();
    let prevout3 = make_txout(ScriptBuf::from_bytes(p2tr_spk));
    assert!(mixed_input_types(&[prevout1, prevout3]));
}

// --- input_order tests ---

fn make_txin_with_outpoint(txid_hex: &str, vout: u32) -> TxIn {
    TxIn {
        previous_output: OutPoint::new(txid_hex.parse().unwrap(), vout),
        script_sig: ScriptBuf::new(),
        sequence: bitcoin::Sequence::MAX,
        witness: bitcoin::Witness::new(),
    }
}

fn make_txout_with_value(sats: u64) -> TxOut {
    TxOut {
        value: Amount::from_sat(sats),
        script_pubkey: ScriptBuf::new(),
    }
}

#[test]
fn test_input_order_single() {
    let inputs = [dummy_txin()];
    let prevouts = [make_txout_with_value(1000)];
    assert_eq!(
        input_order(&inputs, &prevouts),
        vec![InputSortingType::Single]
    );
}

#[test]
fn test_input_order_ascending_values() {
    // Two inputs with ascending prevout values
    let inputs = [
        make_txin_with_outpoint(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            0,
        ),
        make_txin_with_outpoint(
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            0,
        ),
    ];
    let prevouts = [make_txout_with_value(100), make_txout_with_value(200)];
    let result = input_order(&inputs, &prevouts);
    assert!(result.contains(&InputSortingType::Ascending));
}

#[test]
fn test_input_order_descending_values() {
    let inputs = [
        make_txin_with_outpoint(
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            0,
        ),
        make_txin_with_outpoint(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            0,
        ),
    ];
    let prevouts = [make_txout_with_value(200), make_txout_with_value(100)];
    let result = input_order(&inputs, &prevouts);
    assert!(result.contains(&InputSortingType::Descending));
}

#[test]
fn test_input_order_equal_values_skips_asc_desc() {
    // Equal values should not report ascending or descending
    let inputs = [
        make_txin_with_outpoint(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            0,
        ),
        make_txin_with_outpoint(
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            0,
        ),
    ];
    let prevouts = [make_txout_with_value(100), make_txout_with_value(100)];
    let result = input_order(&inputs, &prevouts);
    assert!(!result.contains(&InputSortingType::Ascending));
    assert!(!result.contains(&InputSortingType::Descending));
    // But BIP69 should still be checked — these txids are already in display order
    assert!(result.contains(&InputSortingType::Bip69));
}

#[test]
fn test_input_order_bip69() {
    // BIP69: sort by txid display order (reversed wire bytes), then vout.
    // Wire bytes for "aa..aa" are [0xaa; 32]. Reversed = [0xaa; 32].
    // Wire bytes for "bb..bb" are [0xbb; 32]. Reversed = [0xbb; 32].
    // aa < bb in display order, so aa should come first for BIP69.
    let inputs = [
        make_txin_with_outpoint(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            0,
        ),
        make_txin_with_outpoint(
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            0,
        ),
    ];
    let prevouts = [make_txout_with_value(500), make_txout_with_value(100)];
    let result = input_order(&inputs, &prevouts);
    assert!(result.contains(&InputSortingType::Bip69));
}

#[test]
fn test_input_order_not_bip69() {
    // Reverse order of txids — not BIP69
    let inputs = [
        make_txin_with_outpoint(
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            0,
        ),
        make_txin_with_outpoint(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            0,
        ),
    ];
    let prevouts = [make_txout_with_value(100), make_txout_with_value(500)];
    let result = input_order(&inputs, &prevouts);
    assert!(!result.contains(&InputSortingType::Bip69));
    // Values are ascending though
    assert!(result.contains(&InputSortingType::Ascending));
}

#[test]
fn test_input_order_unknown() {
    // Not ascending, not descending, not BIP69
    let inputs = [
        make_txin_with_outpoint(
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            0,
        ),
        make_txin_with_outpoint(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            0,
        ),
        make_txin_with_outpoint(
            "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
            0,
        ),
    ];
    let prevouts = [
        make_txout_with_value(200),
        make_txout_with_value(100),
        make_txout_with_value(300),
    ];
    let result = input_order(&inputs, &prevouts);
    assert_eq!(result, vec![InputSortingType::Unknown]);
}

// --- output_structure tests ---

#[test]
fn test_output_structure_single() {
    let outputs = [make_txout_with_value(1000)];
    assert_eq!(
        output_structure(&outputs),
        vec![OutputStructureType::Single]
    );
}

#[test]
fn test_output_structure_double() {
    let outputs = [make_txout_with_value(100), make_txout_with_value(200)];
    let result = output_structure(&outputs);
    assert!(result.contains(&OutputStructureType::Double));
    // Values are ascending → BIP69
    assert!(result.contains(&OutputStructureType::Bip69));
}

#[test]
fn test_output_structure_multi() {
    let outputs = [
        make_txout_with_value(100),
        make_txout_with_value(200),
        make_txout_with_value(300),
    ];
    let result = output_structure(&outputs);
    assert!(result.contains(&OutputStructureType::Multi));
    assert!(result.contains(&OutputStructureType::Bip69));
}

#[test]
fn test_output_structure_not_bip69() {
    let outputs = [make_txout_with_value(300), make_txout_with_value(100)];
    let result = output_structure(&outputs);
    assert!(result.contains(&OutputStructureType::Double));
    assert!(!result.contains(&OutputStructureType::Bip69));
}

#[test]
fn test_output_structure_bip69_with_duplicate_amounts() {
    // Duplicate amounts — BIP69 requires sorting by (value, scriptPubKey)
    let addr1 = bitcoin::Address::from_str("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa")
        .unwrap()
        .require_network(bitcoin::Network::Bitcoin)
        .unwrap();
    let addr2 = bitcoin::Address::from_str("1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2")
        .unwrap()
        .require_network(bitcoin::Network::Bitcoin)
        .unwrap();

    let spk1 = addr1.script_pubkey();
    let spk2 = addr2.script_pubkey();

    // Ensure we know which scriptPubKey sorts first
    let (first_spk, second_spk) = if spk1.to_bytes() < spk2.to_bytes() {
        (spk1.clone(), spk2.clone())
    } else {
        (spk2.clone(), spk1.clone())
    };

    let outputs_sorted = [
        TxOut {
            value: Amount::from_sat(1000),
            script_pubkey: first_spk.clone(),
        },
        TxOut {
            value: Amount::from_sat(1000),
            script_pubkey: second_spk.clone(),
        },
    ];
    let result = output_structure(&outputs_sorted);
    assert!(result.contains(&OutputStructureType::Bip69));

    // Reverse order — not BIP69
    let outputs_unsorted = [
        TxOut {
            value: Amount::from_sat(1000),
            script_pubkey: second_spk,
        },
        TxOut {
            value: Amount::from_sat(1000),
            script_pubkey: first_spk,
        },
    ];
    let result = output_structure(&outputs_unsorted);
    assert!(!result.contains(&OutputStructureType::Bip69));
}

// --- tx_version tests ---

#[test]
fn test_tx_version() {
    let tx_v1 = get_tx_from_hex(
        "02000000000102fbc729acd99cee2d45267529dc5b350ea435fe213969fcb54fb3700f88c8184c0100000000fdffffff8d028c4c4836fb6cacb5c12bac2deb8388283360a9947fef734740cd77debbd00200000000fdffffff021826000000000000160014ef70029f96d2dec0d33b2e60838b5ae0db78b78d30a23d0000000000160014fa06516cb2ffdfd3497185660ce286b1aeebef5e0247304402202c0cc3d4d4d787dbbcfe039f12bb06b7bc3da6d52fa804d657b4d5f9c3c3260a0220171e6703016d8b1061bc82d83581045fae332093d76743118a255b5a6fc69e76012103416a98f79cd8f047cbf1c9f9e5c9848620043360cf606b35e6dd324b3896d78002483045022100fcc4e7d3805d54725ca6e539a1e04b576761e938e81e4c6f79db8c38aeecac91022030ae46783c7a5950f69302a2db443fa58ea2bba868f9ca61027d01578ad003d101210308e9e1ed1d187ce8ed6299989fdff6dfe566d116ac2bae0a3fc6a2c28610cd74e44e2700",
    );
    // This tx is version 2
    assert_eq!(tx_version(&tx_v1), 2);
}

// --- round_fee tests ---

#[test]
fn test_round_fee_exact_thousand() {
    // Fee = 10000 - 9000 = 1000 sats (round)
    let prevouts = [make_txout_with_value(10000)];
    let outputs = [make_txout_with_value(9000)];
    assert_eq!(round_fee(&prevouts, &outputs), Some(true));
}

#[test]
fn test_round_fee_not_round() {
    // Fee = 10000 - 9743 = 257 sats (not round)
    let prevouts = [make_txout_with_value(10000)];
    let outputs = [make_txout_with_value(9743)];
    assert_eq!(round_fee(&prevouts, &outputs), Some(false));
}

#[test]
fn test_round_fee_zero() {
    // Fee = 0 sats (not considered round — no fee is not a manual fee)
    let prevouts = [make_txout_with_value(10000)];
    let outputs = [make_txout_with_value(10000)];
    assert_eq!(round_fee(&prevouts, &outputs), Some(false));
}

#[test]
fn test_round_fee_overflow() {
    // Output > input — invalid, returns None
    let prevouts = [make_txout_with_value(1000)];
    let outputs = [make_txout_with_value(2000)];
    assert_eq!(round_fee(&prevouts, &outputs), None);
}

// --- integration test with real tx ---

#[test]
fn test_electrum_tx_fingerprints() {
    // Electrum tx: 5d857401648a667303cde43295bce1326e6329353eac3dddf15b151e701405e7
    let tx = get_tx_from_hex(
        "02000000000102ac5718a0e7b3ee13ce2f273aa9c6a04becf8a1696edb75d3217c0d3790a620860000000000fdffffff74e1d8045cfe6b823943db609ceb3aa13216a936a9e18b92e26db770a8e4eae60000000000fdffffff02f6250000000000001600145333aa7bcef7bd632edaf5a326d4c6085417282d133f0000000000001976a914c8f57d6b8bc08fa211c71b8d255e7c4b25bd432288ac02473044022037059673792d5af9ab1cf5fc8ccf3c1c1ad300e9e6c25edda7a172e455d49e07022046d2c2638c129a8c9a54ca5adb5df01bde564066c36edade43c3845b3d25940101210202ca6c82b9cc52f7a8c34de6a6ccd807d8437a8368ddf7638a2b50002e745b360247304402207b3d3c39ee66bdaa509094072ae629794bd7ef0f14694f0e3695d89ed573c57202205cc9b6d059500ccf621621a657115e33c51064efad2dcf352ad32c69b0ae6ab301210202ca6c82b9cc52f7a8c34de6a6ccd807d8437a8368ddf7638a2b50002e745b3670360c00",
    );

    // Both inputs signal RBF (sequence < 0xfffffffe)
    assert!(signals_rbf(&tx.input[0]));
    assert!(signals_rbf(&tx.input[1]));
    assert!(tx_signals_rbf(&tx.input));

    // Anti-fee-sniping: locktime is non-zero
    assert!(anti_fee_snipe(tx.lock_time.to_consensus_u32()));

    // Low-R grinding on both inputs
    assert!(low_r_grinding(&tx.input[0]));
    assert!(low_r_grinding(&tx.input[1]));

    // Output types
    assert_eq!(output_type(&tx.output[0]), OutputType::P2wpkh);
    assert_eq!(output_type(&tx.output[1]), OutputType::P2pkh);

    // Output structure: 2 outputs (Double), values 9718 < 16147 so BIP69
    let structure = output_structure(&tx.output);
    assert!(structure.contains(&OutputStructureType::Double));
    assert!(structure.contains(&OutputStructureType::Bip69));
}
