use bitcoin::Transaction;
use bitcoin::consensus::Decodable;
use serde_json::{Value, json};
use tx_indexer_fingerprints::types::{InputSortingType, OutputStructureType, OutputType};
use tx_indexer_fingerprints::{input, input_with_prevout, output, transaction};
use tx_indexer_primitives::HasVersion;

fn decode_tx(hex_str: &str) -> Result<Transaction, String> {
    let bytes = hex::decode(hex_str).map_err(|e| format!("Invalid hex: {e}"))?;
    Transaction::consensus_decode(&mut bytes.as_slice())
        .map_err(|e| format!("Invalid transaction: {e}"))
}

fn output_type_str(t: &OutputType) -> &'static str {
    match t {
        OutputType::OpReturn => "op_return",
        OutputType::NonStandard => "non_standard",
        OutputType::P2pkh => "p2pkh",
        OutputType::P2sh => "p2sh",
        OutputType::P2wpkh => "p2wpkh",
        OutputType::P2wsh => "p2wsh",
        OutputType::P2tr => "p2tr",
    }
}

fn input_order_str(t: &InputSortingType) -> &'static str {
    match t {
        InputSortingType::Single => "single",
        InputSortingType::Ascending => "ascending",
        InputSortingType::Descending => "descending",
        InputSortingType::Bip69 => "bip69",
        InputSortingType::Historical => "historical",
        InputSortingType::Unknown => "unknown",
    }
}

fn output_structure_str(t: &OutputStructureType) -> &'static str {
    match t {
        OutputStructureType::Single => "single",
        OutputStructureType::Double => "double",
        OutputStructureType::Multi => "multi",
        OutputStructureType::Bip69 => "bip69",
    }
}

fn fingerprint_spending_tx(tx: &Transaction) -> Value {
    let inputs: Vec<Value> = tx
        .input
        .iter()
        .map(|txin| {
            json!({
                "signals_rbf": input::signals_rbf(txin),
                "low_r_grinding": input::low_r_grinding(txin),
            })
        })
        .collect();

    let outputs: Vec<Value> = tx
        .output
        .iter()
        .map(|txout| {
            json!({
                "type": output_type_str(&output::output_type(txout)),
            })
        })
        .collect();

    json!({
        "version": tx.version(),
        "signals_rbf": transaction::tx_signals_rbf(&tx.input),
        "anti_fee_snipe": transaction::anti_fee_snipe(tx.lock_time.to_consensus_u32()),
        "output_structure": transaction::output_structure(&tx.output).iter().map(output_structure_str).collect::<Vec<_>>(),
        "inputs": inputs,
        "outputs": outputs,
    })
}

fn fingerprint_with_prevouts(tx: &Transaction, prev_outs: &[bitcoin::TxOut]) -> Value {
    let inputs: Vec<Value> = tx
        .input
        .iter()
        .zip(prev_outs.iter())
        .map(|(txin, prevout)| {
            json!({
                "signals_rbf": input::signals_rbf(txin),
                "low_r_grinding": input::low_r_grinding(txin),
                "input_type": output_type_str(&input_with_prevout::input_type(prevout)),
                "has_uncompressed_pubkey": input_with_prevout::has_uncompressed_pubkey(txin, prevout),
                "taproot_keyspend_non_default_sighash": input_with_prevout::taproot_keyspend_non_default_sighash(txin, prevout),
            })
        })
        .collect();

    let outputs: Vec<Value> = tx
        .output
        .iter()
        .map(|txout| {
            json!({
                "type": output_type_str(&output::output_type(txout)),
            })
        })
        .collect();

    json!({
        "version": tx.version(),
        "signals_rbf": transaction::tx_signals_rbf(&tx.input),
        "anti_fee_snipe": transaction::anti_fee_snipe(tx.lock_time.to_consensus_u32()),
        "output_structure": transaction::output_structure(&tx.output).iter().map(output_structure_str).collect::<Vec<_>>(),
        "address_reuse": transaction::address_reuse(&tx.output, prev_outs),
        "mixed_input_types": transaction::mixed_input_types(prev_outs),
        "input_order": transaction::input_order(&tx.input, prev_outs).iter().map(input_order_str).collect::<Vec<_>>(),
        "inputs": inputs,
        "outputs": outputs,
    })
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <tx_hex> [prev_tx_hex...]", args[0]);
        eprintln!();
        eprintln!("  tx_hex       Hex-encoded raw Bitcoin transaction to fingerprint");
        eprintln!("  prev_tx_hex  Hex-encoded raw previous transactions (one per input)");
        std::process::exit(1);
    }

    let tx = match decode_tx(&args[1]) {
        Ok(tx) => tx,
        Err(e) => {
            eprintln!("Error decoding transaction: {e}");
            std::process::exit(1);
        }
    };

    let output = if args.len() < 3 {
        fingerprint_spending_tx(&tx)
    } else {
        let prev_txs: Vec<Transaction> = args[2..]
            .iter()
            .enumerate()
            .map(|(i, hex)| match decode_tx(hex) {
                Ok(tx) => tx,
                Err(e) => {
                    eprintln!("Error decoding prev transaction #{}: {e}", i + 1);
                    std::process::exit(1);
                }
            })
            .collect();

        let prev_outs: Vec<bitcoin::TxOut> = tx
            .input
            .iter()
            .map(|txin| {
                prev_txs
                    .iter()
                    .find(|prev_tx| prev_tx.compute_txid() == txin.previous_output.txid)
                    .unwrap_or_else(|| {
                        eprintln!(
                            "Error: no prev tx found for input outpoint {}",
                            txin.previous_output
                        );
                        std::process::exit(1);
                    })
                    .output[txin.previous_output.vout as usize]
                    .clone()
            })
            .collect();

        fingerprint_with_prevouts(&tx, &prev_outs)
    };

    println!("{}", serde_json::to_string_pretty(&output).unwrap());
}
