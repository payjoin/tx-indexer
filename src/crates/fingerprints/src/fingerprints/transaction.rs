use std::collections::HashSet;

use bitcoin::Amount;
use tx_indexer_primitives::{HasPrevOutput, HasScriptPubkey, HasSequence, HasValue, OutputType};

use crate::types::{InputSortingType, OutputStructureType};

/// Returns true if any input signals RBF.
pub fn tx_signals_rbf(inputs: &[impl HasSequence]) -> bool {
    inputs.iter().any(|input| input.sequence() < 0xfffffffe)
}

/// Returns true if locktime is non-zero (heuristic for anti-fee-sniping).
pub fn anti_fee_snipe(locktime: u32) -> bool {
    locktime != 0
}

/// Returns true if any output scriptPubKey matches any prevout scriptPubKey.
pub fn address_reuse(outputs: &[impl HasScriptPubkey], prevouts: &[impl HasScriptPubkey]) -> bool {
    let input_scripts: HashSet<Vec<u8>> =
        prevouts.iter().map(|p| p.script_pubkey_bytes()).collect();
    let output_scripts: HashSet<Vec<u8>> =
        outputs.iter().map(|o| o.script_pubkey_bytes()).collect();

    !input_scripts.is_disjoint(&output_scripts)
}

/// Returns true if prevouts have more than one distinct scriptPubKey type.
pub fn mixed_input_types(prevouts: &[impl HasScriptPubkey]) -> bool {
    let types: HashSet<OutputType> = prevouts.iter().map(|p| p.output_type()).collect();
    types.len() > 1
}

/// Returns the input sorting types detected in the transaction.
///
/// `inputs` provides outpoint data for BIP69 checking.
/// `prevout_values` provides the value of each prevout (in input order) for
/// ascending/descending checking.
// TODO: should only have one argument. A rich input type that has prevout values and outpoint.
pub fn input_order<I, P>(inputs: &[I], prevout_values: &[P]) -> Vec<InputSortingType>
where
    I: HasPrevOutput,
    P: HasValue,
{
    if inputs.len() == 1 {
        return vec![InputSortingType::Single];
    }

    let mut sorting_types = Vec::new();
    let amounts: Vec<Amount> = prevout_values.iter().map(|p| p.value()).collect();

    // Only check ascending/descending when amounts are not all equal — equal amounts
    // trivially satisfy both orderings and reveal nothing about sorting intent.
    let all_equal = amounts.windows(2).all(|w| w[0] == w[1]);
    if !amounts.is_empty() && !all_equal {
        let mut sorted_amounts = amounts.clone();
        sorted_amounts.sort();
        if amounts == sorted_amounts {
            sorting_types.push(InputSortingType::Ascending);
        }

        sorted_amounts.reverse();
        if amounts == sorted_amounts {
            sorting_types.push(InputSortingType::Descending);
        }
    }

    // Check BIP69 sorting.
    // BIP69 sorts by txid as a little-endian uint256 (MSB = wire byte[31] = display byte[0]),
    // which is equivalent to lexicographic comparison of the reversed wire bytes.
    let outpoints: Vec<([u8; 32], u32)> = inputs
        .iter()
        .map(|i| (i.prev_outpoint_txid_bytes(), i.prev_outpoint_vout()))
        .collect();

    let mut sorted_outpoints = outpoints.clone();
    sorted_outpoints.sort_by(|a, b| {
        a.0.iter()
            .rev()
            .cmp(b.0.iter().rev())
            .then_with(|| a.1.cmp(&b.1))
    });

    if outpoints == sorted_outpoints {
        sorting_types.push(InputSortingType::Bip69);
    }

    if sorting_types.is_empty() {
        sorting_types.push(InputSortingType::Unknown);
    }

    sorting_types
}

/// Returns true if any input opts in to nLockTime enforcement (nSequence < 0xFFFFFFFE)
/// but the transaction sets nLockTime = 0.
///
/// This is a wallet fingerprint: the wallet enables nLockTime enforcement via nSequence
/// but doesn't actually use it (e.g. RBF signaling without anti-fee-sniping).
/// No well-implemented wallet does this intentionally.
pub fn nlocktime_optin_without_use(inputs: &[impl HasSequence], locktime: u32) -> bool {
    locktime == 0 && inputs.iter().any(|input| input.sequence() < 0xfffffffe)
}

/// Returns true if any input enables BIP 68 relative timelocks (nSequence < 0x80000000)
/// while the transaction also sets a non-zero absolute nLockTime.
///
/// Consensus-valid but semantically contradictory: wallets doing anti-fee-sniping don't enter BIP 68 range.
/// Strongly fingerprints a specific protocol or wallet composing these fields incorrectly.
pub fn bip68_with_absolute_locktime(inputs: &[impl HasSequence], locktime: u32) -> bool {
    locktime > 0 && inputs.iter().any(|input| input.sequence() < 0x80000000)
}

/// Returns the output structure types detected in the transaction.
pub fn output_structure<O>(outputs: &[O]) -> Vec<OutputStructureType>
where
    O: HasValue + HasScriptPubkey,
{
    if outputs.len() == 1 {
        return vec![OutputStructureType::Single];
    }

    let mut structure = Vec::new();

    if outputs.len() == 2 {
        structure.push(OutputStructureType::Double);
    } else {
        structure.push(OutputStructureType::Multi);
    }

    // Check BIP69 output sorting: sort by (value, scriptPubKey bytes)
    let pairs: Vec<(Amount, Vec<u8>)> = outputs
        .iter()
        .map(|o| (o.value(), o.script_pubkey_bytes()))
        .collect();

    let amounts: Vec<Amount> = pairs.iter().map(|(v, _)| *v).collect();
    let unique_amounts: HashSet<Amount> = amounts.iter().copied().collect();

    let is_bip69 = if unique_amounts.len() != amounts.len() {
        // Duplicate amounts — check both value and scriptPubKey are sorted
        let mut sorted_pairs = pairs.clone();
        sorted_pairs.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
        pairs == sorted_pairs
    } else {
        // Unique amounts — just check amounts are sorted
        amounts.windows(2).all(|w| w[0] <= w[1])
    };

    if is_bip69 {
        structure.push(OutputStructureType::Bip69);
    }

    structure
}

/// Returns true if the transaction fee appears to be a round number,
/// suggesting manual fee entry rather than automatic fee estimation.
///
/// Checks whether the fee (in satoshis) is divisible by 1000.
/// Round satoshi values are common with manual entry,
/// while automatic fee estimation produces non-round amounts.
///
/// Returns `None` if the fee cannot be computed (e.g. input sum < output sum).
pub fn round_fee(input_values: &[impl HasValue], outputs: &[impl HasValue]) -> Option<bool> {
    let input_sum: u64 = input_values.iter().map(|v| v.value().to_sat()).sum();
    let output_sum: u64 = outputs.iter().map(|v| v.value().to_sat()).sum();
    let fee = input_sum.checked_sub(output_sum)?;
    Some(fee > 0 && fee % 1000 == 0)
}
