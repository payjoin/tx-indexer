use bitcoin::Script;
use bitcoin::script::Instruction;
use tx_indexer_primitives::{HasScriptPubkey, HasWitnessData};

use crate::classify::classify_script_pubkey;
use crate::types::OutputType;

/// Classify the input type by looking at its prevout's scriptPubKey.
pub fn input_type(prevout: &impl HasScriptPubkey) -> OutputType {
    classify_script_pubkey(&prevout.script_pubkey_bytes())
}

/// Returns true if the input uses an uncompressed public key.
///
/// Covers two cases:
/// - P2PK: the uncompressed pubkey is embedded directly in the prevout scriptPubKey.
/// - P2PKH: the uncompressed pubkey is pushed in the spending script_sig.
pub fn has_uncompressed_pubkey(
    input: &(impl HasWitnessData + ?Sized),
    prevout: &(impl HasScriptPubkey + ?Sized),
) -> bool {
    let spk_bytes = prevout.script_pubkey_bytes();
    let script = Script::from_bytes(&spk_bytes);

    // P2PK: pubkey is in the prevout
    if let Some(pk) = script.p2pk_public_key()
        && !pk.compressed
    {
        return true;
    }

    // P2PKH: pubkey is pushed in script_sig — look for a 65-byte push starting with 0x04
    if script.is_p2pkh() {
        let sig_bytes = input.script_sig_bytes();
        let sig_script = Script::from_bytes(&sig_bytes);
        return sig_script
            .instructions()
            .filter_map(|instr| match instr {
                Ok(Instruction::PushBytes(bytes)) => Some(bytes.as_bytes().to_vec()),
                _ => None,
            })
            .any(|data| data.len() == 65 && data[0] == 0x04);
    }

    false
}

/// Returns true if the input is a taproot key spend with a non-default sighash type.
///
/// A taproot key-spend witness has exactly one item (the signature), or two items
/// where the second starts with 0x50 (an annex). A 65-byte signature means an
/// explicit sighash type was encoded instead of the compact default form.
pub fn taproot_keyspend_non_default_sighash(
    input: &(impl HasWitnessData + ?Sized),
    prevout: &(impl HasScriptPubkey + ?Sized),
) -> bool {
    let spk_bytes = prevout.script_pubkey_bytes();
    let script = Script::from_bytes(&spk_bytes);

    if !script.is_p2tr() {
        return false;
    }

    let items = input.witness_items();

    let sig = match items.len() {
        1 => &items[0],
        2 if items[1].first() == Some(&0x50) => &items[0],
        _ => return false,
    };

    sig.len() == 65
}

pub trait HasInputWithPrevoutFingerprints: HasWitnessData + HasScriptPubkey {
    fn has_uncompressed_pubkey(&self, prevout: &impl HasScriptPubkey) -> bool {
        has_uncompressed_pubkey(self, prevout)
    }
    fn taproot_keyspend_non_default_sighash(&self, prevout: &impl HasScriptPubkey) -> bool {
        taproot_keyspend_non_default_sighash(self, prevout)
    }
}
