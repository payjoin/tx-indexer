use crate::ScriptPubkeyHash;

#[allow(unused)]
fn extract_script_pubkey_hash(script: &bitcoin::ScriptBuf) -> ScriptPubkeyHash {
    let script_hash = script.script_hash();

    let mut hash = [0u8; 20];
    hash.copy_from_slice(&script_hash.to_raw_hash()[..]);
    hash
}
