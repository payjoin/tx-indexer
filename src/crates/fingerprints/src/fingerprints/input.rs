use bitcoin::ecdsa::Signature as EcdsaSignature;
use tx_indexer_primitives::{HasScriptSig, HasSequence, HasWitness};

use crate::classify::{
    extract_signatures_from_script_sig, extract_signatures_from_witness, has_low_r_signature,
};

/// Returns true if the input signals RBF (sequence < 0xfffffffe).
pub fn signals_rbf(input: &(impl HasSequence + ?Sized)) -> bool {
    input.sequence() < 0xfffffffe
}

/// Returns true if the input has at least one low-R ECDSA signature.
pub fn low_r_grinding(input: &(impl HasWitness + HasScriptSig + ?Sized)) -> bool {
    let script_sig_bytes = input.script_sig_bytes();
    let witness_items = input.witness_items();

    let mut sigs = Vec::new();
    if !script_sig_bytes.is_empty() {
        sigs.extend(extract_signatures_from_script_sig(&script_sig_bytes));
    }
    if !witness_items.is_empty() {
        sigs.extend(extract_signatures_from_witness(&witness_items));
    }

    sigs.iter().any(|sig_bytes| {
        EcdsaSignature::from_slice(sig_bytes)
            .map(|sig| has_low_r_signature(&sig))
            .unwrap_or(false)
    })
}

/// Bundled trait for input-level fingerprints.
pub trait HasInputFingerprints: HasSequence + HasWitness + HasScriptSig {
    fn signals_rbf(&self) -> bool {
        signals_rbf(self)
    }
    fn low_r_grinding(&self) -> bool {
        low_r_grinding(self)
    }
}

impl<T: HasSequence + HasWitness + HasScriptSig> HasInputFingerprints for T {}
