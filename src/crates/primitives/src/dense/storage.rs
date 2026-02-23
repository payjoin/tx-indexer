use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::Arc,
};

use crate::{
    ScriptPubkeyHash,
    abstract_types::{AbstractTransaction, AbstractTxIn, AbstractTxOut, TxOutIdOps},
    confirmed::ConfirmedTxPtrIndex,
    dense::{BlockFileError, Parser, TxId, TxInId, TxOutId},
    graph_index::{IndexedGraph, PrevOutIndex, ScriptPubkeyIndex, TxInIndex, TxIndex},
};

fn extract_script_pubkey_hash(script: &bitcoin::ScriptBuf) -> ScriptPubkeyHash {
    let script_hash = script.script_hash();

    let mut hash = [0u8; 20];
    hash.copy_from_slice(&script_hash.to_raw_hash()[..]);
    hash
}
