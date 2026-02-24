use std::collections::HashMap;

use crate::{ScriptPubkeyHash, dense::TxOutId};

/// Embedded key-value database for mapping script pubkey hashes to the first
/// TxOutId that uses them (dense representation).
pub trait ScriptPubkeyDb {
    type Error: std::error::Error;

    /// Returns the first dense TxOutId for the given script pubkey hash, if any.
    fn get(&self, spk_hash: &ScriptPubkeyHash) -> Result<Option<TxOutId>, Self::Error>;

    /// Inserts the first dense TxOutId for the given script pubkey hash, if absent.
    fn insert_if_absent(
        &mut self,
        spk_hash: ScriptPubkeyHash,
        out_id: TxOutId,
    ) -> Result<(), Self::Error>;
}

#[derive(Default)]
pub struct InMemoryScriptPubkeyDb {
    map: HashMap<ScriptPubkeyHash, TxOutId>,
}

impl InMemoryScriptPubkeyDb {
    pub fn new() -> Self {
        Self::default()
    }
}

impl ScriptPubkeyDb for InMemoryScriptPubkeyDb {
    type Error = std::io::Error;

    fn get(&self, spk_hash: &ScriptPubkeyHash) -> Result<Option<TxOutId>, Self::Error> {
        Ok(self.map.get(spk_hash).copied())
    }

    fn insert_if_absent(
        &mut self,
        spk_hash: ScriptPubkeyHash,
        out_id: TxOutId,
    ) -> Result<(), Self::Error> {
        self.map.entry(spk_hash).or_insert(out_id);
        Ok(())
    }
}
