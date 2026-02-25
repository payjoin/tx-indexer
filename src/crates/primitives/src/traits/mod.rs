pub mod abstract_fingerprints;
pub mod abstract_types;
pub mod graph_index;

pub use abstract_fingerprints::HasNLockTime;

use crate::ScriptPubkeyHash;
use crate::dense::TxOutId;

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
