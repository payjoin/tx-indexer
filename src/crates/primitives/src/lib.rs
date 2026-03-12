pub mod dense;
pub mod handle;
pub mod indecies;
pub mod loose;
pub mod parser;
pub mod sled;
pub mod test_utils;
pub mod traits;
pub mod unified;

#[cfg(any(test, feature = "test-utils"))]
pub mod integration;

#[cfg(test)]
mod tests;

pub use traits::abstract_types::{
    AbstractTransaction, AbstractTxIn, AbstractTxOut, HasScriptPubkey, HasSequence, HasVersion,
    HasWitnessData,
};
pub use unified::{AnyInId, AnyOutId, AnyTxId, UnifiedStorage, UnifiedStorageBuilder};

pub type ScriptPubkeyHash = [u8; 20];
