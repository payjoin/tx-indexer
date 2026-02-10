//! Built-in operations for the pipeline DSL.
//!
//! This module provides common operations that can be performed on expressions:
//! - Filtering: `filter_with_mask`
//! - Mask operations: `negate`, bitwise `&`
//! - Set operations: `outputs`, `txs`, `join`
//! - Source operations: `AllTxs`

pub mod bitwise;
pub mod filter;
pub mod negate;
pub mod set_ops;
pub mod source;

// Re-export commonly used items
pub use filter::FilterWithMaskNode;
pub use negate::NegateMaskNode;
pub use set_ops::{JoinClusteringNode, OutputsNode, TxsNode};
pub use source::{AllLooseTxs, AllLooseTxsNode};
