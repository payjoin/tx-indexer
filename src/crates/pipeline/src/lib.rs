//! Typed AST-based DSL for transaction analysis pipelines.
//!
//! This crate provides a lazy, type-safe embedded DSL for expressing Bitcoin transaction
//! analysis pipelines. The design uses trait-based extensibility, allowing new node types
//! to be defined without modifying the core infrastructure.
//!
//! # Example
//!
//! ```ignore
//! use pipeline::{PipelineContext, Engine, Expr, Placeholder};
//! use pipeline::value::Clustering;
//!
//! let ctx = PipelineContext::new();
//! let all_txs = AllTxs::new(&ctx);
//!
//! let is_coinjoin_mask = IsCoinJoin::new(all_txs.clone());
//! let non_coinjoin = all_txs.filter_with_mask(is_coinjoin_mask.negate());
//!
//! // Lazy evaluation - nothing runs until eval() is called
//! let mut engine = Engine::new(&ctx, index);
//! engine.run_to_fixpoint();
//! ```

pub mod context;
pub mod engine;
pub mod expr;
pub mod node;
pub mod ops;
pub mod placeholder;
pub mod storage;
pub mod value;

// Re-export main types for convenience
pub use context::PipelineContext;
pub use engine::{Engine, EvalContext};
pub use expr::Expr;
pub use node::{AnyNode, Node, NodeId};
pub use placeholder::Placeholder;
pub use storage::NodeStorage;
pub use value::{Clustering, ExprValue, Mask, TxSet};
