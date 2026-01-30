//! Typed expression handles for the pipeline DSL.
//!
//! This module defines `Expr<T>`, a type-safe handle to a node in the expression graph.
//! The type parameter `T` indicates what kind of value the expression produces.

use std::marker::PhantomData;
use std::sync::Arc;

use crate::context::PipelineContext;
use crate::node::NodeId;
use crate::value::ExprValue;

/// A typed handle to a node in the expression graph.
///
/// `Expr<T>` is the primary type users interact with when building pipelines.
/// The type parameter `T` is a marker type (implementing `ExprValue`) that
/// indicates what kind of value this expression produces when evaluated.
///
/// # Cloning
///
/// `Expr` is cheap to clone - it only contains a `NodeId` and an `Arc` to the context.
/// Cloning does not duplicate the underlying computation.
///
/// # Example
///
/// ```ignore
/// let txs: Expr<TxSet> = AllTxs::new(&ctx);
/// let mask: Expr<Mask<TxId>> = IsCoinJoin::new(txs.clone());
/// let not_conjoins: Expr<TxSet> = txs.filter_with_mask(mask.negate());
/// ```
pub struct Expr<T: ExprValue> {
    pub(crate) id: NodeId,
    pub(crate) ctx: Arc<PipelineContext>,
    pub(crate) _marker: PhantomData<T>,
}

impl<T: ExprValue> Expr<T> {
    /// Create a new expression handle.
    ///
    /// This is typically called by `PipelineContext::register`, not directly by users.
    pub(crate) fn new(id: NodeId, ctx: Arc<PipelineContext>) -> Self {
        Self {
            id,
            ctx,
            _marker: PhantomData,
        }
    }

    /// Get the NodeId of this expression.
    pub fn id(&self) -> NodeId {
        self.id
    }

    /// Get a reference to the pipeline context.
    pub fn context(&self) -> &Arc<PipelineContext> {
        &self.ctx
    }
}

// Cheap clone - only clones the Arc and NodeId
impl<T: ExprValue> Clone for Expr<T> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            ctx: Arc::clone(&self.ctx),
            _marker: PhantomData,
        }
    }
}

impl<T: ExprValue> std::fmt::Debug for Expr<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Expr")
            .field("id", &self.id)
            .field("type", &std::any::type_name::<T>())
            .finish()
    }
}

// Two expressions are equal if they refer to the same node
impl<T: ExprValue> PartialEq for Expr<T> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<T: ExprValue> Eq for Expr<T> {}

impl<T: ExprValue> std::hash::Hash for Expr<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}
