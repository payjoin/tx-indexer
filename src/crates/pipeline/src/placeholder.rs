//! Placeholder support for recursive definitions.
//!
//! Placeholders allow expressing recursive computations in the pipeline DSL.
//! A placeholder is created first, used in expressions, and then unified with
//! the final expression that defines its value.

use std::sync::{Arc, RwLock};

use crate::context::PipelineContext;
use crate::engine::EvalContext;
use crate::expr::Expr;
use crate::node::{Node, NodeId};
use crate::value::ExprValue;

/// A placeholder for recursive definitions.
///
/// Placeholders allow creating cyclic dependencies in the expression graph.
/// The pattern is:
///
/// 1. Create a placeholder: `let p = Placeholder::<Clustering>::new(&ctx)`
/// 2. Use it in expressions: `let mask = IsUnilateral::new(p.as_expr())`
/// 3. Define the actual value: `p.unify(combined_clustering)`
///
/// During evaluation, the engine handles the fixpoint iteration automatically.
///
/// # Example
///
/// ```ignore
/// let global_clustering = Placeholder::<Clustering>::new(&ctx);
/// let unilateral_mask = IsUnilateral::new(global_clustering.as_expr());
/// let change_clustering = ChangeClustering::new(
///     non_coinjoin.filter_with_mask(unilateral_mask)
/// );
/// let combined = change_clustering.join(mih_clustering);
/// global_clustering.unify(combined);
/// ```
pub struct Placeholder<T: ExprValue> {
    /// The expression handle for this placeholder.
    expr: Expr<T>,
    /// The node ID of the expression this placeholder is unified with.
    /// None until `unify()` is called.
    unified_with: Arc<RwLock<Option<NodeId>>>,
}

impl<T: ExprValue> Placeholder<T>
where
    T::Output: Default,
{
    /// Create a new placeholder.
    ///
    /// The placeholder starts uninitialized and must be unified with an
    /// expression before evaluation.
    pub fn new(ctx: &Arc<PipelineContext>) -> Self {
        let unified_with = Arc::new(RwLock::new(None));
        let node = PlaceholderNode::<T> {
            unified_with: Arc::clone(&unified_with),
            _marker: std::marker::PhantomData,
        };
        let expr = ctx.register(node);

        Self { expr, unified_with }
    }

    /// Get the expression handle for this placeholder.
    ///
    /// This can be used in other expressions before `unify()` is called.
    pub fn as_expr(&self) -> Expr<T> {
        self.expr.clone()
    }

    /// Unify this placeholder with an expression.
    ///
    /// After unification, the placeholder will evaluate to the same value
    /// as the target expression. This creates a dependency from the placeholder
    /// to the target.
    ///
    /// # Panics
    ///
    /// Panics if the placeholder has already been unified.
    pub fn unify(&self, target: Expr<T>) {
        let mut unified = self.unified_with.write().expect("lock poisoned");
        if unified.is_some() {
            panic!("Placeholder already unified");
        }
        *unified = Some(target.id());
    }

    /// Check if this placeholder has been unified.
    pub fn is_unified(&self) -> bool {
        self.unified_with.read().expect("lock poisoned").is_some()
    }

    /// Get the target expression ID if unified.
    pub fn unified_target(&self) -> Option<NodeId> {
        *self.unified_with.read().expect("lock poisoned")
    }
}

impl<T: ExprValue> Clone for Placeholder<T> {
    fn clone(&self) -> Self {
        Self {
            expr: self.expr.clone(),
            unified_with: Arc::clone(&self.unified_with),
        }
    }
}

/// Internal node type for placeholders.
struct PlaceholderNode<T: ExprValue> {
    unified_with: Arc<RwLock<Option<NodeId>>>,
    _marker: std::marker::PhantomData<T>,
}

impl<T: ExprValue> Node for PlaceholderNode<T>
where
    T::Output: Default,
{
    type Value = T;

    fn dependencies(&self) -> Vec<NodeId> {
        // If unified, we depend on the target
        match *self.unified_with.read().expect("lock poisoned") {
            Some(id) => vec![id],
            None => vec![],
        }
    }

    fn evaluate(&self, ctx: &EvalContext) -> T::Output {
        // If unified, return the target's value
        // If not unified, return default (for initial fixpoint iteration)
        match *self.unified_with.read().expect("lock poisoned") {
            Some(id) => {
                // We need to get the value from storage directly by ID
                // This is a bit awkward but necessary for type erasure
                ctx.storage
                    .get::<T>(id)
                    .cloned()
                    .unwrap_or_else(T::Output::default)
            }
            None => T::Output::default(),
        }
    }

    fn name(&self) -> &'static str {
        "Placeholder"
    }
}

// Make PlaceholderNode Send + Sync
unsafe impl<T: ExprValue> Send for PlaceholderNode<T> {}
unsafe impl<T: ExprValue> Sync for PlaceholderNode<T> {}
