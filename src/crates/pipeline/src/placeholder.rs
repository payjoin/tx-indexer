//! Placeholder support for recursive/cyclic definitions.
//!
//! Placeholders allow expressing recursive computations in the pipeline DSL.
//! A placeholder is created first, used in expressions, and then unified with
//! the final expression that defines its value.
//!
//! # Cyclic Dependencies
//!
//! Placeholders enable expressing cyclic dependencies in the computation graph.
//! For example:
//!
//! ```text
//! global_clustering = mih_clustering.join(change_clustering)
//! change_clustering depends on IsUnilateral(global_clustering)
//! ```
//!
//! The engine handles this by:
//! 1. Initially evaluating the placeholder to its default value
//! 2. Evaluating dependent nodes with this default
//! 3. Re-evaluating the placeholder with the unified target's value
//! 4. Re-evaluating dependents until fixpoint

use std::sync::{Arc, RwLock};

use crate::context::PipelineContext;
use crate::engine::EvalContext;
use crate::expr::Expr;
use crate::node::{Node, NodeId};
use crate::value::ExprValue;

/// A placeholder for recursive/cyclic definitions.
///
/// Placeholders allow creating cyclic dependencies in the expression graph.
/// The pattern is:
///
/// 1. Create a placeholder: `let p = Placeholder::<Clustering>::new(&ctx)`
/// 2. Use it in expressions: `let mask = IsUnilateral::with_clustering(txs, p.as_expr())`
/// 3. Build the expression that defines the placeholder's value
/// 4. Unify: `p.unify(combined_clustering)`
/// 5. Run fixpoint: `engine.run_to_fixpoint()`
///
/// # Example: Cyclic Clustering
///
/// ```ignore
/// // Create placeholder for global clustering
/// let global_clustering = Placeholder::<Clustering>::new(&ctx);
///
/// // Use placeholder in IsUnilateral (creates cycle)
/// let unilateral_mask = IsUnilateral::with_clustering(
///     non_coinjoin.clone(),
///     global_clustering.as_expr()  // Uses placeholder
/// );
///
/// // Build change clustering (depends on unilateral_mask -> global_clustering)
/// let change_clustering = ChangeClustering::new(
///     non_coinjoin.filter_with_mask(unilateral_mask & txs_with_change),
///     change_mask
/// );
///
/// // Combine MIH and change clustering
/// let combined = change_clustering.join(mih_clustering);
///
/// // Close the cycle: global_clustering IS combined
/// global_clustering.unify(combined);
///
/// // Run to fixpoint - iterates until stable
/// engine.run_to_fixpoint();
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
    /// The placeholder starts uninitialized (returns `Default::default()`)
    /// and must be unified with an expression before meaningful evaluation.
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
    /// During initial evaluation, the placeholder returns its default value.
    pub fn as_expr(&self) -> Expr<T> {
        self.expr.clone()
    }

    /// Unify this placeholder with an expression.
    ///
    /// After unification, the placeholder will evaluate to the same value
    /// as the target expression. This creates a dependency from the placeholder
    /// to the target, which may form a cycle.
    ///
    /// The engine's `run_to_fixpoint()` will iterate until the value stabilizes.
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

    /// Get the placeholder's own node ID.
    pub fn id(&self) -> NodeId {
        self.expr.id()
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
///
/// The placeholder node has special evaluation semantics:
/// - Before unification: returns `Default::default()`
/// - After unification: returns the target's current value (may be default during fixpoint)
struct PlaceholderNode<T: ExprValue> {
    unified_with: Arc<RwLock<Option<NodeId>>>,
    _marker: std::marker::PhantomData<T>,
}

impl<T: ExprValue> Node for PlaceholderNode<T>
where
    T::Output: Default,
{
    type OutputValue = T;

    fn dependencies(&self) -> Vec<NodeId> {
        // If unified, we depend on the target.
        // This creates the cycle that fixpoint iteration resolves.
        match *self.unified_with.read().expect("lock poisoned") {
            Some(id) => vec![id],
            None => vec![],
        }
    }

    fn evaluate(&self, ctx: &EvalContext) -> T::Output {
        // If unified, return the target's value.
        // If the target hasn't been evaluated yet (cycle), return default.
        match *self.unified_with.read().expect("lock poisoned") {
            Some(id) => {
                // Get the target's value from storage.
                // During fixpoint iteration, this may return:
                // - Default (if target not yet evaluated)
                // - Previous iteration's value (enabling convergence)
                ctx.storage
                    .get::<T>(id, ctx.node_id)
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

// TODO: Make PlaceholderNode Send + Sync
unsafe impl<T: ExprValue> Send for PlaceholderNode<T> {}
unsafe impl<T: ExprValue> Sync for PlaceholderNode<T> {}
