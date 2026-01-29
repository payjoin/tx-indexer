//! Lazy evaluation engine for the pipeline DSL.
//!
//! The `Engine` is responsible for:
//! - Lazy evaluation of expressions
//! - Caching results in segregated storage
//! - Dependency resolution and topological ordering
//! - Fixpoint iteration for recursive definitions

use std::collections::HashSet;
use std::sync::Arc;

use tx_indexer_primitives::storage::InMemoryIndex;

use crate::context::PipelineContext;
use crate::expr::Expr;
use crate::node::NodeId;
use crate::storage::NodeStorage;
use crate::value::ExprValue;

/// Context passed to nodes during evaluation.
///
/// Provides access to:
/// - Results of dependency nodes
/// - The underlying transaction index
pub struct EvalContext<'a> {
    pub(crate) storage: &'a NodeStorage,
    index: &'a InMemoryIndex,
}

impl<'a> EvalContext<'a> {
    /// Create a new evaluation context.
    pub fn new(storage: &'a NodeStorage, index: &'a InMemoryIndex) -> Self {
        Self { storage, index }
    }

    /// Get the result of a dependency expression.
    ///
    /// # Panics
    ///
    /// Panics if the dependency hasn't been evaluated yet. The engine
    /// ensures dependencies are evaluated before their dependents.
    pub fn get<T: ExprValue>(&self, expr: &Expr<T>) -> &T::Output {
        self.storage.get::<T>(expr.id()).unwrap_or_else(|| {
            panic!(
                "Dependency {} not evaluated - this is a bug in the engine",
                expr.id()
            )
        })
    }

    /// Get the underlying transaction index.
    pub fn index(&self) -> &InMemoryIndex {
        self.index
    }

    /// Try to get a dependency result, returning None if not yet evaluated.
    pub fn try_get<T: ExprValue>(&self, expr: &Expr<T>) -> Option<&T::Output> {
        self.storage.get::<T>(expr.id())
    }
}

/// Lazy evaluation engine for the pipeline.
///
/// The engine evaluates expressions on demand, caching results and
/// respecting dependencies between nodes.
pub struct Engine {
    ctx: Arc<PipelineContext>,
    index: Arc<InMemoryIndex>,
    storage: NodeStorage,
    /// Nodes that need re-evaluation (dirty tracking).
    dirty: HashSet<NodeId>,
}

impl Engine {
    /// Create a new engine.
    pub fn new(ctx: Arc<PipelineContext>, index: Arc<InMemoryIndex>) -> Self {
        Self {
            ctx,
            index,
            storage: NodeStorage::new(),
            dirty: HashSet::new(),
        }
    }

    /// Get the pipeline context.
    pub fn context(&self) -> &Arc<PipelineContext> {
        &self.ctx
    }

    /// Get the underlying index.
    pub fn index(&self) -> &Arc<InMemoryIndex> {
        &self.index
    }

    /// Lazily evaluate an expression, returning a reference to the result.
    ///
    /// If the expression has already been evaluated (and isn't dirty),
    /// returns the cached result. Otherwise, evaluates the expression
    /// and all its dependencies first.
    pub fn eval<T: ExprValue>(&mut self, expr: &Expr<T>) -> &T::Output {
        self.ensure_evaluated(expr.id());
        self.storage.get::<T>(expr.id()).unwrap()
    }

    /// Ensure a node and all its dependencies are evaluated.
    fn ensure_evaluated(&mut self, id: NodeId) {
        // If already evaluated and not dirty, nothing to do
        if self.storage.contains(id) && !self.dirty.contains(&id) {
            return;
        }

        // Get the node
        let node = self.ctx.get_node(id).expect("Node not found");

        // First, ensure all dependencies are evaluated
        let deps = node.dependencies();
        for dep_id in deps {
            self.ensure_evaluated(dep_id);
        }

        // Now evaluate this node
        let eval_ctx = EvalContext::new(&self.storage, &self.index);
        let result = node.evaluate_any(&eval_ctx);

        // Store the result
        self.storage.slots.insert(id, result);
        self.dirty.remove(&id);
    }

    /// Mark a node as dirty (needs re-evaluation).
    ///
    /// Also marks all nodes that depend on this node as dirty (transitively).
    pub fn invalidate(&mut self, id: NodeId) {
        self.dirty.insert(id);
        self.storage.remove_any(id);

        // Find all nodes that depend on this one (reverse dependencies)
        // and mark them dirty too
        let all_ids: Vec<_> = self.ctx.all_node_ids();
        for other_id in all_ids {
            if let Some(node) = self.ctx.get_node(other_id) {
                if node.dependencies().contains(&id) {
                    self.invalidate(other_id);
                }
            }
        }
    }

    /// Run the pipeline to fixpoint for recursive definitions.
    ///
    /// This iterates until no more changes occur, which handles
    /// recursive definitions created via `Placeholder::unify`.
    ///
    /// Returns the number of iterations performed.
    pub fn run_to_fixpoint(&mut self) -> usize {
        let mut iterations = 0;
        let max_iterations = 1000; // Prevent infinite loops

        loop {
            iterations += 1;
            if iterations > max_iterations {
                panic!("Fixpoint iteration exceeded maximum ({} iterations)", max_iterations);
            }

            // Evaluate all nodes
            let all_ids: Vec<_> = self.ctx.all_node_ids();
            let mut changed = false;

            for id in all_ids {
                // Check if this is a placeholder that's been unified
                if let Some(node) = self.ctx.get_node(id) {
                    // Store old value for comparison
                    let had_value = self.storage.contains(id);

                    // Re-evaluate if dirty or not yet evaluated
                    if self.dirty.contains(&id) || !had_value {
                        let deps = node.dependencies();
                        for dep_id in deps {
                            self.ensure_evaluated(dep_id);
                        }

                        let eval_ctx = EvalContext::new(&self.storage, &self.index);
                        let result = node.evaluate_any(&eval_ctx);

                        // Check if value changed (for fixpoint detection)
                        // This is a simple check - in practice you might want
                        // a more sophisticated comparison
                        if !had_value {
                            changed = true;
                        }

                        self.storage.slots.insert(id, result);
                        self.dirty.remove(&id);
                    }
                }
            }

            // If nothing changed, we've reached fixpoint
            if !changed && self.dirty.is_empty() {
                break;
            }
        }

        iterations
    }

    /// Clear all cached results.
    pub fn clear_cache(&mut self) {
        self.storage.clear();
        self.dirty.clear();
    }

    /// Get direct access to storage (for advanced use cases).
    pub fn storage(&self) -> &NodeStorage {
        &self.storage
    }

    /// Get mutable access to storage (for advanced use cases).
    pub fn storage_mut(&mut self) -> &mut NodeStorage {
        &mut self.storage
    }
}


impl std::fmt::Debug for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Engine")
            .field("context", &self.ctx)
            .field("storage", &self.storage)
            .field("dirty_count", &self.dirty.len())
            .finish()
    }
}
