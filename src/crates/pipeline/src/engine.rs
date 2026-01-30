//! Lazy evaluation engine for the pipeline DSL.
//!
//! The `Engine` is responsible for:
//! - Lazy evaluation of expressions
//! - Caching results in segregated storage
//! - Dependency resolution and topological ordering
//! - Fixpoint iteration for recursive/cyclic definitions

use std::collections::{HashMap, HashSet};
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

    /// Get a dependency result or a default value if not yet evaluated.
    /// This is useful for nodes that may be part of a cycle.
    pub fn get_or_default<T: ExprValue>(&self, expr: &Expr<T>) -> T::Output
    where
        T::Output: Default,
    {
        self.storage
            .get::<T>(expr.id())
            .cloned()
            .unwrap_or_default()
    }
}

/// Lazy evaluation engine for the pipeline.
///
/// The engine evaluates expressions on demand, caching results and
/// respecting dependencies between nodes. It supports cyclic dependencies
/// through fixpoint iteration.
pub struct Engine {
    ctx: Arc<PipelineContext>,
    index: Arc<InMemoryIndex>,
    storage: NodeStorage,
    /// Nodes that need re-evaluation (dirty tracking).
    dirty: HashSet<NodeId>,
    /// Track which iteration each node was last evaluated in (for cycle detection).
    eval_iteration: HashMap<NodeId, usize>,
    /// Nodes currently being evaluated (for cycle detection during single evaluation).
    evaluating: HashSet<NodeId>,
}

impl Engine {
    /// Create a new engine.
    pub fn new(ctx: Arc<PipelineContext>, index: Arc<InMemoryIndex>) -> Self {
        Self {
            ctx,
            index,
            storage: NodeStorage::new(),
            dirty: HashSet::new(),
            eval_iteration: HashMap::new(),
            evaluating: HashSet::new(),
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
    /// Handles cycles by using cached/default values for nodes currently being evaluated.
    fn ensure_evaluated(&mut self, id: NodeId) {
        // If already evaluated and not dirty, nothing to do
        if self.storage.contains(id) && !self.dirty.contains(&id) {
            return;
        }

        // Cycle detection: if we're already evaluating this node, skip
        // (the node will use cached/default value from storage)
        if self.evaluating.contains(&id) {
            return;
        }

        // Mark as currently evaluating
        self.evaluating.insert(id);

        // Get the node
        let node = self.ctx.get_node(id).expect("Node not found");

        // First, ensure all dependencies are evaluated
        // (cycles will be detected and skipped)
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

        // Done evaluating this node
        self.evaluating.remove(&id);
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

    /// Run the pipeline to fixpoint for recursive/cyclic definitions.
    ///
    /// This implements semi-naive evaluation:
    /// 1. Topologically sort nodes (with cycle detection)
    /// 2. Evaluate nodes in order, using defaults for back-edges
    /// 3. Re-evaluate until values stabilize
    ///
    /// Returns the number of iterations performed.
    pub fn run_to_fixpoint(&mut self) -> usize {
        // TODO: make this configurable
        // TODO: if the
        let max_iterations = 100;
        let mut iteration = 0;

        // Get all nodes
        let all_ids: Vec<_> = self.ctx.all_node_ids();

        // Sort nodes topologically (best effort - cycles will be handled)
        let sorted_ids = self.topological_sort(&all_ids);

        // Fixpoint iteration
        loop {
            iteration += 1;
            if iteration > max_iterations {
                panic!(
                    "Fixpoint iteration exceeded maximum ({} iterations)",
                    max_iterations
                );
            }

            let mut any_changed = false;

            // Evaluate all nodes in topological order
            for &id in &sorted_ids {
                let changed = self.evaluate_node_for_fixpoint(id, iteration);
                if changed {
                    any_changed = true;
                }
            }

            // If nothing changed, we've reached fixpoint
            if !any_changed {
                break;
            }

            // Mark all nodes as needing potential re-evaluation
            for &id in &all_ids {
                self.dirty.insert(id);
            }
        }

        iteration
    }

    /// Topologically sort nodes (best effort with cycles).
    fn topological_sort(&self, nodes: &[NodeId]) -> Vec<NodeId> {
        let mut result = Vec::new();
        let mut visited = HashSet::new();
        let mut in_stack = HashSet::new();

        fn visit(
            id: NodeId,
            ctx: &PipelineContext,
            visited: &mut HashSet<NodeId>,
            in_stack: &mut HashSet<NodeId>,
            result: &mut Vec<NodeId>,
        ) {
            if visited.contains(&id) {
                return;
            }
            if in_stack.contains(&id) {
                // Cycle detected - skip
                return;
            }

            in_stack.insert(id);

            if let Some(node) = ctx.get_node(id) {
                for dep_id in node.dependencies() {
                    visit(dep_id, ctx, visited, in_stack, result);
                }
            }

            in_stack.remove(&id);
            visited.insert(id);
            result.push(id);
        }

        for &id in nodes {
            visit(id, &self.ctx, &mut visited, &mut in_stack, &mut result);
        }

        result
    }

    /// Evaluate a node for fixpoint iteration.
    /// Returns true if the value changed (or was first evaluated).
    fn evaluate_node_for_fixpoint(&mut self, id: NodeId, iteration: usize) -> bool {
        let node = match self.ctx.get_node(id) {
            Some(n) => n,
            None => return false,
        };

        // Check if all dependencies have been evaluated at least once
        // If not, we'll try to evaluate anyway - nodes should handle missing deps
        let deps = node.dependencies();
        let all_deps_available = deps.iter().all(|dep_id| self.storage.contains(*dep_id));

        // If we've already evaluated this iteration and all deps are available, skip
        let last_iter = self.eval_iteration.get(&id).copied().unwrap_or(0);
        if last_iter == iteration && all_deps_available {
            return false;
        }

        // Evaluate this node
        let eval_ctx = EvalContext::new(&self.storage, &self.index);
        let result = node.evaluate_any(&eval_ctx);

        // Check if this is a new value
        let is_first_eval = !self.storage.contains(id);

        self.storage.slots.insert(id, result);
        self.eval_iteration.insert(id, iteration);
        self.dirty.remove(&id);

        // Consider it changed if it's the first eval or deps weren't all available
        is_first_eval || !all_deps_available
    }

    /// Clear all cached results.
    pub fn clear_cache(&mut self) {
        self.storage.clear();
        self.dirty.clear();
        self.eval_iteration.clear();
        self.evaluating.clear();
    }

    /// Get direct access to storage
    pub fn storage(&self) -> &NodeStorage {
        &self.storage
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
