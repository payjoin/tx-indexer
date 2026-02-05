//! Lazy evaluation engine for the pipeline DSL.
//!
//! The `Engine` is responsible for:
//! - Lazy evaluation of expressions
//! - Caching results in segregated storage
//! - Dependency resolution and topological ordering
//! - Fixpoint iteration for recursive/cyclic definitions

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tx_indexer_primitives::loose::storage::InMemoryIndex;

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
    /// Node id of the node being evaluated.
    pub(crate) node_id: NodeId,
}

impl<'a> EvalContext<'a> {
    /// Create a new evaluation context.
    pub fn new(storage: &'a NodeStorage, index: &'a InMemoryIndex, node_id: NodeId) -> Self {
        Self {
            storage,
            index,
            node_id,
        }
    }

    /// Get the result of a dependency expression.
    ///
    /// # Panics
    ///
    /// Panics if the dependency hasn't been evaluated yet. The engine
    /// ensures dependencies are evaluated before their dependents.
    pub fn get<T: ExprValue>(&self, expr: &Expr<T>) -> &T::Output {
        self.storage
            .get::<T>(expr.id(), self.node_id)
            .unwrap_or_else(|| {
                // TODO: why is this panicking?
                panic!(
                    "Dependency {} not evaluated - this is a bug in the engine",
                    expr.id()
                )
            })
    }

    pub fn index(&self) -> &InMemoryIndex {
        self.index
    }

    /// Get a dependency result or a default value if not yet evaluated.
    /// This is useful for nodes that may be part of a cycle.
    pub fn get_or_default<T: ExprValue>(&self, expr: &Expr<T>) -> T::Output
    where
        T::Output: Default,
    {
        self.storage
            .get::<T>(expr.id(), self.node_id)
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
    /// Track which iteration each node was last evaluated in (for cycle detection).
    eval_iteration: HashMap<NodeId, usize>,
}

impl Engine {
    /// Create a new engine.
    pub fn new(ctx: Arc<PipelineContext>, index: Arc<InMemoryIndex>) -> Self {
        Self {
            ctx,
            index,
            storage: NodeStorage::new(),
            eval_iteration: HashMap::new(),
        }
    }

    pub fn evaluated_facts<T: ExprValue>(&mut self, expr: &Expr<T>) -> Vec<&T::Output> {
        self.storage
            .non_volatile_get::<T>(expr.id())
            .unwrap_or_default()
    }

    /// Evaluate an expression to a single combined value.
    ///
    /// Fetches all facts for the expression from storage and combines them
    /// using the value type's `combine_facts`. Returns `Default::default()`
    /// if no facts have been produced (e.g. pipeline not yet run).
    pub fn eval<T: ExprValue>(&mut self, expr: &Expr<T>) -> T::Output
    where
        T::Output: Default,
    {
        self.run_to_fixpoint();
        let facts = self
            .storage
            .non_volatile_get::<T>(expr.id())
            .unwrap_or_default();
        if facts.is_empty() {
            return Default::default();
        }
        T::combine_facts(&facts)
    }

    /// Get the pipeline context.
    pub fn context(&self) -> &Arc<PipelineContext> {
        &self.ctx
    }

    /// Get the underlying index.
    pub fn index(&self) -> &Arc<InMemoryIndex> {
        &self.index
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
    /// Returns true if the node was evaluated and its value changed (or first eval).
    ///
    /// Re-evaluation only happens when there is new input: either first time, or at least
    /// one dependency has produced more output since we last read. This prevents infinite
    /// loops where source nodes re-append every iteration and downstream always sees "new" facts.
    fn evaluate_node_for_fixpoint(&mut self, id: NodeId, iteration: usize) -> bool {
        let node = match self.ctx.get_node(id) {
            Some(n) => n,
            None => return false, // TODO: panic? This points to a bug
        };

        let deps = node.dependencies();
        let is_first_eval = !self.storage.contains(id);
        let new_deps_facts = deps.iter().any(|dep_id| {
            self.storage.slot_count(*dep_id) > self.storage.last_read_index(id, *dep_id)
        });

        let last_iter = self.eval_iteration.get(&id).copied().unwrap_or(0);
        let already_ran_this_iteration = last_iter == iteration;
        let has_new_input = is_first_eval || new_deps_facts;

        // Skip if we already ran this iteration, or there is no new input to consume
        if already_ran_this_iteration || !has_new_input {
            return false;
        }

        let eval_ctx = EvalContext::new(&self.storage, &self.index, id);
        let previous = self.storage.get_last(id);
        let (result, changed) = node.evaluate_any(&eval_ctx, previous);

        self.storage.append(id, result);
        self.eval_iteration.insert(id, iteration);

        changed
    }
}

impl std::fmt::Debug for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Engine")
            .field("context", &self.ctx)
            .field("storage", &self.storage)
            .finish()
    }
}
