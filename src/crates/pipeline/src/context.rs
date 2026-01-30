//! Pipeline context for managing the expression graph.
//!
//! The `PipelineContext` is the central registry for all nodes in a pipeline.
//! It allocates node IDs and stores the type-erased nodes.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use crate::expr::Expr;
use crate::node::{Node, NodeId, SharedNode};

/// Central registry for the expression graph.
///
/// The `PipelineContext` manages:
/// - Node ID allocation
/// - Storage of type-erased nodes
/// - Node lookup by ID
///
/// It is typically wrapped in an `Arc` for sharing between expressions.
///
/// # Example
///
/// ```ignore
/// let ctx = Arc::new(PipelineContext::new());
/// let txs = AllTxs::new(&ctx);  // Registers a node and returns Expr<TxSet>
/// ```
pub struct PipelineContext {
    /// Map from NodeId to the type-erased node.
    nodes: RwLock<HashMap<NodeId, SharedNode>>,
    /// Counter for generating unique node IDs.
    next_id: AtomicU64,
}

impl Default for PipelineContext {
    fn default() -> Self {
        Self::new()
    }
}

impl PipelineContext {
    /// Create a new empty pipeline context.
    pub fn new() -> Self {
        Self {
            nodes: RwLock::new(HashMap::new()),
            next_id: AtomicU64::new(0),
        }
    }

    /// Register a new node and return a typed expression handle.
    ///
    /// This is the primary way to add nodes to the expression graph.
    /// The returned `Expr<N::Value>` is a typed handle that can be used
    /// in further operations.
    ///
    /// # Example
    ///
    /// ```ignore
    /// struct MyNode { ... }
    /// impl Node for MyNode { type Value = TxSet; ... }
    ///
    /// let expr: Expr<TxSet> = ctx.register(MyNode { ... });
    /// ```
    pub fn register<N: Node>(self: &Arc<Self>, node: N) -> Expr<N::OutputValue> {
        let id = NodeId(self.next_id.fetch_add(1, Ordering::SeqCst));

        {
            let mut nodes = self.nodes.write().expect("lock poisoned");
            nodes.insert(id, Arc::new(node));
        }

        Expr::new(id, Arc::clone(self))
    }

    /// Get a node by ID.
    ///
    /// Returns `None` if the node doesn't exist.
    pub fn get_node(&self, id: NodeId) -> Option<SharedNode> {
        let nodes = self.nodes.read().expect("lock poisoned");
        nodes.get(&id).cloned()
    }

    /// Get all node IDs in the graph.
    pub fn all_node_ids(&self) -> Vec<NodeId> {
        let nodes = self.nodes.read().expect("lock poisoned");
        nodes.keys().copied().collect()
    }

    /// Get the number of nodes in the graph.
    pub fn node_count(&self) -> usize {
        let nodes = self.nodes.read().expect("lock poisoned");
        nodes.len()
    }

    /// Check if a node exists.
    pub fn contains(&self, id: NodeId) -> bool {
        let nodes = self.nodes.read().expect("lock poisoned");
        nodes.contains_key(&id)
    }
}

impl std::fmt::Debug for PipelineContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let nodes = self.nodes.read().expect("lock poisoned");
        f.debug_struct("PipelineContext")
            .field("node_count", &nodes.len())
            .finish()
    }
}
