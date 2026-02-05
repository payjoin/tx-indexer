//! Node trait and related types for the pipeline DSL.
//!
//! This module defines the core `Node` trait that all expression nodes must implement,
//! along with the `NodeId` type for identifying nodes and the `AnyNode` trait for
//! type-erased storage.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use crate::engine::EvalContext;
use crate::value::ExprValue;

/// Unique identifier for a node in the expression graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId(pub(crate) u64);

impl NodeId {
    /// Create a new NodeId from a raw value.
    pub fn from_raw(value: u64) -> Self {
        Self(value)
    }

    /// Get the raw value of this NodeId.
    pub fn raw(&self) -> u64 {
        self.0
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Node({})", self.0)
    }
}

/// Core trait for expression nodes.
///
/// Any struct can become an expression node by implementing this trait.
/// This is the primary extension point for adding new operations to the DSL.
///
/// # Example
///
/// ```ignore
/// pub struct MyCustomNode {
///     input: Expr<TxSet>,
/// }
///
/// impl Node for MyCustomNode {
///     type Value = Clustering;
///
///     fn dependencies(&self) -> Vec<NodeId> {
///         vec![self.input.id()]
///     }
///
///     fn evaluate(&self, ctx: &EvalContext) -> SparseDisjointSet<TxOutId> {
///         let input_txs = ctx.get(&self.input);
///         // ... compute result
///     }
/// }
/// ```
pub trait Node: Send + Sync + 'static {
    /// The value type this node produces when evaluated.
    type OutputValue: ExprValue;

    /// Return the IDs of nodes this node depends on.
    ///
    /// The engine uses this to determine evaluation order and ensure
    /// dependencies are evaluated before this node.
    fn dependencies(&self) -> Vec<NodeId>;

    /// Evaluate this node given access to dependency results.
    ///
    /// The `EvalContext` provides access to:
    /// - Results of dependency nodes via `ctx.get(&expr)`
    /// - The underlying index via `ctx.index()`
    fn evaluate(&self, ctx: &EvalContext) -> <Self::OutputValue as ExprValue>::Output;

    /// Optional: provide a human-readable name for debugging.
    fn name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }
}

/// Type-erased wrapper for nodes, allowing storage of heterogeneous nodes.
///
/// This trait is automatically implemented for all `Node` types and provides
/// methods for type-erased evaluation and dependency inspection.
pub trait AnyNode: Send + Sync + 'static {
    /// Get the dependencies of this node.
    fn dependencies(&self) -> Vec<NodeId>;

    /// Evaluate this node and return the result plus whether it changed from `previous`.
    /// The node does a single typed downcast of `previous` to compare; the engine does no casting.
    fn evaluate_any(
        &self,
        ctx: &EvalContext,
        previous: Option<&(dyn Any + Send + Sync)>,
    ) -> (Box<dyn Any + Send + Sync>, bool);

    /// Get the name of this node for debugging.
    fn name(&self) -> &'static str;
}

impl<N: Node> AnyNode for N
where
    <N::OutputValue as ExprValue>::Output: Send + Sync,
{
    fn dependencies(&self) -> Vec<NodeId> {
        Node::dependencies(self)
    }

    fn evaluate_any(
        &self,
        ctx: &EvalContext,
        other: Option<&(dyn Any + Send + Sync)>,
    ) -> (Box<dyn Any + Send + Sync>, bool) {
        let out = self.evaluate(ctx);
        let changed = other
            .and_then(|p| p.downcast_ref::<<N::OutputValue as ExprValue>::Output>())
            .map_or(true, |prev| prev != &out);
        (Box::new(out), changed)
    }

    fn name(&self) -> &'static str {
        Node::name(self)
    }
}

/// A shared reference to a type-erased node.
pub type SharedNode = Arc<dyn AnyNode>;
