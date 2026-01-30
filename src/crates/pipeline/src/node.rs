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

    /// Evaluate this node and return the result as a boxed Any.
    fn evaluate_any(&self, ctx: &EvalContext) -> Box<dyn Any + Send + Sync>;

    /// Get the TypeId of the value this node produces.
    fn value_type_id(&self) -> std::any::TypeId;

    /// Get the name of this node for debugging.
    fn name(&self) -> &'static str;

    /// Downcast to a concrete node type (for debugging/introspection).
    fn as_any(&self) -> &dyn Any;
}

impl<N: Node> AnyNode for N {
    fn dependencies(&self) -> Vec<NodeId> {
        Node::dependencies(self)
    }

    fn evaluate_any(&self, ctx: &EvalContext) -> Box<dyn Any + Send + Sync> {
        Box::new(self.evaluate(ctx))
    }

    fn value_type_id(&self) -> std::any::TypeId {
        std::any::TypeId::of::<<N::OutputValue as ExprValue>::Output>()
    }

    fn name(&self) -> &'static str {
        Node::name(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// A shared reference to a type-erased node.
pub type SharedNode = Arc<dyn AnyNode>;
