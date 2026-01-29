//! Segregated storage for node evaluation results.
//!
//! This module provides `NodeStorage`, which maintains a separate storage slot
//! for each node's evaluation result. Results are stored in a type-erased manner
//! but can be retrieved with type safety via the `Expr<T>` handle.

use std::any::Any;
use std::collections::HashMap;

use crate::node::NodeId;
use crate::value::ExprValue;

/// Segregated storage for node evaluation results.
///
/// Each node has its own storage slot for its evaluation result.
/// Results are stored as `Box<dyn Any>` but can be retrieved with
/// type safety using the expression's type parameter.
///
/// # Thread Safety
///
/// `NodeStorage` is not thread-safe by itself. The `Engine` is responsible
/// for ensuring proper synchronization when accessing storage.
pub struct NodeStorage {
    /// Map from NodeId to the stored result (type-erased).
    pub(crate) slots: HashMap<NodeId, Box<dyn Any + Send + Sync>>,
}

impl Default for NodeStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl NodeStorage {
    /// Create a new empty storage.
    pub fn new() -> Self {
        Self {
            slots: HashMap::new(),
        }
    }

    /// Insert a value for a node.
    ///
    /// The value type must match the expression's value type.
    pub fn insert<T: ExprValue>(&mut self, id: NodeId, value: T::Output) {
        self.slots.insert(id, Box::new(value));
    }

    /// Get a reference to a stored value.
    ///
    /// Returns `None` if the node hasn't been evaluated yet or if the
    /// type doesn't match.
    pub fn get<T: ExprValue>(&self, id: NodeId) -> Option<&T::Output> {
        self.slots
            .get(&id)
            .and_then(|boxed| boxed.downcast_ref::<T::Output>())
    }

    /// Get a mutable reference to a stored value.
    ///
    /// Returns `None` if the node hasn't been evaluated yet or if the
    /// type doesn't match.
    pub fn get_mut<T: ExprValue>(&mut self, id: NodeId) -> Option<&mut T::Output> {
        self.slots
            .get_mut(&id)
            .and_then(|boxed| boxed.downcast_mut::<T::Output>())
    }

    /// Check if a value exists for a node.
    pub fn contains(&self, id: NodeId) -> bool {
        self.slots.contains_key(&id)
    }

    /// Remove a value for a node.
    ///
    /// Returns the removed value if it existed and matched the type.
    pub fn remove<T: ExprValue>(&mut self, id: NodeId) -> Option<T::Output> {
        self.slots
            .remove(&id)
            .and_then(|boxed| boxed.downcast::<T::Output>().ok())
            .map(|b| *b)
    }

    /// Remove a value without type checking (for invalidation).
    pub fn remove_any(&mut self, id: NodeId) -> bool {
        self.slots.remove(&id).is_some()
    }

    /// Clear all stored values.
    pub fn clear(&mut self) {
        self.slots.clear();
    }

    /// Get the number of stored values.
    pub fn len(&self) -> usize {
        self.slots.len()
    }

    /// Check if storage is empty.
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    /// Get all stored node IDs.
    pub fn node_ids(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.slots.keys().copied()
    }
}

impl std::fmt::Debug for NodeStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeStorage")
            .field("slot_count", &self.slots.len())
            .finish()
    }
}
