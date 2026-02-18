//! Segregated storage for node evaluation results.
//!
//! This module provides `NodeStorage`, which maintains a separate storage slot
//! for each node's evaluation result. Results are stored in a type-erased manner
//! but can be retrieved with type safety via the `Expr<T>` handle.

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::node::NodeId;
use crate::value::ExprValue;

pub struct BaseFacts<T: ?Sized> {
    facts: Option<Vec<Arc<T>>>,
}

impl<T: ?Sized> BaseFacts<T> {
    pub fn new() -> Self {
        Self { facts: None }
    }
    pub fn set_base_facts(&mut self, facts: impl IntoIterator<Item = Arc<T>>) {
        self.facts = Some(facts.into_iter().collect());
    }
    pub fn take_base_facts(&mut self) -> Option<Vec<Arc<T>>> {
        self.facts.take()
    }
}

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
    /// Map from NodeId to the stored results (type-erased).
    pub(crate) slots: HashMap<NodeId, Vec<Box<dyn Any + Send + Sync>>>,
    cursor: RwLock<HashMap<(NodeId, NodeId), usize>>,
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
            cursor: RwLock::new(HashMap::new()),
        }
    }
    /// Append a value for a node.
    pub fn append(&mut self, id: NodeId, value: Box<dyn Any + Send + Sync>) {
        self.slots.entry(id).or_insert(vec![]).push(value);
    }

    /// Reference to the last stored value for a node (for fixpoint no-progress check).
    pub fn get_last(&self, id: NodeId) -> Option<&(dyn Any + Send + Sync)> {
        self.slots
            .get(&id)
            .and_then(|v| v.last())
            .map(|b| b.as_ref())
    }

    pub fn non_volatile_get<T: ExprValue>(&self, id: NodeId) -> Option<Vec<&T::Output>> {
        self.slots.get(&id).map(|slot_vec| {
            slot_vec
                .iter()
                .map(|boxed| {
                    boxed
                        .as_ref()
                        .downcast_ref::<T::Output>()
                        .expect("correct type")
                })
                .collect::<Vec<&T::Output>>()
        })
    }

    /// Get a reference to a stored value.
    ///
    /// Returns `None` if the node hasn't been evaluated yet or if the
    /// type doesn't match.
    pub fn get<T: ExprValue>(&self, producer: NodeId, dependent: NodeId) -> Option<&T::Output> {
        let last_read_index = self.last_read_index(dependent, producer);
        let res = self.slots.get(&producer).and_then(|slot_vec| {
            slot_vec
                .get(last_read_index)
                .and_then(|boxed| boxed.as_ref().downcast_ref::<T::Output>())
        });

        // Only advance the read cursor when we actually consumed a value.
        // Otherwise dependents never see new facts when a producer appends later (e.g. in cycles).
        if res.is_some() {
            self.cursor
                .write()
                .expect("lock poisoned")
                .insert((dependent, producer), last_read_index + 1);
        } else if self.slot_count(producer) == last_read_index {
            // the producer has not produced any new values so we get the last value
            // This is a edge case for cyclic dependencies that have multiple dependencies
            // Some dependencies may have produced new values but not all of them.
            // TODO: Perhaps it would make more sense to return the combined value of all the dependencies in this case.
            return self
                .get_last(producer)
                .and_then(|boxed| boxed.downcast_ref::<T::Output>());
        }

        res
    }

    pub fn last_read_index(&self, dependent: NodeId, producer: NodeId) -> usize {
        self.cursor
            .read()
            .expect("lock poisoned")
            .get(&(dependent, producer))
            .copied()
            .unwrap_or(0)
    }

    /// Check if a value exists for a node.
    pub fn contains(&self, id: NodeId) -> bool {
        self.slots.contains_key(&id)
    }

    /// Get the size of slots produced by a node
    pub fn slot_count(&self, producer: NodeId) -> usize {
        self.slots
            .get(&producer)
            .map(|boxed| boxed.len())
            .unwrap_or(0)
    }
}

impl std::fmt::Debug for NodeStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeStorage")
            .field("slot_count", &self.slots.len())
            .finish()
    }
}
