//! Value types for the pipeline DSL.
//!
//! This module defines the `ExprValue` trait and marker types that represent
//! different kinds of values that expressions can produce.

use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::{Arc, RwLock};

use tx_indexer_primitives::abstract_id::{AbstractId, LooseIds};
use tx_indexer_primitives::disjoint_set::SparseDisjointSet;
use tx_indexer_primitives::graph_index::{IndexHandleFor, IndexedGraph};
use tx_indexer_primitives::loose::storage::InMemoryIndex;

/// Backend ties an ID family to its index handle type. Only the source (e.g. AllTxs) is concrete.
pub trait Backend: AbstractId  + Sized + Send + Sync + 'static {
    type IndexHandle: IndexHandleFor<Self> + Clone + Default + PartialEq + Send + Sync + 'static;
}

impl Backend for LooseIds {
    type IndexHandle = LooseIndexHandle;
}

/// Wrapper for the loose index handle so it can implement PartialEq (by pointer equality).
#[derive(Clone, Default)]
pub struct LooseIndexHandle(pub Arc<RwLock<InMemoryIndex>>);

impl IndexHandleFor<LooseIds> for LooseIndexHandle {
    fn with_graph<R>(&self, f: impl for<'a> FnOnce(&'a dyn IndexedGraph<LooseIds>) -> R) -> R {
        let guard = self.0.read().expect("lock poisoned");
        f(&*guard)
    }
}

impl PartialEq for LooseIndexHandle {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl Eq for LooseIndexHandle {}

/// Trait for types that can be the value of an expression.
///
/// This is a marker trait that associates a marker type with its concrete output type.
/// Users can define their own value types by implementing this trait.
pub trait ExprValue: 'static {
    /// The concrete Rust type that this expression produces when evaluated.
    type Output: Clone + Default + PartialEq + Send + Sync + 'static;

    /// Combine multiple facts (e.g. from fixpoint iterations) into a single value.
    /// Empty slice returns `Default::default()`. Implementations should clone at most
    /// once (the first fact or accumulator) and merge the rest by reference.
    fn combine_facts(facts: &[&Self::Output]) -> Self::Output;
}

// Built-in Value Types

/// Marker type for a set of transaction IDs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TxSet<K>(PhantomData<K>);

impl<K> Default for TxSet<K> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<K: Eq + Hash + Clone + Send + Sync + 'static> ExprValue for TxSet<K> {
    type Output = HashSet<K>;

    fn combine_facts(facts: &[&Self::Output]) -> Self::Output {
        if facts.is_empty() {
            return Default::default();
        }
        let mut acc = facts[0].clone();
        for rest in &facts[1..] {
            acc.extend(rest.iter().cloned());
        }
        acc
    }
}

/// Marker type for a set of transaction output IDs.
// TODO: since this is generic over its input, its the same as TxSet above. can we just have one set type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TxOutSet<K>(PhantomData<K>);

impl<K> Default for TxOutSet<K> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<K: Eq + Hash + Clone + Send + Sync + 'static> ExprValue for TxOutSet<K> {
    type Output = HashSet<K>;

    fn combine_facts(facts: &[&Self::Output]) -> Self::Output {
        if facts.is_empty() {
            return Default::default();
        }
        let mut acc = facts[0].clone();
        for rest in &facts[1..] {
            acc.extend(rest.iter().cloned());
        }
        acc
    }
}

/// Marker type for a boolean mask over items of type `K`.
///
/// A mask maps keys to boolean values, typically used for filtering.
/// `true` means "include", `false` means "exclude".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Mask<K>(PhantomData<K>);

impl<K> Default for Mask<K> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<K: Eq + Hash + Clone + Send + Sync + 'static> ExprValue for Mask<K> {
    type Output = HashMap<K, bool>;

    fn combine_facts(facts: &[&Self::Output]) -> Self::Output {
        if facts.is_empty() {
            return Default::default();
        }
        let mut acc = facts[0].clone();
        for rest in &facts[1..] {
            acc.extend(rest.iter().map(|(k, v)| (k.clone(), *v)));
        }
        acc
    }
}

/// Marker type for clustering (disjoint set union of transaction outputs).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Clustering<K>(PhantomData<K>);

impl<K: Eq + Hash + Copy  + Send + Sync + 'static> ExprValue for Clustering<K> {
    type Output = SparseDisjointSet<K>;

    fn combine_facts(facts: &[&Self::Output]) -> Self::Output {
        if facts.is_empty() {
            return Default::default();
        }
        if facts.len() == 1 {
            return facts[0].clone();
        }
        let mut acc = facts[0].clone();
        for next in &facts[1..] {
            acc = acc.join(next);
        }
        acc
    }
}

/// Marker type for the index expression for a backend. Output is the backend's index handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Index<I: Backend>(PhantomData<I>);

impl<I: Backend> Default for Index<I> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<I: Backend + Send + Sync + 'static> ExprValue for Index<I> {
    type Output = I::IndexHandle;

    fn combine_facts(facts: &[&Self::Output]) -> Self::Output {
        if facts.is_empty() {
            return Default::default();
        }
        facts[0].clone()
    }
}

/// Alias for the loose backend index. Nodes take `Expr<Index<I>>`; for I = LooseIds this is LooseIndex.
pub type LooseIndex = Index<LooseIds>;

/// Output of the AllTxs source node: the set of transaction IDs and the index handle.
/// Use projection nodes to obtain `Expr<TxSet<I::TxId>>` and `Expr<Index<I>>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AllTxsOutput<I: Backend> {
    _marker: PhantomData<I>,
}

impl<I: Backend> Default for AllTxsOutput<I> {
    fn default() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

impl<I: Backend + Send + Sync + 'static> ExprValue for AllTxsOutput<I>
where
    I::TxId: Eq + Hash + Clone + Send + Sync + 'static,
{
    type Output = (HashSet<I::TxId>, I::IndexHandle);

    fn combine_facts(facts: &[&Self::Output]) -> Self::Output {
        if facts.is_empty() {
            return Default::default();
        }
        facts[0].clone()
    }
}

