//! Value types for the pipeline DSL.
//!
//! This module defines the `ExprValue` trait and marker types that represent
//! different kinds of values that expressions can produce.

use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::{Arc, RwLock};

use tx_indexer_disjoint_set::SparseDisjointSet;
use tx_indexer_primitives::abstract_types::IdFamily;

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
pub struct Clustering<T>(PhantomData<T>);

impl<T> ExprValue for Clustering<T>
where
    T: Eq + Hash + Copy + Clone + Send + Sync + 'static,
{
    type Output = SparseDisjointSet<T>;

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

/// Marker type for an index handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Index<G>(PhantomData<G>);

#[derive(Debug)]
// TODO: lock seems unnecessary here as writing is only happening during index building.
pub struct IndexHandle<G>(Option<Arc<RwLock<G>>>);

impl<G> Clone for IndexHandle<G> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<G> Default for IndexHandle<G> {
    fn default() -> Self {
        Self(None)
    }
}

impl<G> PartialEq for IndexHandle<G> {
    fn eq(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (Some(left), Some(right)) => Arc::ptr_eq(left, right),
            (None, None) => true,
            _ => false,
        }
    }
}

impl<G> Eq for IndexHandle<G> {}

impl<G> IndexHandle<G> {
    pub fn new(index: Arc<RwLock<G>>) -> Self {
        Self(Some(index))
    }

    pub fn as_arc(&self) -> &Arc<RwLock<G>> {
        self.0.as_ref().expect("index handle not initialized")
    }
}

impl<G> ExprValue for Index<G>
where
    G: Send + Sync + 'static,
{
    type Output = IndexHandle<G>;

    fn combine_facts(facts: &[&Self::Output]) -> Self::Output {
        (*facts.last().expect("no fact present")).clone()
    }
}

/// Marker type for a source bundle (index + tx set).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceOutput<I, G>(PhantomData<(I, G)>);

#[derive(Debug)]
pub struct SourceOutputData<I: IdFamily, G> {
    pub index: IndexHandle<G>,
    pub txs: HashSet<I::TxId>,
}

impl<I: IdFamily, G> Clone for SourceOutputData<I, G>
where
    I::TxId: Clone,
{
    fn clone(&self) -> Self {
        Self {
            index: self.index.clone(),
            txs: self.txs.clone(),
        }
    }
}

impl<I: IdFamily, G> PartialEq for SourceOutputData<I, G>
where
    I::TxId: Eq,
{
    fn eq(&self, other: &Self) -> bool {
        self.index == other.index && self.txs == other.txs
    }
}

impl<I: IdFamily, G> Eq for SourceOutputData<I, G> where I::TxId: Eq {}

// TODO: index shouldnot impl Default
impl<I: IdFamily, G> Default for SourceOutputData<I, G> {
    fn default() -> Self {
        Self {
            index: IndexHandle::default(),
            txs: HashSet::new(),
        }
    }
}

impl<I, G> ExprValue for SourceOutput<I, G>
where
    I: IdFamily + 'static,
    G: Send + Sync + 'static,
    I::TxId: Eq + Hash + Clone + Send + Sync + 'static,
{
    type Output = SourceOutputData<I, G>;

    fn combine_facts(facts: &[&Self::Output]) -> Self::Output {
        facts
            .last()
            .map(|r| (*r).clone())
            .unwrap_or_else(Self::Output::default)
    }
}

// Value Type Aliases for convenience
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionSet<I: IdFamily>(PhantomData<I>);

impl<I> ExprValue for TransactionSet<I>
where
    I: IdFamily + 'static,
    I::TxId: Eq + Hash + Clone + Send + Sync + 'static,
{
    type Output = HashSet<I::TxId>;

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionOutSet<I: IdFamily>(PhantomData<I>);

impl<I> ExprValue for TransactionOutSet<I>
where
    I: IdFamily + 'static,
    I::TxOutId: Eq + Hash + Clone + Send + Sync + 'static,
{
    type Output = HashSet<I::TxOutId>;

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

pub type TxSet<I> = TransactionSet<I>;
pub type TxOutSet<I> = TransactionOutSet<I>;
pub type TxMask<I> = Mask<<I as IdFamily>::TxId>;
pub type TxOutMask<I> = Mask<<I as IdFamily>::TxOutId>;
pub type TxOutClustering<I> = Clustering<<I as IdFamily>::TxOutId>;
