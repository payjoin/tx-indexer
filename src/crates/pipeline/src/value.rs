//! Value types for the pipeline DSL.
//!
//! This module defines the `ExprValue` trait and marker types that represent
//! different kinds of values that expressions can produce.

use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::marker::PhantomData;

use tx_indexer_primitives::disjoint_set::SparseDisjointSet;

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
pub struct Set<T>(PhantomData<T>);

impl<T> ExprValue for Set<T>
where
    T: Eq + Hash + Clone + Send + Sync + 'static,
{
    type Output = HashSet<T>;

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

// Value Type Aliases for convenience
// TODO: these dont seem needed

/// A mask over transaction IDs.
pub type TxMask<T> = Mask<T>;

/// A mask over transaction output IDs.
pub type TxOutMask<T> = Mask<T>;
