use std::{collections::HashMap, marker::PhantomData};

use crate::{
    disjoint_set::{DisJointSet, SparseDisjointSet},
    storage::InMemoryIndex,
};

pub type BoxIter<T> = Box<dyn Iterator<Item = T> + Send>;

/// An analysis pass: consumes itself and produces `O`.
/// `I` is the "declared input shape" (useful for type-level plumbing),
/// but in this simple model we only materialize the output.
pub trait AnalysisPass<I, O>: Sized {
    fn output(self) -> O;

    fn filter<F, O2>(self, filter: F) -> Filtered<Self, I, O, F>
    where
        F: FilterPass<O, O2>,
    {
        Filtered {
            upstream: self,
            filter,
            _pd: PhantomData,
        }
    }

    fn map<M, O2>(self, map: M) -> Mapped<Self, I, O, M, O2>
    where
        M: MapPass<O, O2>,
    {
        Mapped {
            upstream: self,
            map,
            _pd: PhantomData,
        }
    }

    // Merges the output of some other pass with this one
    fn merge<M, O2, P2, I2, O3>(self, other: P2, merge: M) -> Merged<Self, I, O, P2, I2, O2, M, O3>
    where
        P2: AnalysisPass<I2, O2>,
        // Merge pass takes the output of the two passes and produces an output
        M: MergePass<O, O2, O3>,
    {
        Merged {
            upstream: self,
            other,
            merge,
            _pd: PhantomData,
        }
    }
}

/// A filter pass: transforms an input collection into an output collection.
pub trait FilterPass<I, O> {
    fn filter(self, input: I) -> O;
}

/// A map pass: transforms item type (or shape) of the iterator.
pub trait MapPass<I, O> {
    fn map(self, input: I) -> O;
}

// Merging two analysis passes together
pub trait MergePass<I, I2, O> {
    fn merge(self, input: I, input2: I2) -> O;
}

/// The analysis pass produced by calling `.map()` on an upstream pass.
pub struct Mapped<P, I, O, M, O2> {
    upstream: P,
    map: M,
    _pd: PhantomData<(I, O, O2)>,
}

impl<P, I, O, M, O2> AnalysisPass<I, O2> for Mapped<P, I, O, M, O2>
where
    P: AnalysisPass<I, O>,
    M: MapPass<O, O2>,
{
    fn output(self) -> O2 {
        self.map.map(self.upstream.output())
    }
}

pub struct Filtered<P, I, O, F> {
    upstream: P,
    filter: F,
    _pd: PhantomData<(I, O)>,
}

impl<P, I, O, F> AnalysisPass<I, O> for Filtered<P, I, O, F>
where
    P: AnalysisPass<I, O>,
    F: FilterPass<O, O>,
{
    fn output(self) -> O {
        self.filter.filter(self.upstream.output())
    }
}

pub struct Merged<P, I, O, P2, I2, O2, M, O3> {
    upstream: P,
    other: P2,
    merge: M,
    _pd: PhantomData<(I, O, O2, I2, O3)>,
}

impl<P, I, O, P2, I2, O2, M, O3> AnalysisPass<I, O3> for Merged<P, I, O, P2, I2, O2, M, O3>
where
    P: AnalysisPass<I, O>,
    P2: AnalysisPass<I2, O2>,
    M: MergePass<O, O2, O3>,
{
    fn output(self) -> O3 {
        self.merge
            .merge(self.upstream.output(), self.other.output())
    }
}

pub struct FnMergePass<'a, T, U, Pred> {
    pred: Pred,
    _pd: PhantomData<(T, U)>,
    // TODO: remove later or replace with a trait object
    index: &'a InMemoryIndex,
}

impl<'a, T, U, Pred> FnMergePass<'a, T, U, Pred> {
    pub fn new(pred: Pred, index: &'a InMemoryIndex) -> Self {
        Self {
            pred,
            _pd: PhantomData,
            index,
        }
    }
}

impl<'a, T, U, Pred> MergePass<SparseDisjointSet<T>, HashMap<U, bool>, SparseDisjointSet<T>>
    for FnMergePass<'a, T, U, Pred>
where
    T: Eq + std::hash::Hash + Copy,
    U: Eq + std::hash::Hash + Copy,
    Pred: FnMut(&U, &InMemoryIndex) -> Option<(T, T)> + Send + 'static,
{
    fn merge(mut self, set: SparseDisjointSet<T>, map: HashMap<U, bool>) -> SparseDisjointSet<T> {
        for (u, is_true) in map {
            if is_true {
                // Perhaps is true should be passed into the closure
                if let Some((a, b)) = (self.pred)(&u, self.index) {
                    set.union(a, b);
                }
            }
        }
        set
    }
}

/// Generic filter pass built from a predicate:
/// BoxIter<T> -> BoxIter<T>
pub struct PredicateFilterPass<T, Pred> {
    pred: Pred,
    _pd: PhantomData<T>,
}

impl<T, Pred> PredicateFilterPass<T, Pred> {
    pub fn new(pred: Pred) -> Self {
        Self {
            pred,
            _pd: PhantomData,
        }
    }
}

impl<T, Pred> FilterPass<BoxIter<T>, BoxIter<T>> for PredicateFilterPass<T, Pred>
where
    T: Send + 'static,
    Pred: FnMut(&T) -> bool + Send + 'static,
{
    // TODO: should this be a an AnalysisPass where the output is a BoxIter<T>?
    fn filter(mut self, input: BoxIter<T>) -> BoxIter<T> {
        Box::new(input.filter(move |item| (self.pred)(item)))
    }
}

// --- Concrete types and passes

/// Generic map pass:
/// BoxIter<T> -> DisJointSet<U>
pub struct FnMapPass<T, U, F> {
    f: F,
    _pd: PhantomData<(T, U)>,
}

impl<T, U, F> FnMapPass<T, U, F> {
    pub fn new(f: F) -> Self {
        Self {
            f,
            _pd: PhantomData,
        }
    }
}

impl<T, U, F> MapPass<BoxIter<T>, SparseDisjointSet<U>> for FnMapPass<T, U, F>
where
    T: Send + 'static,
    U: Eq + std::hash::Hash + Copy + Send + 'static,
    F: FnMut(T) -> Option<Vec<(U, U)>> + Send + 'static,
{
    fn map(mut self, input: BoxIter<T>) -> SparseDisjointSet<U> {
        let disjoint_set = SparseDisjointSet::new();
        for item in input {
            if let Some(pairs) = (self.f)(item) {
                for (a, b) in pairs {
                    disjoint_set.union(a, b);
                }
            }
        }
        disjoint_set
    }
}

/// Generic map pass:
/// BoxIter<T> -> HashMap<P, bool>
pub struct FnMapToBoolMapPass<T, K, F> {
    f: F,
    _pd: PhantomData<(T, K)>,
}

impl<T, K, F> FnMapToBoolMapPass<T, K, F> {
    pub fn new(f: F) -> Self {
        Self {
            f,
            _pd: PhantomData,
        }
    }
}

impl<T, K, F> MapPass<BoxIter<T>, HashMap<K, bool>> for FnMapToBoolMapPass<T, K, F>
where
    T: Send + 'static,
    K: Eq + std::hash::Hash + Copy + Send + 'static,
    F: FnMut(&T) -> HashMap<K, bool> + Send + 'static,
{
    fn map(mut self, input: BoxIter<T>) -> HashMap<K, bool> {
        let mut map = HashMap::new();
        for item in input {
            let res = (self.f)(&item);
            map.extend(res);
        }
        map
    }
}

/// Generic map pass:
/// BoxIter<T> -> BoxIter<O>
pub struct FnMapIterPass<T, O, F> {
    f: F,
    _pd: PhantomData<(T, O)>,
}

impl<T, O, F> FnMapIterPass<T, O, F> {
    pub fn new(f: F) -> Self {
        Self {
            f,
            _pd: PhantomData,
        }
    }
}

impl<T, O, F> MapPass<BoxIter<T>, BoxIter<O>> for FnMapIterPass<T, O, F>
where
    T: Send + 'static,
    O: Send + 'static,
    F: FnMut(&T) -> O + Send + 'static,
{
    fn map(mut self, input: BoxIter<T>) -> BoxIter<O> {
        Box::new(input.map(move |item| (self.f)(&item)))
    }
}
