//! Filter operations for the pipeline DSL.

use std::collections::HashSet;
use std::hash::Hash;
use std::sync::Arc;

use crate::engine::EvalContext;
use crate::expr::Expr;
use crate::node::{Node, NodeId};
use crate::value::{ExprValue, Mask, TxOutSet, TxSet};
use tx_indexer_primitives::unified::AnyOutId;
use tx_indexer_primitives::unified::AnyTxId;

/// Node that filters a set using a boolean mask.
///
/// Items where the mask is `true` are kept, items where the mask is `false`
/// (or not present) are removed.
pub struct FilterWithMaskNode<T: ExprValue, K: Eq + Hash + Clone + Send + Sync + 'static> {
    input: Expr<T>,
    mask: Expr<Mask<K>>,
}

impl<T: ExprValue, K: Eq + Hash + Clone + Send + Sync + 'static> FilterWithMaskNode<T, K> {
    pub fn new(input: Expr<T>, mask: Expr<Mask<K>>) -> Self {
        Self { input, mask }
    }
}

impl Node for FilterWithMaskNode<TxSet, AnyTxId> {
    type OutputValue = TxSet;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.input.id(), self.mask.id()]
    }

    fn evaluate(&self, ctx: &EvalContext) -> HashSet<AnyTxId> {
        let input_set = ctx.get_or_default(&self.input);
        let mask = ctx.get_or_default(&self.mask);

        input_set
            .iter()
            .filter(|&id| *mask.get(id).unwrap_or(&false))
            .copied()
            .collect()
    }

    fn name(&self) -> &'static str {
        "FilterWithMask<TxSet>"
    }
}

impl Node for FilterWithMaskNode<TxOutSet, AnyOutId> {
    type OutputValue = TxOutSet;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.input.id(), self.mask.id()]
    }

    fn evaluate(&self, ctx: &EvalContext) -> HashSet<AnyOutId> {
        let input_set = ctx.get_or_default(&self.input);
        let mask = ctx.get_or_default(&self.mask);

        input_set
            .iter()
            .filter(|&id| *mask.get(id).unwrap_or(&false))
            .copied()
            .collect()
    }

    fn name(&self) -> &'static str {
        "FilterWithMask<TxOutSet>"
    }
}

/// Node that filters a set using an arbitrary predicate closure.
///
/// The predicate receives each element ID and the evaluation context,
/// and returns `true` to keep the element.
pub struct FilterWithPredicateNode<T: ExprValue, K: Eq + Hash + Clone + Send + Sync + 'static> {
    input: Expr<T>,
    #[allow(clippy::type_complexity)]
    predicate: Arc<dyn Fn(&K, &EvalContext) -> bool + Send + Sync>,
}

impl<T: ExprValue, K: Eq + Hash + Clone + Send + Sync + 'static> FilterWithPredicateNode<T, K> {
    #[allow(clippy::type_complexity)]
    pub fn new(
        input: Expr<T>,
        predicate: Arc<dyn Fn(&K, &EvalContext) -> bool + Send + Sync>,
    ) -> Self {
        Self { input, predicate }
    }
}

impl Node for FilterWithPredicateNode<TxSet, AnyTxId> {
    type OutputValue = TxSet;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.input.id()]
    }

    fn evaluate(&self, ctx: &EvalContext) -> HashSet<AnyTxId> {
        let input_set = ctx.get_or_default(&self.input);
        input_set
            .iter()
            .filter(|id| (self.predicate)(id, ctx))
            .copied()
            .collect()
    }

    fn name(&self) -> &'static str {
        "FilterWithPredicate<TxSet>"
    }
}

impl Node for FilterWithPredicateNode<TxOutSet, AnyOutId> {
    type OutputValue = TxOutSet;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.input.id()]
    }

    fn evaluate(&self, ctx: &EvalContext) -> HashSet<AnyOutId> {
        let input_set = ctx.get_or_default(&self.input);
        input_set
            .iter()
            .filter(|id| (self.predicate)(id, ctx))
            .copied()
            .collect()
    }

    fn name(&self) -> &'static str {
        "FilterWithPredicate<TxOutSet>"
    }
}

// Extension methods on Expr<TxSet>
impl Expr<TxSet> {
    /// Filter transactions using a boolean mask.
    ///
    /// Keeps transactions where the mask value is `true`.
    pub fn filter_with_mask(&self, mask: Expr<Mask<AnyTxId>>) -> Expr<TxSet> {
        self.ctx
            .register(FilterWithMaskNode::new(self.clone(), mask))
    }
    /// Filter transactions using an arbitrary predicate.
    ///
    /// The closure receives each transaction ID and the evaluation context.
    pub fn filter(
        &self,
        f: impl Fn(&AnyTxId, &EvalContext) -> bool + Send + Sync + 'static,
    ) -> Expr<TxSet> {
        self.ctx
            .register(FilterWithPredicateNode::new(self.clone(), Arc::new(f)))
    }
}

// Extension methods on Expr<TxOutSet>
impl Expr<TxOutSet> {
    /// Filter transaction outputs using a boolean mask.
    ///
    /// Keeps outputs where the mask value is `true`.
    pub fn filter_with_mask(&self, mask: Expr<Mask<AnyOutId>>) -> Expr<TxOutSet> {
        self.ctx
            .register(FilterWithMaskNode::new(self.clone(), mask))
    }
    /// Filter transaction outputs using an arbitrary predicate.
    ///
    /// The closure receives each output ID and the evaluation context.
    pub fn filter(
        &self,
        f: impl Fn(&AnyOutId, &EvalContext) -> bool + Send + Sync + 'static,
    ) -> Expr<TxOutSet> {
        self.ctx
            .register(FilterWithPredicateNode::new(self.clone(), Arc::new(f)))
    }
}

/// Node that filters a set by excluding items where mask is true.
/// This is the inverse of FilterWithMask - keeps items where mask is false.
pub struct FilterExcludeNode<T: ExprValue, K: Eq + Hash + Clone + Send + Sync + 'static> {
    input: Expr<T>,
    mask: Expr<Mask<K>>,
}

impl<T: ExprValue, K: Eq + Hash + Clone + Send + Sync + 'static> FilterExcludeNode<T, K> {
    pub fn new(input: Expr<T>, mask: Expr<Mask<K>>) -> Self {
        Self { input, mask }
    }
}

impl Node for FilterExcludeNode<TxSet, AnyTxId> {
    type OutputValue = TxSet;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.input.id(), self.mask.id()]
    }

    fn evaluate(&self, ctx: &EvalContext) -> HashSet<AnyTxId> {
        let input_set = ctx.get(&self.input);
        let mask = ctx.get(&self.mask);

        input_set
            .iter()
            .filter(|&id| !*mask.get(id).unwrap_or(&false))
            .copied()
            .collect()
    }

    fn name(&self) -> &'static str {
        "FilterExclude<TxSet>"
    }
}

impl Node for FilterExcludeNode<TxOutSet, AnyOutId> {
    type OutputValue = TxOutSet;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.input.id(), self.mask.id()]
    }

    fn evaluate(&self, ctx: &EvalContext) -> HashSet<AnyOutId> {
        let input_set = ctx.get(&self.input);
        let mask = ctx.get(&self.mask);

        input_set
            .iter()
            .filter(|&id| !*mask.get(id).unwrap_or(&false))
            .copied()
            .collect()
    }

    fn name(&self) -> &'static str {
        "FilterExclude<TxOutSet>"
    }
}
