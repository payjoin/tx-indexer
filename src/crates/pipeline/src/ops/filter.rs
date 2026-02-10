//! Filter operations for the pipeline DSL.

use std::collections::HashSet;
use std::hash::Hash;

use crate::engine::EvalContext;
use crate::expr::Expr;
use crate::node::{Node, NodeId};
use crate::value::{ExprValue, Mask, TxOutSet, TxSet};

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

impl<T, K> Node for FilterWithMaskNode<T, K>
where
    T: ExprValue<Output = HashSet<K>> + Send + Sync + 'static,
    K: Eq + Hash + Clone + Send + Sync + 'static,
{
    type OutputValue = T;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.input.id(), self.mask.id()]
    }

    fn evaluate(&self, ctx: &EvalContext) -> HashSet<K> {
        let input_set = ctx.get_or_default(&self.input);
        let mask = ctx.get_or_default(&self.mask);

        input_set
            .iter()
            .filter(|&id| *mask.get(id).unwrap_or(&false))
            .cloned()
            .collect()
    }

    fn name(&self) -> &'static str {
        "FilterWithMask"
    }
}

/// Node that filters a set by excluding items where mask is true.
pub struct FilterExcludeNode<T: ExprValue, K: Eq + Hash + Clone + Send + Sync + 'static> {
    input: Expr<T>,
    mask: Expr<Mask<K>>,
}

impl<T: ExprValue, K: Eq + Hash + Clone + Send + Sync + 'static> FilterExcludeNode<T, K> {
    pub fn new(input: Expr<T>, mask: Expr<Mask<K>>) -> Self {
        Self { input, mask }
    }
}

impl<T, K> Node for FilterExcludeNode<T, K>
where
    T: ExprValue<Output = HashSet<K>> + Send + Sync + 'static,
    K: Eq + Hash + Clone + Send + Sync + 'static,
{
    type OutputValue = T;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.input.id(), self.mask.id()]
    }

    fn evaluate(&self, ctx: &EvalContext) -> HashSet<K> {
        let input_set = ctx.get(&self.input);
        let mask = ctx.get(&self.mask);

        input_set
            .iter()
            .filter(|&id| !*mask.get(id).unwrap_or(&false))
            .cloned()
            .collect()
    }

    fn name(&self) -> &'static str {
        "FilterExclude"
    }
}

// Extension methods: Expr<TxSet<K>> and Expr<TxOutSet<K>>
impl<K: Eq + std::hash::Hash + Clone + Send + Sync + 'static> Expr<TxSet<K>> {
    /// Filter transactions using a boolean mask.
    pub fn filter_with_mask(&self, mask: Expr<Mask<K>>) -> Expr<TxSet<K>> {
        self.ctx
            .register(FilterWithMaskNode::new(self.clone(), mask))
    }
}

impl<K: Eq + std::hash::Hash + Clone + Send + Sync + 'static> Expr<TxOutSet<K>> {
    /// Filter transaction outputs using a boolean mask.
    pub fn filter_with_mask(&self, mask: Expr<Mask<K>>) -> Expr<TxOutSet<K>> {
        self.ctx
            .register(FilterWithMaskNode::new(self.clone(), mask))
    }
}
