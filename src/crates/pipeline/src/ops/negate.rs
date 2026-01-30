//! Mask negation operation for the pipeline DSL.

use std::collections::HashMap;
use std::hash::Hash;

use crate::engine::EvalContext;
use crate::expr::Expr;
use crate::node::{Node, NodeId};
use crate::value::Mask;

/// Node that negates a boolean mask.
///
/// `true` becomes `false` and vice versa.
pub struct NegateMaskNode<K: Eq + Hash + Clone + Send + Sync + 'static> {
    input: Expr<Mask<K>>,
}

impl<K: Eq + Hash + Clone + Send + Sync + 'static> NegateMaskNode<K> {
    pub fn new(input: Expr<Mask<K>>) -> Self {
        Self { input }
    }
}

impl<K: Eq + Hash + Clone + Send + Sync + 'static> Node for NegateMaskNode<K> {
    type OutputValue = Mask<K>;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.input.id()]
    }

    fn evaluate(&self, ctx: &EvalContext) -> HashMap<K, bool> {
        let input_mask = ctx.get(&self.input);
        input_mask.iter().map(|(k, v)| (k.clone(), !v)).collect()
    }

    fn name(&self) -> &'static str {
        "NegateMask"
    }
}

// Extension methods on Expr<Mask<K>>
impl<K: Eq + Hash + Clone + Send + Sync + 'static> Expr<Mask<K>> {
    /// Negate this mask.
    ///
    /// Returns a new mask where `true` values become `false` and vice versa.
    pub fn negate(&self) -> Expr<Mask<K>> {
        self.ctx.register(NegateMaskNode::new(self.clone()))
    }
}
