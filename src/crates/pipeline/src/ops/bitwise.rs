//! Bitwise operations for masks in the pipeline DSL.

use std::collections::HashMap;
use std::hash::Hash;
use std::ops::BitAnd;

use crate::engine::EvalContext;
use crate::expr::Expr;
use crate::node::{Node, NodeId};
use crate::value::Mask;

/// Node that performs bitwise AND on two masks.
///
/// The result is `true` only where both input masks are `true`.
/// Keys present in only one mask are treated as `false` in the other.
pub struct AndMasksNode<K: Eq + Hash + Clone + Send + Sync + 'static> {
    left: Expr<Mask<K>>,
    right: Expr<Mask<K>>,
}

impl<K: Eq + Hash + Clone + Send + Sync + 'static> AndMasksNode<K> {
    pub fn new(left: Expr<Mask<K>>, right: Expr<Mask<K>>) -> Self {
        Self { left, right }
    }
}

impl<K: Eq + Hash + Clone + Send + Sync + 'static> Node for AndMasksNode<K> {
    type Value = Mask<K>;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.left.id(), self.right.id()]
    }

    fn evaluate(&self, ctx: &EvalContext) -> HashMap<K, bool> {
        let left = ctx.get(&self.left);
        let right = ctx.get(&self.right);

        // Union of keys, AND of values
        let mut result = HashMap::new();

        for (k, &v) in left {
            let right_val = right.get(k).copied().unwrap_or(false);
            result.insert(k.clone(), v && right_val);
        }

        // Also include keys only in right (they'll be false since left doesn't have them)
        for (k, &v) in right {
            if !left.contains_key(k) {
                result.insert(k.clone(), false && v);
            }
        }

        result
    }

    fn name(&self) -> &'static str {
        "AndMasks"
    }
}

/// Node that performs bitwise OR on two masks.
///
/// The result is `true` where either input mask is `true`.
pub struct OrMasksNode<K: Eq + Hash + Clone + Send + Sync + 'static> {
    left: Expr<Mask<K>>,
    right: Expr<Mask<K>>,
}

impl<K: Eq + Hash + Clone + Send + Sync + 'static> OrMasksNode<K> {
    pub fn new(left: Expr<Mask<K>>, right: Expr<Mask<K>>) -> Self {
        Self { left, right }
    }
}

impl<K: Eq + Hash + Clone + Send + Sync + 'static> Node for OrMasksNode<K> {
    type Value = Mask<K>;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.left.id(), self.right.id()]
    }

    fn evaluate(&self, ctx: &EvalContext) -> HashMap<K, bool> {
        let left = ctx.get(&self.left);
        let right = ctx.get(&self.right);

        // Union of keys, OR of values
        let mut result = HashMap::new();

        for (k, &v) in left {
            let right_val = right.get(k).copied().unwrap_or(false);
            result.insert(k.clone(), v || right_val);
        }

        for (k, &v) in right {
            if !left.contains_key(k) {
                result.insert(k.clone(), v);
            }
        }

        result
    }

    fn name(&self) -> &'static str {
        "OrMasks"
    }
}

// Implement BitAnd trait for Expr<Mask<K>>
impl<K: Eq + Hash + Clone + Send + Sync + 'static> BitAnd for Expr<Mask<K>> {
    type Output = Expr<Mask<K>>;

    fn bitand(self, rhs: Self) -> Self::Output {
        self.ctx.register(AndMasksNode::new(self.clone(), rhs))
    }
}

// Also implement for references to avoid moves
impl<K: Eq + Hash + Clone + Send + Sync + 'static> BitAnd for &Expr<Mask<K>> {
    type Output = Expr<Mask<K>>;

    fn bitand(self, rhs: Self) -> Self::Output {
        self.ctx
            .register(AndMasksNode::new(self.clone(), rhs.clone()))
    }
}

// Extension methods for explicit and/or
impl<K: Eq + Hash + Clone + Send + Sync + 'static> Expr<Mask<K>> {
    /// Bitwise AND with another mask.
    ///
    /// Same as `self & other`.
    pub fn and(&self, other: Expr<Mask<K>>) -> Expr<Mask<K>> {
        self.ctx.register(AndMasksNode::new(self.clone(), other))
    }

    /// Bitwise OR with another mask.
    pub fn or(&self, other: Expr<Mask<K>>) -> Expr<Mask<K>> {
        self.ctx.register(OrMasksNode::new(self.clone(), other))
    }
}
