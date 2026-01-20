use std::any::TypeId;
use std::collections::HashMap;

use crate::disjoint_set::SparseDisjointSet;
use crate::loose::{TxId, TxOutId};
use crate::storage::{FactStore, MemStore};
use crate::test_utils::DummyTxData;

/// A named set of facts of the same shape.
/// Facts can either be base facts or derived facts.
/// Derived facts are derived by applying rules
pub trait Relation: 'static {
    type Fact: Clone + Eq + 'static;
    const NAME: &'static str;
}

pub struct TxRel;
impl Relation for TxRel {
    // TODO: this should be a abstract transaction type
    type Fact = DummyTxData;
    const NAME: &'static str = "Tx";
}

pub struct IsCoinJoinRel;
impl Relation for IsCoinJoinRel {
    // TODO Replace bool with enum
    type Fact = (TxId, bool);
    const NAME: &'static str = "IsCoinJoin";
}

pub struct ClusterRel;
impl Relation for ClusterRel {
    type Fact = SparseDisjointSet<TxOutId>;
    const NAME: &'static str = "Cluster";
}

pub struct ChangeIdentificationRel;
impl Relation for ChangeIdentificationRel {
    type Fact = (TxOutId, bool);
    const NAME: &'static str = "ChangeIdentification";
}
// Tagged facts: different facts existing in different domain of knowledge

/// A rule is a function that takes a set of facts and emits a set of derived facts.
pub trait Rule {
    fn name(&self) -> &'static str;

    /// Which relations does this rule depend on? Used for optimization later.
    fn inputs(&self) -> &'static [TypeId];

    /// Called repeatedly until no rule makes progress.
    /// Must only process deltas via `cursors.read_delta::<R>(rule_id, store)`.
    fn step(&mut self, rule_id: usize, store: &mut MemStore, cursors: &mut CursorBook) -> usize;
}

pub struct Engine {
    rules: Vec<Box<dyn Rule>>,
    cursors: CursorBook,
}

pub struct EngineBuilder {
    rules: Vec<Box<dyn Rule>>,
}

impl Default for EngineBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl EngineBuilder {
    pub fn new() -> Self {
        Self { rules: Vec::new() }
    }

    pub fn add_rule(mut self, rule: Box<dyn Rule>) -> Self {
        self.rules.push(rule);
        self
    }

    pub fn build(self) -> Engine {
        Engine {
            rules: self.rules,
            cursors: CursorBook::new(),
        }
    }
}

impl Engine {
    pub fn run_to_fixpoint(&mut self, store: &mut MemStore) {
        loop {
            let mut progressed = 0;
            for (rid, r) in self.rules.iter_mut().enumerate() {
                progressed += r.step(rid, store, &mut self.cursors);
            }
            if progressed == 0 {
                break;
            }
        }
    }
}
pub struct CursorBook {
    // (rule_id, relation_typeid) -> offset
    offsets: HashMap<(usize, TypeId), usize>,
}

impl Default for CursorBook {
    fn default() -> Self {
        Self::new()
    }
}

impl CursorBook {
    pub fn new() -> Self {
        Self {
            offsets: HashMap::new(),
        }
    }

    fn get(&self, rule_id: usize, rel: TypeId) -> usize {
        *self.offsets.get(&(rule_id, rel)).unwrap_or(&0)
    }

    fn set(&mut self, rule_id: usize, rel: TypeId, off: usize) {
        self.offsets.insert((rule_id, rel), off);
    }

    // LEFT of here: need a trait for getting a collection of facts regadless of the shape of the fact
    pub fn read_delta<R: Relation>(&mut self, rule_id: usize, store: &MemStore) -> Vec<R::Fact> {
        let rel = TypeId::of::<R>();
        let start = self.get(rule_id, rel);
        let end = store.len::<R>();
        let delta = store.read_range::<R>(start, end);
        self.set(rule_id, rel, end);
        delta
    }
}
