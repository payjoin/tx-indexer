use std::collections::{HashMap, HashSet};
use std::{any::TypeId, hash::Hash};

use crate::loose::{TxId, TxOutId};
use crate::test_utils::DummyTxData;

/// A named set of facts of the same shape.
/// Facts can either be base facts or derived facts.
/// Derived facts are derived by applying rules
pub trait Relation: 'static {
    type Fact: Clone + Eq + Hash + 'static;
    const NAME: &'static str;
}

pub struct TxRel;
impl Relation for TxRel {
    type Fact = DummyTxData;
    const NAME: &'static str = "Tx";
}

pub struct IsCoinJoinRel;
impl Relation for IsCoinJoinRel {
    // TODO Replace bool with enum
    type Fact = (TxId, bool);
    const NAME: &'static str = "IsCoinJoin";
}

pub struct LinkRel;
impl Relation for LinkRel {
    type Fact = (TxOutId, TxOutId);
    const NAME: &'static str = "Link";
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
pub trait FactStore {
    fn insert<R: Relation>(&mut self, fact: R::Fact) -> bool;
    fn len<R: Relation>(&self) -> usize;
    fn read_range<R: Relation>(&self, start: usize, end: usize) -> Vec<R::Fact>;
    fn contains<R: Relation>(&self, fact: &R::Fact) -> bool;
}

pub struct MemStore {
    /// Map of relational type ids and their state
    rels: HashMap<TypeId, Box<dyn RelBox>>,
}

impl MemStore {
    pub fn new() -> Self {
        Self {
            rels: HashMap::new(),
        }
    }

    // TODO: placeholder hack should remove later
    pub fn initialize<R: Relation>(&mut self) {
        let tid = TypeId::of::<R>();
        if !self.rels.contains_key(&tid) {
            self.rels.insert(tid, Box::new(RelState::<R::Fact>::new()));
        }
    }

    fn get_or_init<R: Relation>(&mut self) -> &mut RelState<R::Fact> {
        let tid = TypeId::of::<R>();
        if !self.rels.contains_key(&tid) {
            self.rels.insert(tid, Box::new(RelState::<R::Fact>::new()));
        }
        self.rels
            .get_mut(&tid)
            .unwrap()
            .as_any_mut()
            .downcast_mut::<RelState<R::Fact>>()
            .expect("relation state type mismatch")
    }

    fn get<R: Relation>(&self) -> &RelState<R::Fact> {
        let tid = TypeId::of::<R>();
        self.rels
            .get(&tid)
            .expect("relation not initialized")
            .as_any()
            .downcast_ref::<RelState<R::Fact>>()
            .expect("relation state type mismatch")
    }
}

trait RelBox {
    fn as_any(&self) -> &dyn std::any::Any;
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
}

struct RelState<F: Clone + Eq + Hash + 'static> {
    log: Vec<F>,
    seen: HashSet<F>,
}

impl<F: Clone + Eq + Hash + 'static> RelState<F> {
    fn new() -> Self {
        Self {
            log: Vec::new(),
            seen: HashSet::new(),
        }
    }

    fn insert(&mut self, fact: F) -> bool {
        if self.seen.insert(fact.clone()) {
            self.log.push(fact);
            true
        } else {
            false
        }
    }
}

impl<F: Clone + Eq + Hash + 'static> RelBox for RelState<F> {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl FactStore for MemStore {
    fn insert<R: Relation>(&mut self, fact: R::Fact) -> bool {
        self.get_or_init::<R>().insert(fact)
    }

    fn len<R: Relation>(&self) -> usize {
        self.get::<R>().log.len()
    }

    fn read_range<R: Relation>(&self, start: usize, end: usize) -> Vec<R::Fact> {
        let st = self.get::<R>();
        let end = end.min(st.log.len());
        if start >= end {
            return Vec::new();
        }
        st.log[start..end].to_vec()
    }

    fn contains<R: Relation>(&self, fact: &R::Fact) -> bool {
        self.get::<R>().seen.contains(fact)
    }
}

pub struct CursorBook {
    // (rule_id, relation_typeid) -> offset
    offsets: HashMap<(usize, TypeId), usize>,
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

    pub fn read_delta<R: Relation>(
        &mut self,
        rule_id: usize,
        store: &impl FactStore,
    ) -> Vec<R::Fact> {
        let rel = TypeId::of::<R>();
        let start = self.get(rule_id, rel);
        let end = store.len::<R>();
        let delta = store.read_range::<R>(start, end);
        self.set(rule_id, rel, end);
        delta
    }
}
