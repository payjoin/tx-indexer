use std::any::TypeId;
use std::collections::{HashMap, HashSet};

use crate::abstract_types::AbstractTxWrapper;
use crate::disjoint_set::SparseDisjointSet;
use crate::loose::{TxId, TxOutId};
use crate::storage::{FactStore, InMemoryIndex, MemStore};

/// A named set of facts of the same shape.
/// Facts can either be base facts or derived facts.
/// Derived facts are derived by applying rules
pub trait Relation: 'static {
    type Fact: Clone + Eq + 'static;
    const NAME: &'static str;
}

pub struct RawTxRel;
impl Relation for RawTxRel {
    type Fact = AbstractTxWrapper;
    const NAME: &'static str = "RawTx";
}

pub struct TxRel;
impl Relation for TxRel {
    type Fact = TxId;
    const NAME: &'static str = "Tx";
}

#[derive(Clone, Eq, PartialEq, Copy, Debug)]
pub enum TxAnnotation {
    NotCoinJoin,
    CoinJoin,
}

pub struct IsCoinJoinRel;
impl Relation for IsCoinJoinRel {
    type Fact = (TxId, TxAnnotation);
    const NAME: &'static str = "IsCoinJoin";
}

pub struct ClusterRel;
impl Relation for ClusterRel {
    type Fact = SparseDisjointSet<TxOutId>;
    const NAME: &'static str = "Cluster";
}

#[derive(Clone, Eq, PartialEq, Copy, Debug)]
pub enum TxOutAnnotation {
    Change,
    NotChange,
}

pub struct ChangeIdentificationRel;
impl Relation for ChangeIdentificationRel {
    type Fact = (TxOutId, TxOutAnnotation);
    const NAME: &'static str = "ChangeIdentification";
}

pub struct GlobalClusteringRel;
impl Relation for GlobalClusteringRel {
    type Fact = SparseDisjointSet<TxOutId>;
    const NAME: &'static str = "GlobalClustering";
}

// TODO Tagged facts: different facts existing in different domain of knowledge

/// Trait for rule input types that can collect data from relations
pub trait RuleInput: 'static {
    /// Collect data from the specified relations, reading only new deltas
    fn collect_from_relations(
        relations: &[TypeId],
        store: &MemStore,
        cursors: &mut CursorBook,
        rule_id: usize,
    ) -> Self;

    /// Extract self from the enum, or return None if type doesn't match
    fn from_enum(input: RuleInputEnum) -> Option<Self>
    where
        Self: Sized;
}

/// Type-erased rule input for use with trait objects
pub enum RuleInputEnum {
    Transaction(TransactionInput),
    Cluster(ClusterInput),
    TransactionAnnotation(TransactionAnnotationInput),
    RawTransaction(RawTransactionInput),
}

impl RuleInputEnum {
    pub fn collect_from_relations(
        input_type: TypeId,
        relations: &[TypeId],
        store: &MemStore,
        cursors: &mut CursorBook,
        rule_id: usize,
    ) -> Self {
        if input_type == TypeId::of::<TransactionInput>() {
            RuleInputEnum::Transaction(TransactionInput::collect_from_relations(
                relations, store, cursors, rule_id,
            ))
        } else if input_type == TypeId::of::<ClusterInput>() {
            RuleInputEnum::Cluster(ClusterInput::collect_from_relations(
                relations, store, cursors, rule_id,
            ))
        } else if input_type == TypeId::of::<TransactionAnnotationInput>() {
            RuleInputEnum::TransactionAnnotation(
                TransactionAnnotationInput::collect_from_relations(
                    relations, store, cursors, rule_id,
                ),
            )
        } else if input_type == TypeId::of::<RawTransactionInput>() {
            RuleInputEnum::RawTransaction(RawTransactionInput::collect_from_relations(
                relations, store, cursors, rule_id,
            ))
        } else {
            panic!("Unknown input type");
        }
    }
}

/// Convert clustering sets to affected transaction IDs.
///
/// For each clustering set, extracts:
/// - Containing transaction IDs: transactions that contain the txouts in the clusters
/// - Spending transaction IDs: transactions that spend those txouts
///
/// This is a reusable conversion function that defines how to transform
/// GlobalClusteringRel facts (SparseDisjointSet<TxOutId>) into affected TxIds.
fn affected_txids_from_clustering_sets(
    clustering_sets: Vec<SparseDisjointSet<TxOutId>>,
    index: &InMemoryIndex,
) -> HashSet<TxId> {
    let mut affected_txids = HashSet::new();

    for cluster_set in clustering_sets {
        // Get all parent IDs (roots) in this clustering set
        let parent_ids: Vec<_> = cluster_set.iter_parent_ids().collect();

        // For each parent, get all elements in that set
        for parent_id in parent_ids {
            for txout_id in cluster_set.iter_set(parent_id) {
                // Add containing transaction ID
                affected_txids.insert(txout_id.txid);

                // Add spending transaction IDs
                if let Some(txin_id) = index.spending_txins.get(&txout_id) {
                    affected_txids.insert(txin_id.txid());
                }
            }
        }
    }

    affected_txids
}

/// Iterator-based input for rules that process transactions
pub struct TransactionInput {
    // TODO should this be a vec of refrences to the fact store?
    txids: HashSet<TxId>,
}

impl TransactionInput {
    pub fn iter(&self) -> impl Iterator<Item = TxId> + '_ {
        self.txids.iter().copied()
    }

    pub fn new(txids: HashSet<TxId>) -> Self {
        Self { txids }
    }
}

impl RuleInput for TransactionInput {
    fn collect_from_relations(
        relations: &[TypeId],
        store: &MemStore,
        cursors: &mut CursorBook,
        rule_id: usize,
    ) -> Self {
        let mut txids = HashSet::new();

        for &rel_type_id in relations {
            if rel_type_id == TypeId::of::<TxRel>() {
                let delta: Vec<TxId> = cursors.read_delta::<TxRel>(rule_id, store);
                txids.extend(delta);
            } else if rel_type_id == TypeId::of::<GlobalClusteringRel>() {
                let delta: Vec<SparseDisjointSet<TxOutId>> =
                    cursors.read_delta::<GlobalClusteringRel>(rule_id, store);
                let affected = affected_txids_from_clustering_sets(delta, store.index());
                txids.extend(affected);
            }
        }

        Self {
            txids: txids.into_iter().collect(),
        }
    }

    fn from_enum(input: RuleInputEnum) -> Option<Self> {
        match input {
            RuleInputEnum::Transaction(i) => Some(i),
            _ => None,
        }
    }
}

/// Iterator-based input for rules that process clusters
pub struct ClusterInput {
    clusters: Vec<SparseDisjointSet<TxOutId>>,
}

impl ClusterInput {
    pub fn iter(&self) -> impl Iterator<Item = &SparseDisjointSet<TxOutId>> {
        self.clusters.iter()
    }
}

impl RuleInput for ClusterInput {
    fn collect_from_relations(
        relations: &[TypeId],
        store: &MemStore,
        cursors: &mut CursorBook,
        rule_id: usize,
    ) -> Self {
        let mut clusters = Vec::new();

        for &rel_type_id in relations {
            if rel_type_id == TypeId::of::<ClusterRel>() {
                let delta: Vec<SparseDisjointSet<TxOutId>> =
                    cursors.read_delta::<ClusterRel>(rule_id, store);
                clusters.extend(delta);
            }
        }

        Self { clusters }
    }

    fn from_enum(input: RuleInputEnum) -> Option<Self> {
        match input {
            RuleInputEnum::Cluster(i) => Some(i),
            _ => None,
        }
    }
}

/// Iterator-based input for rules that process transaction annotations
pub struct TransactionAnnotationInput {
    annotations: Vec<(TxId, TxAnnotation)>,
}

impl TransactionAnnotationInput {
    pub fn iter(&self) -> impl Iterator<Item = (TxId, TxAnnotation)> + '_ {
        self.annotations.iter().copied()
    }
}

impl RuleInput for TransactionAnnotationInput {
    fn collect_from_relations(
        relations: &[TypeId],
        store: &MemStore,
        cursors: &mut CursorBook,
        rule_id: usize,
    ) -> Self {
        let mut annotations = Vec::new();

        for &rel_type_id in relations {
            if rel_type_id == TypeId::of::<IsCoinJoinRel>() {
                let delta: Vec<(TxId, TxAnnotation)> =
                    cursors.read_delta::<IsCoinJoinRel>(rule_id, store);
                annotations.extend(delta);
            }
        }

        Self { annotations }
    }

    fn from_enum(input: RuleInputEnum) -> Option<Self> {
        match input {
            RuleInputEnum::TransactionAnnotation(i) => Some(i),
            _ => None,
        }
    }
}

/// Iterator-based input for rules that process transaction outputs
pub struct TxOutInput {
    txouts: Vec<TxOutId>,
}

impl TxOutInput {
    pub fn iter(&self) -> impl Iterator<Item = TxOutId> + '_ {
        self.txouts.iter().copied()
    }

    pub fn new(txouts: Vec<TxOutId>) -> Self {
        Self { txouts }
    }
}

/// Input for raw transaction ingestion
pub struct RawTransactionInput {
    transactions: Vec<AbstractTxWrapper>,
}

impl RawTransactionInput {
    pub fn iter(&self) -> impl Iterator<Item = &AbstractTxWrapper> {
        self.transactions.iter()
    }
}

impl RuleInput for RawTransactionInput {
    fn collect_from_relations(
        relations: &[TypeId],
        store: &MemStore,
        cursors: &mut CursorBook,
        rule_id: usize,
    ) -> Self {
        let mut transactions = Vec::new();

        for &rel_type_id in relations {
            if rel_type_id == TypeId::of::<RawTxRel>() {
                let delta: Vec<AbstractTxWrapper> = cursors.read_delta::<RawTxRel>(rule_id, store);
                transactions.extend(delta);
            }
        }

        Self { transactions }
    }

    fn from_enum(input: RuleInputEnum) -> Option<Self> {
        match input {
            RuleInputEnum::RawTransaction(i) => Some(i),
            _ => None,
        }
    }
}

/// A rule is a function that takes a set of facts and emits a set of derived facts.
pub trait Rule {
    type Input: RuleInput;

    fn name(&self) -> &'static str;

    /// Which relations does this rule depend on? Used for optimization later.
    fn inputs(&self) -> &'static [TypeId];

    /// Called repeatedly until no rule makes progress.
    /// The input contains all new data from the rule's dependencies.
    fn step(&mut self, input: Self::Input, store: &mut MemStore) -> usize;
}

/// Object-safe helper trait for rules
pub trait RuleObj: 'static {
    fn name(&self) -> &'static str;
    fn inputs(&self) -> &'static [TypeId];
    fn input_type_id(&self) -> TypeId;
    fn step(&mut self, input: RuleInputEnum, store: &mut MemStore) -> usize;
}

impl<T: Rule + 'static> RuleObj for T
where
    T::Input: RuleInput,
{
    fn name(&self) -> &'static str {
        <T as Rule>::name(self)
    }

    fn inputs(&self) -> &'static [TypeId] {
        <T as Rule>::inputs(self)
    }

    fn input_type_id(&self) -> TypeId {
        std::any::TypeId::of::<T::Input>()
    }

    fn step(&mut self, input: RuleInputEnum, store: &mut MemStore) -> usize {
        if let Some(converted_input) = T::Input::from_enum(input) {
            self.step(converted_input, store)
        } else {
            panic!(
                "Input type mismatch for rule {}: expected {:?}",
                self.name(),
                std::any::TypeId::of::<T::Input>()
            );
        }
    }
}

pub struct Engine {
    rules: Vec<Box<dyn RuleObj>>,
    cursors: CursorBook,
}

pub struct EngineBuilder {
    rules: Vec<Box<dyn RuleObj>>,
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

    pub fn add_rule<T: RuleObj>(mut self, rule: T) -> Self {
        self.rules.push(Box::new(rule));
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
                let dependencies = r.inputs();
                let input_type = r.input_type_id();
                let input = RuleInputEnum::collect_from_relations(
                    input_type,
                    dependencies,
                    store,
                    &mut self.cursors,
                    rid,
                );
                progressed += r.step(input, store);
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

    pub fn read_delta<R: Relation>(&mut self, rule_id: usize, store: &MemStore) -> Vec<R::Fact> {
        let rel = TypeId::of::<R>();
        let start = self.get(rule_id, rel);
        let end = store.len::<R>();
        let delta = store.read_range::<R>(start, end);
        self.set(rule_id, rel, end);
        delta
    }
}
