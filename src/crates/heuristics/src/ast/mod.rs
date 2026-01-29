//! AST-based heuristics for the pipeline DSL.
//!
//! This module provides implementations of the Bitcoin transaction analysis heuristics
//! as nodes in the typed pipeline DSL.

mod coinjoin;
mod common_input;
mod change;

#[cfg(test)]
mod tests;

pub use coinjoin::{IsCoinJoin, IsCoinJoinNode};
pub use common_input::{MultiInputHeuristic, MultiInputHeuristicNode};
pub use change::{ChangeIdentification, ChangeIdentificationNode, ChangeClustering, ChangeClusteringNode, IsUnilateral, IsUnilateralNode};
