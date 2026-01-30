//! AST-based heuristics for the pipeline DSL.
//!
//! This module provides implementations of the Bitcoin transaction analysis heuristics
//! as nodes in the typed pipeline DSL.

mod change;
mod coinjoin;
mod common_input;
mod same_address;

#[cfg(test)]
mod tests;

pub use change::{
    ChangeClustering, ChangeClusteringNode, ChangeIdentification, ChangeIdentificationNode,
    IsUnilateral, IsUnilateralNode,
};
pub use coinjoin::{IsCoinJoin, IsCoinJoinNode};
pub use common_input::{MultiInputHeuristic, MultiInputHeuristicNode};
pub use same_address::SameAddressClusteringNode;