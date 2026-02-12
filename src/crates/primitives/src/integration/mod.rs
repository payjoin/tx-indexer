//! Integration test utilities: regtest node and wallet setup, test harness.
//!
//! Compiled when `test` is enabled or the `integration-test` feature is set,
//! so that other crates can reuse the same node/harness by depending on
//! `tx-indexer-primitives` with `dev-dependencies` and feature `integration-test`.

mod node_harness;

pub use node_harness::{HarnessOut, NodeHarness, run_harness};
