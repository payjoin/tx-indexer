# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
cargo build                          # build entire workspace
cargo test                           # run all tests
cargo test --lib --no-fail-fast      # run unit tests (as CI does)
cargo test -p tx-indexer-heuristics  # run tests for one crate
cargo test -p tx-indexer-heuristics ast::tests  # run a specific test module
cargo fmt --all                      # format code
cargo clippy                         # lint
cargo bench --all-features           # run benchmarks
```

## Architecture

This is a Rust workspace (`bitcoin-transaction-indexer`) with four crates:

```
src/crates/
  primitives/    # ID types, storage backends, abstract traits
  pipeline/      # lazy DSL engine for analysis pipelines
  heuristics/    # Bitcoin privacy heuristics (direct + AST/pipeline)
  disjoint-set/  # union-find data structure used for clustering
```

### Crate dependency order

`disjoint-set` ← `primitives` ← `pipeline` ← `heuristics`

---

### `primitives`: IDs, storage, and traits

**Unified ID system** (`src/crates/primitives/src/unified/mod.rs`, `indecies.rs`):
- `AnyTxId(i32)`, `AnyOutId(i64)`, `AnyInId(i64)` — unified IDs that encode whether data is confirmed (dense, `>= 0`) or loose/unconfirmed (sparse, `< 0`).
- Dense IDs are chain-order indices (0-based array offsets). Loose IDs pack a 32-bit tx key (`k32`) into the high 32 bits and a `vout`/`vin` index in the low 32 bits.
- Conversion from dense (`dense::TxId`, `dense::TxOutId`, `dense::TxInId`) or loose types via `From` impls. The `.confirmed_txid()`/`.loose_txid()` accessors route correctly.

**Storage backends**:
- `DenseStorage` — flat arrays backed by prefix-sum boundary arrays (`tx_out_end`, `tx_in_end`, `block_tx_end`). See `docs/datalayout.md` for the on-disk format.
- `InMemoryIndex` (loose) — `HashMap`-based in-memory index for unconfirmed/simulated txs.
- `UnifiedStorage` — facade over both; implements `IndexedGraph` and all index traits.

**Handle types** (`src/crates/primitives/src/handle.rs`):
- `TxHandle<'a>`, `TxOutHandle<'a>`, `TxInHandle<'a>` — ergonomic wrappers that pair an ID with an `&dyn IndexedGraph` reference. Created via `.with(index)` on any `Any*Id`.
- Handles implement the abstract traits (`AbstractTransaction`, `AbstractTxIn`, `AbstractTxOut`) and heuristic helper traits (`EnumerateInputValueInArbitraryOrder`, `HasNLockTime`, etc.).

**Trait system** (`src/crates/primitives/src/traits/`):
- `IndexedGraph` (marker super-trait) composed of `TxIoIndex`, `PrevOutIndex`, `TxInOwnerIndex`, `OutpointIndex`, `TxOutDataIndex`, `ScriptPubkeyIndex`, `TxInIndex`, `TxIndex`.
- `AbstractTransaction` / `AbstractTxIn` / `AbstractTxOut` — abstract over concrete tx types used by heuristics.
- Abstract fingerprint traits (`HasNLockTime`, etc.) in `abstract_fingerprints.rs`.

**Script pubkey indexing**: hashed to `[u8; 20]` (HASH160) stored in `SledScriptPubkeyDb`.

---

### `pipeline`: lazy DSL engine

The pipeline crate provides a typed embedded DSL for composing Bitcoin analysis operations.

**Core types**:
- `Expr<T: ExprValue>` — a typed handle to a node in the expression graph (wraps a `NodeId` + `Arc<PipelineContext>`).
- `PipelineContext` — registry for all nodes; shared via `Arc`.
- `NodeStorage` — stores evaluated results (facts) keyed by `NodeId`.
- `Engine` — runs the evaluation loop; call `engine.run_to_fixpoint()` to evaluate, then `engine.eval(&expr)` to read results.

**Node traits**:
- `Node` — implement this to define a new analysis step. Requires `type OutputValue: ExprValue`, `dependencies() -> Vec<NodeId>`, and `evaluate(&EvalContext) -> Output`.
- `SourceNode` — for nodes that read directly from `UnifiedStorage` (e.g., `AllTxs`). Uses `SourceNodeEvalContext` which exposes cursor positions for incremental processing.
- `AnyNode` / `AnySourceNode` — type-erased blanket impls; used internally for storage in `PipelineContext`.

**Value types** (`value.rs`):
- `ExprValue` — marker trait associating a type tag with a concrete output type.
- Built-in: `TxSet` (→ `HashSet<AnyTxId>`), `TxOutSet` (→ `HashSet<AnyOutId>`), `TxMask` (→ `HashMap<AnyTxId, bool>`), `TxOutMask` (→ `HashMap<AnyOutId, bool>`), `TxOutClustering` (→ `SparseDisjointSet<AnyOutId>`).

**Evaluation**: semi-naive fixpoint iteration. Source nodes produce facts; downstream nodes re-evaluate when any dependency has new output. Cycle-safe via `get_or_default`.

---

### `heuristics`: Bitcoin privacy heuristics

Each heuristic exists in two forms:

1. **Direct / standalone** (e.g., `common_input.rs`, `uih.rs`, `coinjoin_detection.rs`, `change_identification.rs`) — functions/structs operating directly on `AbstractTransaction` and friends.

2. **AST/pipeline nodes** (under `src/crates/heuristics/src/ast/`) — implement `Node` for each heuristic, making them composable in the pipeline DSL. Factories (`IsCoinJoin::new(input)`, `MultiInputHeuristic::new(...)`, etc.) take `Expr<T>` arguments and return `Expr<T>`.

Key pipeline nodes:
- `IsCoinJoinNode` — detects CoinJoin transactions (→ `TxMask`)
- `MultiInputHeuristicNode` — common-input-ownership heuristic (→ `TxOutClustering`)
- `SameAddressClusteringNode` — clusters outputs sharing a script pubkey (→ `TxOutClustering`)
- `UnnecessaryInputHeuristic1Node` / `Unnecessary...2Node` (UIH1/UIH2) — unnecessary input heuristics
- `ChangeIdentificationNode` / `FingerPrintChangeIdentificationNode` — identifies change outputs
- `IsUnilateralNode` / `ChangeClusteringNode` — gates and applies change clustering

---

### `disjoint-set`

- `SparseDisjointSet<K>` — union-find over arbitrary hashable keys, backed by `Arc<RwLock<...>>` (clone-safe, interior-mutable). Supports `.join()` for merging two DSUs (lattice join of equivalence relations).
- `SequentialDisjointSet` — dense array-backed union-find for integer keys.

---

## Conventions

- **Rust 2024 edition**; rustfmt defaults (4-space indent).
- **Naming**: heuristic structs have a factory type (`IsCoinJoin`) and a node type (`IsCoinJoinNode`). The factory's `new()` registers the node and returns an `Expr<T>`.
- **Tests**: colocated with code. AST-layer tests live in `src/crates/heuristics/src/ast/tests.rs`. Run with `cargo test -p tx-indexer-heuristics`.
- **`test-utils` feature**: enables integration testing helpers (`corepc-node`, `node_harness.rs`) in the `primitives` crate. Not compiled in production.
