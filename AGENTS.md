# Repository Guidelines

## Project Structure & Module Organization
- Rust workspace with three crates under `src/crates/`: `primitives`, `pipeline`, and `heuristics`.
- Shared workspace configuration in `Cargo.toml` at the repo root.
- Core pipeline logic lives in `src/crates/pipeline/src/`, heuristic AST and rules in `src/crates/heuristics/src/`, and ID/index primitives in `src/crates/primitives/src/`.
- Tests are colocated with code (e.g., `src/crates/heuristics/src/ast/tests.rs`).

## Build, Test, and Development Commands
- `cargo build` builds the entire workspace.
- `cargo test` runs all tests across crates.
- `cargo test -p heuristics` runs the heuristics crate tests only.
- `cargo fmt` formats Rust code using rustfmt.
- `cargo clippy` runs Rust lints for the workspace.

## Coding Style & Naming Conventions
- Rust 2024 edition; follow standard Rust style (4-space indentation, rustfmt defaults).
- Types use `CamelCase`, functions/variables use `snake_case`, and modules mirror file names.
- Prefer generic ID families in pipeline/heuristics APIs (avoid concrete loose/dense types in AST nodes).

## Testing Guidelines
- Tests are written with Rust’s built-in test framework (`#[test]`).
- Place AST-specific tests under `src/crates/heuristics/src/ast/tests.rs`.
- Run targeted tests with `cargo test -p heuristics` during AST changes.

## Commit & Pull Request Guidelines
- Recent commits use short, imperative summaries and often start with `WIP - ...`.
- Keep commit messages concise and scoped to the change.
- For PRs, include a clear description, steps to reproduce/verify, and link related issues if available.
