# ID Families, Abstract Transactions, and Handles

This project models transaction data generically so the pipeline and heuristics can work across multiple ID representations (e.g., loose vs dense).

## ID Families

An `IdFamily` groups the ID types used throughout the system. Specifically an ID family consists of the following types:

- `TxId`: Transaction ID
- `TxInId`: Transaction Input ID
- `TxOutId`: Transaction Output ID

The family lets code stay generic and avoids hardcoding concrete ID types in AST nodes or pipeline ops.

Dense IDs are pointers to the on disk representation of the transaction and its constituent components.
Loose IDs are truncated hashed of the transaction hash. Typically an in memory index will store these loose transactions.

## Abstract Transactions

Abstract transactions are traits that describe something that looks like a Bitcoin transaction. They are parameterized by an `IdFamily`.
They expose operations like:

- iterating inputs/outputs
- resolving output IDs from a tx ID and vout
- retrieving metadata (locktime, scripts, etc.)

Hueristics may only need access to specific property of a transaction. In which case they may use a more granular trait e.g `EnumerateSpentTxOuts`.

## Transaction Handles

Handles are references to transactions or transaction components used in the analysis passes.
They are typed by the same `IdFamily` so that all related values remain consistent.

Any Id type in an IdFamily is convertable to a handle if the correct data storage type is provided (i.e typed to the same `IdFamily`). For example:

```rust
let tx_id: <LooseIds as IdFamily>::TxId = ...;
let index: &dyn IndexedGraph<LooseIds> = ...;
let tx_handle = tx_id.with_index(&index);
let output_len = tx_handle.output_len();
```
