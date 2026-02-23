# Confirmed-only Lite Tx Log + Index Spec

This spec assumes **confirmed transactions only** (no loose/mempool), and stores **pointers into `blocks.dat`** instead of raw tx bytes. It defines the minimal on-disk files and the common operations used to query them, including **input↔output linkage**.

## ID spaces

IDs are dense (0..N-1) and index into flat arrays.

* `TxId`: `u32`
  “the *t*-th transaction in chain order”

* `OutId`: `u64`
  “the *o*-th output created in chain order”

* `InId`: `u64`
  “the *i*-th input consumed in chain order”

> Chain order here means: iterate blocks by height, within each block iterate txs in block order, within each tx iterate inputs/outputs in serialization order.

## Common operations (definitions)

### Prefix-sum boundary arrays

We store prefix sums that end each group.

* `tx_out_end[t] = total number of outputs in txs [0..t]` (inclusive)
* `tx_in_end[t]  = total number of inputs  in txs [0..t]` (inclusive)
* `block_tx_end[h] = total number of txs in blocks [0..h]` (inclusive)

From any “*_end” array you can derive a range:

**Range for tx `t` in an `tx_*_end` array**

* `start = (t == 0) ? 0 : end_arr[t-1]`
* `end   = end_arr[t]`
* the members are dense IDs in `[start, end)` (half-open interval)

Example: outputs of tx `t` are `OutId ∈ [out_start(t), out_end(t))`.

### `upper_bound(A, x)`

Given a **non-decreasing** array `A[0..n)`:

`upper_bound(A, x)` returns the **smallest index `i`** such that:

* `A[i] > x`

If no such `i` exists, it returns `n`.

This is the standard way to invert prefix sums:

* `creating_tx(OutId o) = upper_bound(tx_out_end, o)`
* `creating_tx(InId  i) = upper_bound(tx_in_end,  i)`
* `block_of_tx(TxId t)  = upper_bound(block_tx_end, t)`

## On-disk files and layouts

All integers are **little-endian**. Arrays are stored as raw contiguous bytes.

## 3.1 Tx pointer log (the “tx log”)

### File: `confirmed_txptr.bin`

Fixed-size record per `TxId`. Random access by `TxId`.

```rust
struct TxPtr {        
  blk_file_no: u32,    // blkNNNNN.dat number
  blk_file_off: u32,   // offset to start of raw tx bytes within that file
}
```

Access:

* `TxPtr ptr = txptr[TxId]`
* mmap `blk{ptr.blk_file_no}.dat` and read `bytes[ptr.blk_file_off .. ptr.blk_file_off+ptr.tx_len)`

### Block -> Tx boundaries

### File: `block_tx_end.u32`

Array length = number of blocks `H`.

* `block_tx_end[h] = Σ_{k=0..h} tx_count_in_block[k]`

Queries:

* `tx_range_for_block(h)`:

  * `end   = block_tx_end[h]`
  * `start = (h==0 ? 0 : block_tx_end[h-1])`
  * txs are `TxId ∈ [start, end)`

* `block_of_tx(t)`:

  * `h = upper_bound(block_tx_end, t)`

### Tx -> Output boundaries

### File: `tx_out_end.u64`

Array length = number of txs `N`.

* `tx_out_end[t] = Σ_{k=0..t} out_count(tx_k)`

Queries:

* outputs of tx `t`:

  * `out_end   = tx_out_end[t]`
  * `out_start = (t==0 ? 0 : tx_out_end[t-1])`
  * `OutId ∈ [out_start, out_end)`

* creating tx of output `o`:

  * `t = upper_bound(tx_out_end, o)`
  * `vout = o - out_start(t)` (optional derivation)

### Tx -> Input boundaries

### File: `tx_in_end.u64`

Array length = number of txs `N`.

* `tx_in_end[t] = Σ_{k=0..t} in_count(tx_k)`

Queries:

* inputs of tx `t`:

  * `in_end   = tx_in_end[t]`
  * `in_start = (t==0 ? 0 : tx_in_end[t-1])`
  * `InId ∈ [in_start, in_end)`

* creating tx of input `i` (i.e., which tx contains that input):

  * `t = upper_bound(tx_in_end, i)`
  * `vin = i - in_start(t)` (optional derivation)

### Input <-> Output linkage indexes

#### File: `in_prevout_outid.u64`

Array length = total inputs `K = tx_in_end[N-1]`.

* `in_prevout_outid[i] = OutId` that this input spends

Query: for tx `t`, list the outputs it spends:

* for each `InId i` in `[in_start(t), in_end(t))`:

  * `spent_out = in_prevout_outid[i]`

This gives you **(TxId, vin) -> OutId** without reading tx bytes.

> Coinbase inputs: set `in_prevout_outid[i] = OUTID_NONE` (e.g., `u64::MAX`).

### Outputs -> Inputs

* `out_spent_by_inid.u64` length = total outputs `M`

Semantics:

* `out_spent_by_inid[o] = InId` that spends it, or `IN_NONE` (`u64::MAX`)

Query:

* `spender_in = out_spent_by_inid[o]`
* spending tx is `upper_bound(tx_in_end, spender_in)`

Option A is usually more convenient for “jump to spender tx”.

### Minimal additional per-output data (no script hash table)

If you still want basic economics without scripts:

* `out_value.u64` length `M`

(That’s optional for pure graph traversal.)

### Ingest invariants (what gets appended vs updated)

When processing blocks in order:

Append-only

* append `TxPtr` for each tx -> grows `confirmed_txptr.bin`
* append each block’s cumulative tx count -> grows `block_tx_end`
* append each tx’s cumulative in/out totals -> grows `tx_in_end`, `tx_out_end`
* append `in_prevout_outid` entries for each input
* append any per-output columns (e.g., `out_value`)

Mutable

* when you see an input spending some `OutId o_prev`, write:

  * Option A: `out_spent_by_tx[o_prev]=this_tx`, `out_spent_by_vin[o_prev]=vin`
  * Option B: `out_spent_by_inid[o_prev]=this_inid`

## Core queries summary

* **TxId -> outputs**: `tx_out_end` range
* **TxId -> inputs**: `tx_in_end` range
* **Input -> prevout output**: `in_prevout_outid[InId]`
* **Output -> spender**: `out_spent_by_*[OutId]`
* **TxId -> block height**: `upper_bound(block_tx_end, TxId)`
* **TxId -> raw tx bytes**: `TxPtr[TxId]` into `blocks.dat`
