# Unified IDs for confirmed + loose txs

This spec defines a unified 64-bit ID scheme for transactions, outputs, and inputs across:

* **Confirmed chain data** (dense, array-indexed; tx bytes referenced via `blocks.dat` pointers)
* **Loose/unconfirmed data** (sparse, hash-derived; stored in a KV + blob)

It also defines how a storage layer distinguishes ("scrutinizes") IDs and routes reads accordingly.

## Unified ID types

* **Transaction IDs**: `AnyTxId(i32)` -> confirmed dense index or loose u32-derived.
* **Output IDs**: `AnyOutId` -> **64-bit** (u64).
* **Input IDs**: `AnyInId` -> **64-bit** (u64).

### Tagging rule (global invariant)

* `id >= 0`  -> **confirmed / dense**
* `id < 0`   -> **loose / sparse**

## Confirmed (dense) semantics

### Transaction IDs

* `AnyTxId = +TxId`
* `TxId` is the **chain-order transaction index** (0..N-1)

Storage:

* `TxPtr[TxId] -> (blk_file_no, blk_file_off, tx_len)` (fixed-size array)

### Output IDs

* `AnyOutId = +OutId`
* `OutId` is the **chain-order output index** (0..M-1)

Storage (arrays keyed by `OutId`):
Mapping:

* `TxId -> [OutId]` via `tx_out_end` prefix sums

### Input IDs

* `AnyInId = +InId`
* `InId` is the **chain-order input index** (0..K-1)

Storage:

* `in_prevout_outid[InId] -> OutId` (or sentinel for coinbase)
* `TxId -> [InId]` via `tx_in_end` prefix sums

## Loose (sparse) semantics

Loose txs do not exist in `blocks.dat`. They are stored in a KV-backed store. These could be transaction from the mempool, simulations, or other sources.

### Loose Tx key (u32)

Define a 32-bit key for loose txs.

* `k32 = low32( KeyedHash(txid) )` or similar
  (Keyed hash to prevent adversarial collisions; or stable u32 allocation.)

Encoding as unified ID:

* `AnyTxId = -k32`  (always negative; u32 space 0..2^32-1)

### Loose tx storage

Loose txs are stored in either a in memory db or a embedded key value store.
Loose outputs/inputs are represented as **packed negative 64-bit handles** that embed:

* a reference to the loose tx key (`k32`, u32)
* the `vout` / `vin` (index within tx)

With **u32** loose tx keys we have 32 bits for the tx and 32 bits for vout/vin (or a different split). Design options:

### Packed handle format (negative, 64-bit)

We define a 64-bit payload (when interpreted as negative `AnyOutId`/`AnyInId`):

* High 32 bits: loose tx key `k32` (u32)
* Low 32 bits: `vout` or `vin` index
* Optional: high bit or tag for kind (out vs in) if desired, reducing index range

### Loose Output/Input resolution (KV)

Because packed IDs only contain a **prefix** of the tx key, resolution uses KV:

* For a packed `AnyOutId`:

  1. unpack `(k32, vout)` from the 64-bit handle
  2. lookup loose tx by `k32` in KV
  3. resolve to tx bytes and output at `vout`

## Storage-layer scrutiny and routing

Define a storage facade that takes unified IDs and routes to the correct backend:

```text
StorageLayer
  - confirmed: arrays + blocks.dat pointers
  - loose: KVS
```

### Routing rule

For any `Any*Id`:

* if `id >= 0`: confirmed backend
* else: loose backend

### Validation (“scrutiny”) rules

The storage layer MUST reject malformed IDs early:

### Confirmed IDs (`>= 0`)

* `TxId < tx_count`
* `OutId < out_count`
* `InId < in_count`

### Loose IDs (`< 0`)

* unpack payload `(k32, index)`; lookup by `k32` must exist in KV

## Unified graph operations (examples)

### `get_tx_bytes(AnyTxId)`

* confirmed: `TxPtr -> blocks.dat slice`
* loose: `k32 -> ptr -> loose_tx_blob slice`

### `get_tx_outputs(AnyTxId) -> iterator<AnyOutId>`

* confirmed: `tx_out_end` range produces positive `AnyOutId`
* loose: parse tx bytes to get `vout_count`, produce **packed** negative `AnyOutId` (k32 + vout in 64 bits)

### `get_output_spender(AnyOutId) -> Option<AnyInId or AnyTxId>`

* confirmed: `out_spent_by_tx[OutId]`
* loose: either

  * maintain `loose_out_spent_by` in KV (if you track mempool graph), or
  * compute on demand by scanning loose tx set (not recommended)
