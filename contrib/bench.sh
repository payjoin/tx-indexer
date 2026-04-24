#!/usr/bin/env bash
set -euo pipefail

# Run all Criterion benchmark suites across the workspace.
#
# Optional env vars:
#   BENCH_FILTER   – Criterion name filter passed via --bench (e.g. BENCH_FILTER=parse)

FILTER="${BENCH_FILTER:-}"

extra_args=()

if [[ -n "$FILTER" ]]; then
    extra_args+=("--bench" "$FILTER")
fi

cargo bench --workspace "${extra_args[@]}"
