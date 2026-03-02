use std::collections::{HashMap, HashSet};
use tx_indexer_primitives::traits::abstract_types::{
    EnumerateInputValueInArbitraryOrder, EnumerateOutputValueInArbitraryOrder,
};

/// One sub-transaction within a mapping.
#[derive(Clone, Debug)]
pub struct MappingBlock {
    /// Indices into the transaction's input array.
    pub inputs: Vec<usize>,
    /// Indices into the transaction's output array.
    pub outputs: Vec<usize>,
}

/// A complete mapping: a partition of all inputs and all outputs into balanced
/// sub-transactions where each block's input sum equals its output sum.
pub type Mapping = Vec<MappingBlock>;

/// Compute all non-empty subset sums of `values` using DP.
/// This is the precomputation used as the `sums` filter in `find_partitions`.
fn all_subsums(values: &[u64]) -> HashSet<u64> {
    // Start from {0} (empty subset), add each value to all existing sums.
    let mut sums: HashSet<u64> = HashSet::from([0]);
    for &v in values {
        let additions: Vec<u64> = sums.iter().map(|&s| s + v).collect();
        sums.extend(additions);
    }
    sums.remove(&0);
    sums
}

/// Find all partitions of `remaining` indices (into `values`) where every
/// block's sum appears in `valid_sums`.
///
/// Uses a recursive DFS with two optimisations from §4.2 of the paper:
///   1. Early abort — a subset is only explored when its sum is in `valid_sums`.
///   2. Canonical ordering — the smallest remaining index is always placed in
///      the current block, so each partition is generated exactly once.
fn find_partitions_dfs(
    remaining: &[usize],
    values: &[u64],
    valid_sums: &HashSet<u64>,
    current: &mut Vec<(Vec<usize>, u64)>,
    results: &mut Vec<Vec<(Vec<usize>, u64)>>,
) {
    if remaining.is_empty() {
        results.push(current.clone());
        return;
    }

    let first = remaining[0];
    let candidates = &remaining[1..];
    let n = candidates.len();

    // Enumerate all subsets of `candidates` to pair with `first` in one block.
    for mask in 0usize..(1 << n) {
        let mut block = vec![first];
        let mut excluded = vec![];
        for (bit, &idx) in candidates.iter().enumerate() {
            if mask & (1 << bit) != 0 {
                block.push(idx);
            } else {
                excluded.push(idx);
            }
        }

        let sum: u64 = block.iter().map(|&i| values[i]).sum();
        if !valid_sums.contains(&sum) {
            continue; // early abort
        }

        current.push((block, sum));
        find_partitions_dfs(&excluded, values, valid_sums, current, results);
        current.pop();
    }
}

/// Enumerate all bijections between `ip` (input blocks) and `op` (output blocks)
/// that pair blocks of equal sum.  Multiple bijections arise when several blocks
/// share the same sum.
fn build_bijections(
    ip: &[(Vec<usize>, u64)],
    op: &[(Vec<usize>, u64)],
    op_used: &mut Vec<bool>,
    current: &mut Vec<MappingBlock>,
    results: &mut Vec<Mapping>,
) {
    if ip.is_empty() {
        results.push(current.clone());
        return;
    }
    let (i_block, i_sum) = &ip[0];
    for j in 0..op.len() {
        if op_used[j] {
            continue;
        }
        let (o_block, o_sum) = &op[j];
        if o_sum != i_sum {
            continue;
        }
        op_used[j] = true;
        current.push(MappingBlock {
            inputs: i_block.clone(),
            outputs: o_block.clone(),
        });
        build_bijections(&ip[1..], op, op_used, current, results);
        current.pop();
        op_used[j] = false;
    }
}

/// A mapping is *derived* if any of its blocks can be split into two smaller
/// valid sub-transactions — i.e., if a proper non-empty subset of a block's
/// inputs sums to a proper non-empty subset of that block's outputs.
///
/// Derived mappings add no information over the non-derived ones from which
/// they were produced by merging (§5 of the paper).
fn is_derived(mapping: &Mapping, input_values: &[u64], output_values: &[u64]) -> bool {
    for block in mapping {
        let i_vals: Vec<u64> = block.inputs.iter().map(|&i| input_values[i]).collect();
        let o_vals: Vec<u64> = block.outputs.iter().map(|&i| output_values[i]).collect();

        // Proper non-empty subsums of the output block (excluding the full sum).
        let full_o_sum: u64 = o_vals.iter().sum();
        let mut o_subsums = all_subsums(&o_vals);
        o_subsums.remove(&full_o_sum);

        // Check all proper non-empty subsets of the input block.
        let ni = i_vals.len();
        for mask in 1usize..((1usize << ni).saturating_sub(1)) {
            let s: u64 = (0..ni)
                .filter(|&b| mask & (1 << b) != 0)
                .map(|b| i_vals[b])
                .sum();
            if o_subsums.contains(&s) {
                return true; // block is splittable → mapping is derived
            }
        }
    }
    false
}

/// Find all non-derived mappings of a CoinJoin transaction.
///
/// Implements the partition-finding algorithm (Listing 1, §4) with the
/// optimisations described in §4.3 of Maurer et al., "Anonymous CoinJoin
/// Transactions with Arbitrary Values":
///
/// - Input and output partitions are searched separately, each pruned by the
///   set of valid subsums from the opposing side (the HashSet here plays the
///   role of the Bloom filter described in the paper, without false positives).
/// - Partitions are matched by their sum multisets, then all bijections are
///   enumerated.
/// - Derived mappings (those obtainable by merging blocks of a finer mapping)
///   are filtered out.
pub fn get_non_derived_mappings<T>(tx: &T) -> Vec<Mapping>
where
    T: EnumerateInputValueInArbitraryOrder + EnumerateOutputValueInArbitraryOrder,
{
    let input_values: Vec<u64> = tx.input_values().map(|a| a.to_sat()).collect();
    let output_values: Vec<u64> = tx.output_values().map(|a| a.to_sat()).collect();

    let input_indices: Vec<usize> = (0..input_values.len()).collect();
    let output_indices: Vec<usize> = (0..output_values.len()).collect();

    // Precompute cross-side subset sums for pruning.
    let output_subsums = all_subsums(&output_values);
    let input_subsums = all_subsums(&input_values);

    // Find all valid input partitions (blocks whose sums are in output_subsums).
    let mut input_partitions: Vec<Vec<(Vec<usize>, u64)>> = vec![];
    find_partitions_dfs(
        &input_indices,
        &input_values,
        &output_subsums,
        &mut vec![],
        &mut input_partitions,
    );

    // Find all valid output partitions (blocks whose sums are in input_subsums).
    let mut output_partitions: Vec<Vec<(Vec<usize>, u64)>> = vec![];
    find_partitions_dfs(
        &output_indices,
        &output_values,
        &input_subsums,
        &mut vec![],
        &mut output_partitions,
    );

    // Group output partitions by their sorted sum multiset for O(1) lookup.
    let mut output_by_sums: HashMap<Vec<u64>, Vec<Vec<(Vec<usize>, u64)>>> = HashMap::new();
    for op in output_partitions {
        let mut key: Vec<u64> = op.iter().map(|(_, s)| *s).collect();
        key.sort_unstable();
        output_by_sums.entry(key).or_default().push(op);
    }

    // Match each input partition against output partitions with the same sum
    // multiset, then enumerate all valid bijections.
    let mut all_mappings: Vec<Mapping> = vec![];
    for ip in &input_partitions {
        let mut key: Vec<u64> = ip.iter().map(|(_, s)| *s).collect();
        key.sort_unstable();
        let Some(matching_ops) = output_by_sums.get(&key) else {
            continue;
        };
        for op in matching_ops {
            build_bijections(
                ip,
                op,
                &mut vec![false; op.len()],
                &mut vec![],
                &mut all_mappings,
            );
        }
    }

    // Drop derived mappings — those constructable by merging blocks of a finer
    // valid mapping.
    all_mappings.retain(|m| !is_derived(m, &input_values, &output_values));
    all_mappings
}

#[cfg(test)]
mod tests {
    use bitcoin::Amount;
    use tx_indexer_primitives::test_utils::DummyTxData;
    use tx_indexer_primitives::traits::abstract_types::{
        AbstractTransaction, AbstractTxIn, AbstractTxOut, EnumerateInputValueInArbitraryOrder,
        EnumerateOutputValueInArbitraryOrder,
    };

    use super::*;

    // HACK: DummyTxData doesnt impl `EnumerateInputValueInArbitraryOrder` so we use this wrapper.
    struct MockTx {
        inputs: Vec<u64>,
        tx: DummyTxData,
    }

    impl AbstractTransaction for MockTx {
        fn inputs(&self) -> Box<dyn Iterator<Item = Box<dyn AbstractTxIn + '_>> + '_> {
            self.tx.inputs()
        }
        fn outputs(&self) -> Box<dyn Iterator<Item = Box<dyn AbstractTxOut + '_>> + '_> {
            self.tx.outputs()
        }
        fn input_len(&self) -> usize {
            self.inputs.len()
        }
        fn output_len(&self) -> usize {
            self.tx.output_len()
        }
        fn output_at(&self, index: usize) -> Option<Box<dyn AbstractTxOut + '_>> {
            self.tx.output_at(index)
        }
        fn locktime(&self) -> u32 {
            self.tx.locktime()
        }
        fn is_coinbase(&self) -> bool {
            self.tx.is_coinbase()
        }
    }

    impl EnumerateInputValueInArbitraryOrder for MockTx {
        fn input_values(&self) -> impl Iterator<Item = Amount> {
            self.inputs.iter().map(|&v| Amount::from_sat(v))
        }
    }

    impl EnumerateOutputValueInArbitraryOrder for MockTx {
        fn output_values(&self) -> impl Iterator<Item = Amount> {
            self.tx.output_values()
        }
    }

    fn is_mapping_valid(mapping: &Mapping, inputs: &[u64], outputs: &[u64]) -> bool {
        let all_inputs: Vec<usize> = mapping
            .iter()
            .flat_map(|b| b.inputs.iter().cloned())
            .collect();
        let all_outputs: Vec<usize> = mapping
            .iter()
            .flat_map(|b| b.outputs.iter().cloned())
            .collect();

        // Every input and output index appears exactly once.
        let mut sorted_inputs = all_inputs.clone();
        sorted_inputs.sort_unstable();
        sorted_inputs.dedup();
        if sorted_inputs.len() != all_inputs.len() || sorted_inputs.len() != inputs.len() {
            return false;
        }

        let mut sorted_outputs = all_outputs.clone();
        sorted_outputs.sort_unstable();
        sorted_outputs.dedup();
        if sorted_outputs.len() != all_outputs.len() || sorted_outputs.len() != outputs.len() {
            return false;
        }

        // Each block is balanced.
        mapping.iter().all(|b| {
            let i_sum: u64 = b.inputs.iter().map(|&i| inputs[i]).sum();
            let o_sum: u64 = b.outputs.iter().map(|&i| outputs[i]).sum();
            i_sum == o_sum
        })
    }

    /// Figure 2 from the paper: two sub-transactions, no mixing applied.
    /// Alice: i1=21, i2=12 → o1=25, o2=8
    /// Bob:   i3=36, i4=28 → o3=50, o4=14
    /// Expected: exactly 1 non-derived mapping.
    #[test]
    fn figure_2_unmixed() {
        let outputs = vec![25, 8, 50, 14];
        let tx = MockTx {
            inputs: vec![21, 12, 36, 28],
            tx: DummyTxData::new_with_amounts(outputs.clone()),
        };
        let mappings = get_non_derived_mappings(&tx);
        println!("{:?}", mappings);
        println!("{:?}", mappings.len());
        assert_eq!(mappings.len(), 1);
        assert!(is_mapping_valid(&mappings[0], &tx.inputs, &outputs));
        // The one mapping must have 2 blocks.
        assert_eq!(mappings[0].len(), 2);
    }

    /// Figure 6 from the paper: same transaction after output splitting.
    /// o3 has been split into o3.1=31 and o3.2=19.
    /// Expected: exactly 2 non-derived mappings.
    #[test]
    fn figure_6_mixed() {
        let outputs = vec![25, 8, 31, 19, 14];
        let tx = MockTx {
            inputs: vec![21, 12, 36, 28],
            tx: DummyTxData::new_with_amounts(outputs.clone()),
        };
        let mappings = get_non_derived_mappings(&tx);
        println!("{:?}", mappings.len());
        assert_eq!(mappings.len(), 2);
        for m in &mappings {
            assert!(is_mapping_valid(m, &tx.inputs, &outputs));
        }
    }

    /// Single sub-transaction (not a CoinJoin) — should have exactly 1 mapping.
    #[test]
    fn single_subtransaction() {
        let tx = MockTx {
            inputs: vec![100, 50],
            tx: DummyTxData::new_with_amounts(vec![80, 70]),
        };
        let mappings = get_non_derived_mappings(&tx);
        assert_eq!(mappings.len(), 1);
        assert_eq!(mappings[0].len(), 1);
    }
}
