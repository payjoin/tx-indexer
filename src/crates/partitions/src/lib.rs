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

/// Enumerate all bijections between `ip` (input blocks) and `op` (output
/// blocks) that pair blocks of equal sum. Multiple bijections arise when
/// several blocks share the same sum.
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
fn is_derived(mapping: &Mapping, input_values: &[u64], output_values: &[u64]) -> bool {
    for block in mapping {
        let i_vals: Vec<u64> = block.inputs.iter().map(|&i| input_values[i]).collect();
        let o_vals: Vec<u64> = block.outputs.iter().map(|&i| output_values[i]).collect();

        let full_o_sum: u64 = o_vals.iter().sum();
        let mut o_subsums = all_subsums(&o_vals);
        o_subsums.remove(&full_o_sum);

        let ni = i_vals.len();
        // Enumerate proper non-empty subsets of the input block.
        for mask in 1usize..((1usize << ni).saturating_sub(1)) {
            let s: u64 = (0..ni)
                .filter(|&b| mask & (1 << b) != 0)
                .map(|b| i_vals[b])
                .sum();
            if o_subsums.contains(&s) {
                return true;
            }
        }
    }
    false
}

fn compute_all_mappings(input_values: &[u64], output_values: &[u64]) -> Vec<Mapping> {
    let input_indices: Vec<usize> = (0..input_values.len()).collect();
    let output_indices: Vec<usize> = (0..output_values.len()).collect();

    let output_subsums = all_subsums(output_values);
    let input_subsums = all_subsums(input_values);

    let mut input_partitions: Vec<Vec<(Vec<usize>, u64)>> = vec![];
    find_partitions_dfs(
        &input_indices,
        input_values,
        &output_subsums,
        &mut vec![],
        &mut input_partitions,
    );

    let mut output_partitions: Vec<Vec<(Vec<usize>, u64)>> = vec![];
    find_partitions_dfs(
        &output_indices,
        output_values,
        &input_subsums,
        &mut vec![],
        &mut output_partitions,
    );

    // Group output partitions by sorted sum multiset for O(1) lookup.
    let mut output_by_sums: HashMap<Vec<u64>, Vec<Vec<(Vec<usize>, u64)>>> = HashMap::new();
    for op in output_partitions {
        let mut key: Vec<u64> = op.iter().map(|(_, s)| *s).collect();
        key.sort_unstable();
        output_by_sums.entry(key).or_default().push(op);
    }

    let mut mappings: Vec<Mapping> = vec![];
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
                &mut mappings,
            );
        }
    }
    mappings
}

/// Find all valid mappings of a CoinJoin transaction, including derived ones.
///
/// Implements the partition-finding algorithm (Listing 1, §4) with the
/// optimisations described in §4.3 of Maurer et al., "Anonymous CoinJoin
/// Transactions with Arbitrary Values".
pub fn get_all_mappings<T>(tx: &T) -> Vec<Mapping>
where
    T: EnumerateInputValueInArbitraryOrder + EnumerateOutputValueInArbitraryOrder,
{
    let input_values: Vec<u64> = tx.input_values().map(|a| a.to_sat()).collect();
    let output_values: Vec<u64> = tx.output_values().map(|a| a.to_sat()).collect();
    compute_all_mappings(&input_values, &output_values)
}

/// Find only the non-derived mappings of a CoinJoin transaction.
///
/// A mapping is derived if it can be produced by merging blocks of a finer
/// valid mapping (§4 of the paper). Derived mappings add no information beyond
/// the non-derived ones, so only the non-derived set is needed for analysis.
pub fn get_non_derived_mappings<T>(tx: &T) -> Vec<Mapping>
where
    T: EnumerateInputValueInArbitraryOrder + EnumerateOutputValueInArbitraryOrder,
{
    let input_values: Vec<u64> = tx.input_values().map(|a| a.to_sat()).collect();
    let output_values: Vec<u64> = tx.output_values().map(|a| a.to_sat()).collect();
    let mut mappings = compute_all_mappings(&input_values, &output_values);
    mappings.retain(|m| !is_derived(m, &input_values, &output_values));
    mappings
}

#[cfg(test)]
mod tests {
    use bitcoin::Amount;
    use serde::Deserialize;
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

    /// Type used to deserialise the JSON regression fixtures.
    #[derive(Deserialize)]
    struct Run {
        in_coins: Vec<u64>,
        out_coins: Vec<u64>,
        /// Each entry is (input_partition, output_partition) where a partition
        /// is a list of sets of coin values.
        partition_tuples: Vec<(Vec<Vec<u64>>, Vec<Vec<u64>>)>,
    }

    fn set_eq(a: &[u64], b: &[u64]) -> bool {
        let mut sa = a.to_vec();
        sa.sort_unstable();
        let mut sb = b.to_vec();
        sb.sort_unstable();
        sa == sb
    }

    fn partition_eq(pa: &[Vec<u64>], pb: &[Vec<u64>]) -> bool {
        pa.len() == pb.len() && pa.iter().all(|sa| pb.iter().any(|sb| set_eq(sa, sb)))
    }

    fn mapping_to_value_tuple(
        mapping: &Mapping,
        in_vals: &[u64],
        out_vals: &[u64],
    ) -> (Vec<Vec<u64>>, Vec<Vec<u64>>) {
        let in_part = mapping
            .iter()
            .map(|b| b.inputs.iter().map(|&i| in_vals[i]).collect())
            .collect();
        let out_part = mapping
            .iter()
            .map(|b| b.outputs.iter().map(|&i| out_vals[i]).collect())
            .collect();
        (in_part, out_part)
    }

    fn is_mapping_valid(mapping: &Mapping, inputs: &[u64], outputs: &[u64]) -> bool {
        let mut all_in: Vec<usize> = mapping
            .iter()
            .flat_map(|b| b.inputs.iter().cloned())
            .collect();
        let mut all_out: Vec<usize> = mapping
            .iter()
            .flat_map(|b| b.outputs.iter().cloned())
            .collect();

        all_in.sort_unstable();
        all_out.sort_unstable();

        // Every index appears exactly once and all indices are covered.
        let in_unique = all_in.windows(2).all(|w| w[0] != w[1]);
        let out_unique = all_out.windows(2).all(|w| w[0] != w[1]);
        if !in_unique || !out_unique {
            return false;
        }
        if all_in.len() != inputs.len() || all_out.len() != outputs.len() {
            return false;
        }

        // Each block is balanced.
        mapping.iter().all(|b| {
            let i_sum: u64 = b.inputs.iter().map(|&i| inputs[i]).sum();
            let o_sum: u64 = b.outputs.iter().map(|&i| outputs[i]).sum();
            i_sum == o_sum
        })
    }

    //  Migrated from cja/src/partition/test.rs
    //  https://github.com/payjoin/cja/tree/master/src/partition

    /// Adapted from `test_sum_filtered_partition_iterator`.
    ///
    /// Verifies that `find_partitions_dfs` produces the correct partitions for
    /// the simple case: input set [1, 3, 18] filtered by the subsums of
    /// [3, 4, 19].  The only valid partitions are [[3],[1,18]] and [[1,3,18]].
    #[test]
    fn test_find_partitions() {
        let values = vec![1u64, 3, 18];
        let filter_values = vec![3u64, 4, 19];
        let valid_sums = all_subsums(&filter_values);
        let indices: Vec<usize> = (0..values.len()).collect();

        let mut partitions: Vec<Vec<(Vec<usize>, u64)>> = vec![];
        find_partitions_dfs(&indices, &values, &valid_sums, &mut vec![], &mut partitions);

        // Normalise to sorted value sets for order-independent comparison.
        let mut value_partitions: Vec<Vec<Vec<u64>>> = partitions
            .iter()
            .map(|p| {
                let mut sets: Vec<Vec<u64>> = p
                    .iter()
                    .map(|(idx, _)| {
                        let mut v: Vec<u64> = idx.iter().map(|&i| values[i]).collect();
                        v.sort_unstable();
                        v
                    })
                    .collect();
                sets.sort();
                sets
            })
            .collect();
        value_partitions.sort();
        value_partitions.dedup();

        assert_eq!(value_partitions.len(), 2);
        assert!(value_partitions.contains(&vec![vec![1u64, 18], vec![3u64]]));
        assert!(value_partitions.contains(&vec![vec![1u64, 3, 18]]));
    }

    /// Adapted from `regression_test_sum_filtered_partition_iterator`.
    ///
    /// For each of the three JSON fixture files, verifies that every expected
    /// mapping in `partition_tuples` is present in the output of
    /// `get_all_mappings`.  The total number of expected tuples across all
    /// three files is 30.
    #[test]
    fn regression_test() {
        let files = [
            include_str!("result-none-t-3-s-2-r-1.json"),
            include_str!("result-output-t-3-s-2-r-1.json"),
            include_str!("result-input-t-3-s-2-r-1.json"),
        ];

        let mut counter = 0u64;
        for file in &files {
            let run: Run = serde_json::from_str(file).expect("invalid JSON fixture");
            let tx = MockTx {
                inputs: run.in_coins.clone(),
                tx: DummyTxData::new_with_amounts(run.out_coins.clone()),
            };
            let mappings = get_all_mappings(&tx);
            let computed: Vec<(Vec<Vec<u64>>, Vec<Vec<u64>>)> = mappings
                .iter()
                .map(|m| mapping_to_value_tuple(m, &run.in_coins, &run.out_coins))
                .collect();

            for (exp_in, exp_out) in &run.partition_tuples {
                counter += 1;
                assert!(
                    computed
                        .iter()
                        .any(|(got_in, got_out)| partition_eq(exp_in, got_in)
                            && partition_eq(exp_out, got_out)),
                    "Expected mapping ({:?}, {:?}) not found",
                    exp_in,
                    exp_out
                );
            }
        }
        assert_eq!(counter, 30);
    }

    // Paper examples

    /// Figure 2: two sub-transactions, no mixing.
    /// Alice: i1=21, i2=12 → o1=25, o2=8
    /// Bob:   i3=36, i4=28 → o3=50, o4=14
    /// Expected: exactly 1 non-derived mapping with 2 blocks.
    #[test]
    fn figure_2_unmixed() {
        let outputs = vec![25, 8, 50, 14];
        let tx = MockTx {
            inputs: vec![21, 12, 36, 28],
            tx: DummyTxData::new_with_amounts(outputs.clone()),
        };
        let mappings = get_non_derived_mappings(&tx);
        assert_eq!(mappings.len(), 1);
        assert!(is_mapping_valid(&mappings[0], &tx.inputs, &outputs));
        // The one mapping must have 2 blocks.
        assert_eq!(mappings[0].len(), 2);
    }

    /// Figure 6: same transaction after output splitting.
    /// o3 split into o3.1=31, o3.2=19.
    /// Expected: exactly 2 non-derived mappings.
    #[test]
    fn figure_6_mixed() {
        let outputs = vec![25, 8, 31, 19, 14];
        let tx = MockTx {
            inputs: vec![21, 12, 36, 28],
            tx: DummyTxData::new_with_amounts(outputs.clone()),
        };
        let mappings = get_non_derived_mappings(&tx);
        assert_eq!(mappings.len(), 2);
        for m in &mappings {
            assert!(is_mapping_valid(m, &tx.inputs, &outputs));
        }
    }

    /// Single-user transaction (not a CoinJoin): exactly 1 mapping of 1 block.
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
