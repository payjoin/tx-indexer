use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    sync::{Arc, RwLock},
};

pub trait DisJointSet<K: Eq + Hash + Copy> {
    fn find(&self, x: K) -> K;
    fn union(&self, x: K, y: K) -> bool; // true if merged
}

#[derive(Clone, Debug)]
struct Inner<K: Eq + Hash + Copy> {
    // key: element, value: parent
    parent: HashMap<K, K>,
    // key: element, value: rank
    // rank is the height of the tree
    rank: HashMap<K, u32>,
}

impl<K: Eq + Hash + Copy> Default for Inner<K> {
    fn default() -> Self {
        Self {
            parent: HashMap::new(),
            rank: HashMap::new(),
        }
    }
}

#[derive(Clone)]
pub struct SparseDisjointSet<K: Eq + Hash + Copy>(Arc<RwLock<Inner<K>>>);

impl<K: Eq + Hash + Copy> Default for SparseDisjointSet<K> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Eq + Hash + Copy> SparseDisjointSet<K> {
    pub fn new() -> Self {
        Self(Arc::new(RwLock::new(Inner::default())))
    }

    pub fn is_empty(&self) -> bool {
        let g = self.0.read().expect("poisoned lock");
        g.parent.is_empty()
    }

    /// Ensure element exists as a singleton set (x is its own parent).
    fn make_set(inner: &mut Inner<K>, x: K) {
        inner.parent.entry(x).or_insert(x);
        inner.rank.entry(x).or_insert(0);
    }

    fn find_in(inner: &mut Inner<K>, x: K) -> K {
        Self::make_set(inner, x);

        // Find root
        let mut cur = x;
        while inner.parent[&cur] != cur {
            cur = inner.parent[&cur];
        }
        let root = cur;

        // Path compression
        let mut cur = x;
        while inner.parent[&cur] != cur {
            let p = inner.parent[&cur];
            inner.parent.insert(cur, root);
            cur = p;
        }

        root
    }

    /// Snapshot parent pointers for off-lock processing.
    fn snapshot(&self) -> (HashMap<K, K>, HashMap<K, u32>) {
        let g = self.0.read().expect("poisoned lock");
        (g.parent.clone(), g.rank.clone())
    }

    /// Partition join (lattice join): the coarsest partition implied by either DSU.
    ///
    /// Equivalent: take equivalence relations from both DSUs and union them together.
    pub fn join(&self, other: &Self) -> Self {
        // Snapshot both, so we don't hold locks while doing work.
        let (mut p1, _r1) = self.snapshot();
        let (mut p2, _r2) = other.snapshot();

        if p1.is_empty() {
            return other.clone();
        }

        if p2.is_empty() {
            return self.clone();
        }

        // Collect universe of elements mentioned by either DSU (keys and values).
        let mut universe: HashSet<K> = HashSet::new();
        universe.extend(p1.keys().copied());
        universe.extend(p1.values().copied());
        universe.extend(p2.keys().copied());
        universe.extend(p2.values().copied());

        let out = Self::new();

        // Helper: local find with compression on a local parent map.
        fn local_find<K: Eq + Hash + Copy>(parent: &mut HashMap<K, K>, x: K) -> K {
            parent.entry(x).or_insert(x);

            // root
            let mut cur = x;
            while parent[&cur] != cur {
                cur = parent[&cur];
            }
            let root = cur;

            // compress
            let mut cur = x;
            while parent[&cur] != cur {
                let p = parent[&cur];
                parent.insert(cur, root);
                cur = p;
            }
            root
        }

        // Initialize all elements in out as singleton sets (optional, union will also do it).
        {
            let mut g = out.0.write().expect("poisoned lock");
            for x in universe.iter().copied() {
                Self::make_set(&mut g, x);
            }
        }

        // For each element x, union x with its representative in partition 1 and partition 2.
        //
        // This is sufficient because: if a ~ b in a partition, then find(x) is constant on
        // the whole block, so unioning each element to its rep recreates the partition.
        for &x in universe.iter() {
            let rep1 = local_find(&mut p1, x);
            let rep2 = local_find(&mut p2, x);

            out.union(x, rep1);
            out.union(x, rep2);
        }

        out
    }

    /// Inspect current parent pointer
    pub fn parent_of(&self, x: K) -> K {
        let g = self.0.read().expect("poisoned lock");
        g.parent.get(&x).copied().unwrap_or(x)
    }

    // Get all elements in the set (share the same root)
    pub fn iter_set(&self, x: K) -> impl Iterator<Item = K> {
        let root = self.find(x);
        // Need to find all keys that share the same value.
        let g = self.0.read().expect("poisoned lock");
        let elements = g.parent.keys().copied().collect::<HashSet<_>>();
        drop(g);
        elements
            .into_iter()
            .filter(move |v| self.parent_of(*v) == root)
    }
    /// Get all parent ids
    pub fn iter_parent_ids(&self) -> impl Iterator<Item = K> {
        let g = self.0.read().expect("poisoned lock");
        g.parent
            .values()
            .copied()
            .collect::<HashSet<_>>()
            .into_iter()
    }
}

impl<K: Eq + Hash + Copy> DisJointSet<K> for SparseDisjointSet<K> {
    fn find(&self, x: K) -> K {
        let mut g = self.0.write().expect("poisoned lock");
        Self::find_in(&mut g, x)
    }

    fn union(&self, x: K, y: K) -> bool {
        let mut g = self.0.write().expect("poisoned lock");
        let rx = Self::find_in(&mut g, x);
        let ry = Self::find_in(&mut g, y);

        if rx == ry {
            return false;
        }

        let rank_x = *g.rank.get(&rx).unwrap_or(&0);
        let rank_y = *g.rank.get(&ry).unwrap_or(&0);

        // Union by rank
        if rank_x < rank_y {
            g.parent.insert(rx, ry);
        } else if rank_x > rank_y {
            g.parent.insert(ry, rx);
        } else {
            g.parent.insert(ry, rx);
            g.rank.insert(rx, rank_x + 1);
        }

        true
    }
}

impl<K: Eq + Hash + Copy> Eq for SparseDisjointSet<K> {}

impl<K: Eq + Hash + Copy> PartialEq for SparseDisjointSet<K> {
    fn eq(&self, other: &Self) -> bool {
        // FIXME:
        let s = self.0.read().unwrap();
        let o = other.0.read().unwrap();
        s.parent == o.parent && s.rank == o.rank
    }
}

// / For sequentially ordered keys. Keys here are global txout indices.
pub struct SequentialDisjointSet(Arc<RwLock<Vec<usize>>>);

impl SequentialDisjointSet {
    pub fn new(n: usize) -> Self {
        Self(Arc::new(RwLock::new(Vec::from_iter(0..n))))
    }
}

impl DisJointSet<usize> for SequentialDisjointSet {
    fn find(&self, x: usize) -> usize {
        let parent = self.0.read().unwrap()[x];
        if parent == x {
            return x;
        }
        let root = self.find(parent);
        self.0.write().unwrap()[x] = root;
        root
    }

    /// Declares that x and y are in the same subset. Merges the subsets of x and y.
    fn union(&self, x: usize, y: usize) -> bool {
        let x_root = self.find(x);
        let y_root = self.find(y);

        if x_root == y_root {
            return false;
        }

        let (parent, child) = if x_root < y_root {
            (x_root, y_root)
        } else {
            (y_root, x_root)
        };

        self.0.write().unwrap()[child] = parent;
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_union_find() {
        // Singleton case
        assert_eq!(SequentialDisjointSet::new(1).find(0), 0);

        let uf = SequentialDisjointSet::new(5);
        uf.union(0, 2);
        assert_eq!(uf.find(0), 0);
        assert_eq!(uf.find(1), 1);
        assert_eq!(uf.find(2), 0);
        assert_eq!(uf.find(3), 3);
        assert_eq!(uf.find(4), 4);

        uf.union(4, 2);
        assert_eq!(uf.find(0), 0);
        assert_eq!(uf.find(1), 1);
        assert_eq!(uf.find(2), 0);
        assert_eq!(uf.find(3), 3);
        assert_eq!(uf.find(4), 0);

        uf.union(3, 1);
        assert_eq!(uf.find(0), 0);
        assert_eq!(uf.find(1), 1);
        assert_eq!(uf.find(2), 0);
        assert_eq!(uf.find(3), 1);
        assert_eq!(uf.find(4), 0);

        uf.union(3, 4);
        assert_eq!(uf.find(0), 0);
        assert_eq!(uf.find(1), 0);
        assert_eq!(uf.find(2), 0);
        assert_eq!(uf.find(3), 0);
        assert_eq!(uf.find(4), 0);

        let uf = SequentialDisjointSet::new(5);
        uf.union(0, 2);
        uf.union(4, 2);
        uf.union(3, 1);
        uf.union(3, 4);
        assert_eq!(uf.find(0), 0);
        assert_eq!(uf.find(1), 0);
        assert_eq!(uf.find(2), 0);
        assert_eq!(uf.find(3), 0);
        assert_eq!(uf.find(4), 0);
    }

    #[test]
    fn test_sparse_union_find() {
        // Singleton case
        assert_eq!(SparseDisjointSet::new().find(0), 0);

        let uf = SparseDisjointSet::new();
        uf.union(0, 2);
        assert_eq!(uf.find(0), uf.find(2));
        assert_eq!(uf.find(1), uf.find(1));
        assert_eq!(uf.find(2), uf.find(0));
        assert_eq!(uf.find(3), uf.find(3));
        assert_eq!(uf.find(4), uf.find(4));

        uf.union(4, 2);
        assert_eq!(uf.find(0), uf.find(0));
        assert_eq!(uf.find(1), uf.find(1));
        assert_eq!(uf.find(2), uf.find(0));
        assert_eq!(uf.find(3), uf.find(3));
        assert_eq!(uf.find(4), uf.find(0));

        uf.union(3, 1);
        assert_eq!(uf.find(0), uf.find(0));
        assert_eq!(uf.find(1), uf.find(1));
        assert_eq!(uf.find(2), uf.find(0));
        assert_eq!(uf.find(3), uf.find(1));
        assert_eq!(uf.find(4), uf.find(0));

        uf.union(3, 4);
        assert_eq!(uf.find(0), uf.find(0));
        assert_eq!(uf.find(1), uf.find(0));
        assert_eq!(uf.find(2), uf.find(0));
        assert_eq!(uf.find(3), uf.find(0));
        assert_eq!(uf.find(4), uf.find(0));

        let uf = SparseDisjointSet::new();
        uf.union(0, 2);
        uf.union(4, 2);
        uf.union(3, 1);
        uf.union(3, 4);
        assert_eq!(uf.find(0), uf.find(0));
        assert_eq!(uf.find(1), uf.find(0));
        assert_eq!(uf.find(2), uf.find(0));
        assert_eq!(uf.find(3), uf.find(0));
        assert_eq!(uf.find(4), uf.find(0));
    }

    #[test]
    fn test_union_find_no_unions() {
        // Test that all elements remain separate when no unions are performed
        let uf = SequentialDisjointSet::new(10);
        for i in 0..10 {
            assert_eq!(uf.find(i), i);
        }
    }

    #[test]
    fn test_union_find_sequential_chain() {
        // Test sequential unions forming a chain: 0-1-2-3-4
        let uf = SequentialDisjointSet::new(5);
        uf.union(0, 1);
        uf.union(1, 2);
        uf.union(2, 3);
        uf.union(3, 4);

        // All should have root 0
        for i in 0..5 {
            assert_eq!(uf.find(i), 0);
        }
    }

    #[test]
    fn test_union_find_idempotent_union() {
        // Test that unioning the same pair multiple times is idempotent
        let uf = SequentialDisjointSet::new(5);
        uf.union(1, 2);
        uf.union(1, 2);
        uf.union(2, 1);
        uf.union(1, 2);

        assert_eq!(uf.find(1), 1);
        assert_eq!(uf.find(2), 1);
    }

    #[test]
    fn test_union_find_union_with_self() {
        // Test that unioning an element with itself is idempotent
        let uf = SequentialDisjointSet::new(5);
        uf.union(2, 2);
        assert_eq!(uf.find(2), 2);

        uf.union(2, 2);
        assert_eq!(uf.find(2), 2);
    }

    #[test]
    fn test_union_find_path_compression() {
        // Test that path compression works correctly
        // Create a chain: 0 <- 1 <- 2 <- 3 <- 4
        let uf = SequentialDisjointSet::new(5);
        uf.union(0, 1);
        uf.union(1, 2);
        uf.union(2, 3);
        uf.union(3, 4);

        // First find should compress the path
        assert_eq!(uf.find(4), 0);
        // Subsequent finds should be fast (path already compressed)
        assert_eq!(uf.find(4), 0);
        assert_eq!(uf.find(3), 0);
        assert_eq!(uf.find(2), 0);
        assert_eq!(uf.find(1), 0);
    }

    #[test]
    fn test_union_find_star_pattern() {
        // Test star pattern: all elements connected to center
        let uf = SequentialDisjointSet::new(10);
        for i in 1..10 {
            uf.union(0, i);
        }

        // All should have root 0
        for i in 0..10 {
            assert_eq!(uf.find(i), 0);
        }
    }

    #[test]
    fn test_union_find_two_groups() {
        // Test two separate groups that never merge
        let uf = SequentialDisjointSet::new(10);
        // Group 1: 0, 1, 2, 3, 4
        for i in 1..5 {
            uf.union(0, i);
        }
        // Group 2: 5, 6, 7, 8, 9
        for i in 6..10 {
            uf.union(5, i);
        }

        // Group 1 should all have root 0
        for i in 0..5 {
            assert_eq!(uf.find(i), 0);
        }
        // Group 2 should all have root 5
        for i in 5..10 {
            assert_eq!(uf.find(i), 5);
        }
    }

    #[test]
    fn test_union_find_transitivity() {
        // Test transitivity: if A is connected to B and B to C, then A is connected to C
        let uf = SequentialDisjointSet::new(5);
        uf.union(0, 1);
        uf.union(1, 2);
        // Without directly unioning 0 and 2, they should be connected
        assert_eq!(uf.find(0), uf.find(2));

        uf.union(3, 4);
        // 3 and 4 should be connected, but not to 0, 1, 2
        assert_eq!(uf.find(3), uf.find(4));
        assert_ne!(uf.find(0), uf.find(3));
    }

    #[test]
    fn test_union_find_large_set() {
        // Test with a larger set
        let uf = SequentialDisjointSet::new(100);
        // Create groups of 10
        for group in 0..10 {
            let base = group * 10;
            for i in 1..10 {
                uf.union(base, base + i);
            }
        }

        // Verify each group is internally connected
        for group in 0..10 {
            let base = group * 10;
            let root = uf.find(base);
            for i in 1..10 {
                assert_eq!(uf.find(base + i), root);
            }
        }

        // Verify groups are separate
        assert_ne!(uf.find(0), uf.find(10));
        assert_ne!(uf.find(20), uf.find(30));
    }

    #[test]
    fn test_union_find_merge_groups() {
        // Test merging two existing groups
        let uf = SequentialDisjointSet::new(10);
        // Create group 1: 0-4
        for i in 1..5 {
            uf.union(0, i);
        }
        // Create group 2: 5-9
        for i in 6..10 {
            uf.union(5, i);
        }

        // Merge the two groups
        uf.union(2, 7);

        // Now all should be connected (root should be 0 since 0 < 5)
        for i in 0..10 {
            assert_eq!(uf.find(i), 0);
        }
    }

    #[test]
    fn test_union_find_reverse_order() {
        // Test that union order doesn't matter for final result
        let uf1 = SequentialDisjointSet::new(5);
        uf1.union(0, 1);
        uf1.union(2, 3);
        uf1.union(1, 2);
        uf1.union(3, 4);

        let uf2 = SequentialDisjointSet::new(5);
        uf2.union(4, 3);
        uf2.union(3, 2);
        uf2.union(2, 1);
        uf2.union(1, 0);

        // Both should result in all elements connected
        for i in 0..5 {
            assert_eq!(uf1.find(i), uf1.find(0));
            assert_eq!(uf2.find(i), uf2.find(0));
        }
    }

    #[test]
    fn test_join_empty_sets() {
        // Test joining two empty DSUs
        let dsu1 = SparseDisjointSet::new();
        let dsu2 = SparseDisjointSet::new();
        let joined = dsu1.join(&dsu2);

        // Should still be empty (no elements added)
        assert_eq!(joined.find(0), 0);
    }

    #[test]
    fn test_join_empty_with_non_empty() {
        // Test joining an empty DSU with a non-empty one
        let dsu1 = SparseDisjointSet::new();
        dsu1.union(1, 2);
        dsu1.union(2, 3);

        let dsu2 = SparseDisjointSet::new();
        let joined = dsu1.join(&dsu2);

        // The joined set should preserve the structure of dsu1
        assert_eq!(joined.find(1), joined.find(2));
        assert_eq!(joined.find(2), joined.find(3));
        assert_eq!(joined.find(1), joined.find(3));
    }

    #[test]
    fn test_join_disjoint_sets() {
        // Test joining two DSUs with completely disjoint sets
        let dsu1 = SparseDisjointSet::new();
        dsu1.union(1, 2);
        dsu1.union(2, 3);

        let dsu2 = SparseDisjointSet::new();
        dsu2.union(4, 5);
        dsu2.union(5, 6);

        let joined = dsu1.join(&dsu2);

        // Each DSU's internal structure should be preserved
        assert_eq!(joined.find(1), joined.find(2));
        assert_eq!(joined.find(2), joined.find(3));
        assert_eq!(joined.find(4), joined.find(5));
        assert_eq!(joined.find(5), joined.find(6));

        // But sets from different DSUs should remain separate
        assert_ne!(joined.find(1), joined.find(4));
    }

    #[test]
    fn test_join_overlapping_sets() {
        // Test joining two DSUs with overlapping elements
        let dsu1 = SparseDisjointSet::new();
        dsu1.union(1, 2);
        dsu1.union(2, 3);

        let dsu2 = SparseDisjointSet::new();
        dsu2.union(3, 4);
        dsu2.union(4, 5);

        let joined = dsu1.join(&dsu2);

        // Since element 3 is in both, and it's connected to different groups in each,
        // the join should merge all elements together
        assert_eq!(joined.find(1), joined.find(2));
        assert_eq!(joined.find(2), joined.find(3));
        assert_eq!(joined.find(3), joined.find(4));
        assert_eq!(joined.find(4), joined.find(5));
        // All should be in the same set
        assert_eq!(joined.find(1), joined.find(5));
    }

    #[test]
    fn test_join_commutative() {
        // Test that join is commutative (join(a, b) has same equivalence classes as join(b, a))
        let dsu1 = SparseDisjointSet::new();
        dsu1.union(1, 2);
        dsu1.union(3, 4);

        let dsu2 = SparseDisjointSet::new();
        dsu2.union(2, 3);
        dsu2.union(5, 6);

        let joined1 = dsu1.join(&dsu2);
        let joined2 = dsu2.join(&dsu1);

        // Check that equivalence classes are the same
        // All elements 1-4 should be connected in both
        assert_eq!(joined1.find(1), joined1.find(4));
        assert_eq!(joined2.find(1), joined2.find(4));

        // Elements 5-6 should be connected in both
        assert_eq!(joined1.find(5), joined1.find(6));
        assert_eq!(joined2.find(5), joined2.find(6));

        // But 1-4 and 5-6 should be separate in both
        assert_ne!(joined1.find(1), joined1.find(5));
        assert_ne!(joined2.find(1), joined2.find(5));
    }

    #[test]
    fn test_join_same_structure() {
        // Test joining two DSUs with the same structure
        let dsu1 = SparseDisjointSet::new();
        dsu1.union(1, 2);
        dsu1.union(3, 4);

        let dsu2 = SparseDisjointSet::new();
        dsu2.union(1, 2);
        dsu2.union(3, 4);

        let joined = dsu1.join(&dsu2);

        // Should preserve the same structure
        assert_eq!(joined.find(1), joined.find(2));
        assert_eq!(joined.find(3), joined.find(4));
        assert_ne!(joined.find(1), joined.find(3));
    }

    #[test]
    fn test_join_complex_overlap() {
        // complex scenario with multiple overlapping groups
        let dsu1 = SparseDisjointSet::new();
        dsu1.union(1, 2);
        dsu1.union(3, 4);
        dsu1.union(5, 6);

        let dsu2 = SparseDisjointSet::new();
        dsu2.union(2, 3);
        dsu2.union(4, 5);

        let joined = dsu1.join(&dsu2);

        // All elements 1-6 should be connected through the chain
        assert_eq!(joined.find(1), joined.find(2));
        assert_eq!(joined.find(2), joined.find(3));
        assert_eq!(joined.find(3), joined.find(4));
        assert_eq!(joined.find(4), joined.find(5));
        assert_eq!(joined.find(5), joined.find(6));
        assert_eq!(joined.find(1), joined.find(6));
    }

    #[test]
    fn test_join_preserves_original_sets() {
        // Test that join doesn't modify the original DSUs
        let dsu1 = SparseDisjointSet::new();
        dsu1.union(1, 2);

        let dsu2 = SparseDisjointSet::new();
        dsu2.union(3, 4);

        let root1_before = dsu1.find(1);
        let root2_before = dsu2.find(3);

        let _joined = dsu1.join(&dsu2);

        // Original sets should be unchanged
        assert_eq!(dsu1.find(1), root1_before);
        assert_eq!(dsu1.find(2), root1_before);
        assert_eq!(dsu2.find(3), root2_before);
        assert_eq!(dsu2.find(4), root2_before);
    }

    #[test]
    fn test_join_with_empty() {
        // Test that join doesn't modify the original DSUs
        let dsu1 = SparseDisjointSet::new();
        dsu1.union(1, 2);

        let dsu2 = SparseDisjointSet::new();

        let joined = dsu1.join(&dsu2);

        // Original sets should be unchanged
        assert_eq!(joined.find(1), dsu1.find(1));
        assert_eq!(joined.find(2), dsu1.find(2));
    }
}
