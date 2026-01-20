use std::collections::HashMap;

pub trait DisJointSet<K: Eq + std::hash::Hash + Copy> {
    fn find(&mut self, x: K) -> K;
    fn union(&mut self, x: K, y: K);
}

// open question: canonicalizing the parent and child requires the key to impl Ord -- probably fine. But we didnt do it.
// For the vec type we need conversion into usize. Should we create a trait bound for that?
// For "loose" transactions. No sequential order.
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct SparseDisjointSet<K: Eq + std::hash::Hash + Copy>(HashMap<K, K>);

impl<K: Eq + std::hash::Hash + Copy> Default for SparseDisjointSet<K> {
    fn default() -> Self {
        Self(HashMap::new())
    }
}

impl<K: Eq + std::hash::Hash + Copy> DisJointSet<K> for SparseDisjointSet<K> {
    fn find(&mut self, x: K) -> K {
        let parent = *self.0.get(&x).unwrap_or(&x);
        if parent == x {
            return x;
        }
        let root = self.find(parent);
        self.0.insert(x, root);

        root
    }
    fn union(&mut self, x: K, y: K) {
        let x_root = self.find(x);
        let y_root = self.find(y);

        if x_root == y_root {
            return;
        }

        self.0.insert(y_root, x_root);
    }
}

/// For sequentially ordered keys. Keys here are global txout indices.
pub struct SequentialDisjointSet(Vec<usize>);

impl SequentialDisjointSet {
    pub fn new(n: usize) -> Self {
        Self(Vec::from_iter(0..n))
    }
}

impl DisJointSet<usize> for SequentialDisjointSet {
    fn find(&mut self, x: usize) -> usize {
        let parent = self.0[x];
        if parent == x {
            return x;
        }
        let root = self.find(parent);
        self.0[x] = root;
        root
    }

    /// Declares that x and y are in the same subset. Merges the subsets of x and y.
    fn union(&mut self, x: usize, y: usize) {
        let x_root = self.find(x);
        let y_root = self.find(y);

        if x_root == y_root {
            return;
        }

        let (parent, child) = if x_root < y_root {
            (x_root, y_root)
        } else {
            (y_root, x_root)
        };

        self.0[child] = parent;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_union_find() {
        // Singleton case
        assert_eq!(SequentialDisjointSet::new(1).find(0), 0);

        let mut uf = SequentialDisjointSet::new(5);
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

        let mut uf = SequentialDisjointSet::new(5);
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
        assert_eq!(SparseDisjointSet::default().find(0), 0);

        let mut uf = SparseDisjointSet::default();
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

        let mut uf = SparseDisjointSet::default();
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
        let mut uf = SequentialDisjointSet::new(10);
        for i in 0..10 {
            assert_eq!(uf.find(i), i);
        }
    }

    #[test]
    fn test_union_find_sequential_chain() {
        // Test sequential unions forming a chain: 0-1-2-3-4
        let mut uf = SequentialDisjointSet::new(5);
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
        let mut uf = SequentialDisjointSet::new(5);
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
        let mut uf = SequentialDisjointSet::new(5);
        uf.union(2, 2);
        assert_eq!(uf.find(2), 2);

        uf.union(2, 2);
        assert_eq!(uf.find(2), 2);
    }

    #[test]
    fn test_union_find_path_compression() {
        // Test that path compression works correctly
        // Create a chain: 0 <- 1 <- 2 <- 3 <- 4
        let mut uf = SequentialDisjointSet::new(5);
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
        let mut uf = SequentialDisjointSet::new(10);
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
        let mut uf = SequentialDisjointSet::new(10);
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
        let mut uf = SequentialDisjointSet::new(5);
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
        let mut uf = SequentialDisjointSet::new(100);
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
        let mut uf = SequentialDisjointSet::new(10);
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
        let mut uf1 = SequentialDisjointSet::new(5);
        uf1.union(0, 1);
        uf1.union(2, 3);
        uf1.union(1, 2);
        uf1.union(3, 4);

        let mut uf2 = SequentialDisjointSet::new(5);
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
}
