//! Peak-RAM probe for the four_counts harness. The counting global allocator is only
//! installed under the `alloc-probe` feature; without it `System` runs unwrapped and
//! `peak_bytes()` stays at zero. Per-primitive peak is only meaningful single-threaded.

use std::sync::atomic::{AtomicUsize, Ordering};

static CURRENT: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
static BASELINE: AtomicUsize = AtomicUsize::new(0);

#[cfg(feature = "alloc-probe")]
pub(crate) struct CountingAllocator;

#[cfg(feature = "alloc-probe")]
unsafe impl std::alloc::GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        let ptr = std::alloc::System.alloc(layout);
        if !ptr.is_null() {
            let now = CURRENT.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            PEAK.fetch_max(now, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: std::alloc::Layout) {
        CURRENT.fetch_sub(layout.size(), Ordering::Relaxed);
        std::alloc::System.dealloc(ptr, layout);
    }
}

/// `peak_bytes()` is relative to a preceding `reset_peak()`: pre-startup frees can wrap
/// `CURRENT`, so the raw totals are not absolute.
pub(crate) fn reset_peak() {
    let cur = CURRENT.load(Ordering::Relaxed);
    BASELINE.store(cur, Ordering::Relaxed);
    PEAK.store(cur, Ordering::Relaxed);
}

pub(crate) fn peak_bytes() -> usize {
    PEAK.load(Ordering::Relaxed)
        .saturating_sub(BASELINE.load(Ordering::Relaxed))
}

#[cfg(all(test, feature = "alloc-probe"))]
mod tests {
    use super::*;

    // The allocator counter is process-global, so parallel test threads can perturb any
    // single reading. Retry a few times and require one attempt to observe the live 8 MiB.
    #[test]
    fn peak_tracks_a_large_live_allocation() {
        for _ in 0..8 {
            reset_peak();
            let v: Vec<u8> = vec![7u8; 8 << 20];
            let peak = peak_bytes();
            assert_eq!(v[0], 7);
            drop(v);
            if peak >= (1 << 20) {
                return; // observed our allocation's growth
            }
        }
        panic!("peak_bytes never reflected an 8 MiB live allocation across 8 attempts");
    }
}
