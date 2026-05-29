//! Scoped-thread parallel map — rayon-free work-stealing helper.
//!
//! Replaces lbzip2's old dedicated `rayon::ThreadPool`. Spawns a fixed
//! pool of scoped threads (sized to physical cores, overridable via
//! `LBZIP2_THREADS`) that dynamically claim work items from `0..n`, so a
//! few cheap items and a few expensive ones still balance across cores.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

/// Worker budget: `LBZIP2_THREADS` if set, else available parallelism.
///
/// Separate from any caller pool so bzip2 decode doesn't oversubscribe.
pub fn worker_count() -> usize {
    std::env::var("LBZIP2_THREADS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4)
        })
        .max(1)
}

/// Apply `f` to every index in `0..n` in parallel, returning the results
/// in index order.
pub fn par_map<T, F>(n: usize, f: F) -> Vec<T>
where
    T: Send,
    F: Fn(usize) -> T + Sync,
{
    if n == 0 {
        return Vec::new();
    }
    let threads = worker_count().min(n);
    if threads <= 1 {
        return (0..n).map(f).collect();
    }

    let next = AtomicUsize::new(0);
    let slots: Vec<Mutex<Option<T>>> = (0..n).map(|_| Mutex::new(None)).collect();
    let next_ref = &next;
    let slots_ref = &slots;
    let f_ref = &f;

    std::thread::scope(|s| {
        for _ in 0..threads {
            s.spawn(move || loop {
                let i = next_ref.fetch_add(1, Ordering::Relaxed);
                if i >= n {
                    break;
                }
                let v = f_ref(i);
                *slots_ref[i].lock().unwrap() = Some(v);
            });
        }
    });

    slots
        .into_iter()
        .map(|m| m.into_inner().unwrap().expect("slot filled"))
        .collect()
}
