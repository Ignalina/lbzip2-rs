//! CLI parallel bzip2 decompressor — pure Rust, all available cores.
//!
//! Usage: lbunzip2 <input.bz2> [output]
//!   If output is omitted, strips .bz2 extension.
//!
//! Worker-pool pipeline with 6-slot ring buffer:
//!
//!   Reader ──→ Main (carry+split) ──→ 32 Workers ──→ Collector ──→ Writer
//!     ↑                                                    │
//!     └──────────── slot recycle ──────────────────────────┘
//!
//! Main thread posts work immediately after splitting — no barrier.
//! Workers flow freely across slots. Collector writes slots in order.

use std::{
    fs::File,
    io::{BufReader, BufWriter, Read, Write},
    sync::mpsc,
    time::Instant,
};

const CHUNK_SIZE: usize = 200 * 1024 * 1024;
const BUF_CAP: usize = 4 * 1024 * 1024;
const RING_SLOTS: usize = 6;
/// Headroom at the start of each slot for carry data.
const CARRY_HEADROOM: usize = 32 * 1024 * 1024;
const SLOT_SIZE: usize = CARRY_HEADROOM + CHUNK_SIZE;

/// Detect physical (non-SMT) core count via Linux sysfs topology.
/// Falls back to available_parallelism() on non-Linux or if sysfs is unavailable.
fn physical_cores() -> Option<usize> {
    #[cfg(target_os = "linux")]
    {
        use std::collections::HashSet;
        let mut ids = HashSet::new();
        for entry in std::fs::read_dir("/sys/devices/system/cpu").ok()? {
            let entry = entry.ok()?;
            let name = entry.file_name();
            let name = name.to_str()?;
            if !name.starts_with("cpu") || !name[3..].chars().next()?.is_ascii_digit() {
                continue;
            }
            let base = entry.path().join("topology");
            let pkg = std::fs::read_to_string(base.join("physical_package_id"))
                .ok()
                .and_then(|s| s.trim().parse::<u32>().ok())
                .unwrap_or(0);
            if let Ok(s) = std::fs::read_to_string(base.join("core_id")) {
                if let Ok(core) = s.trim().parse::<u32>() {
                    ids.insert((pkg, core));
                }
            }
        }
        if !ids.is_empty() {
            return Some(ids.len());
        }
    }
    std::thread::available_parallelism().map(|n| n.get()).ok()
}

fn read_chunk(reader: &mut impl Read, buf: &mut [u8]) -> usize {
    let mut got = 0;
    while got < buf.len() {
        match reader.read(&mut buf[got..]) {
            Ok(0) => break,
            Ok(k) => got += k,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => {
                eprintln!("read error: {e}");
                break;
            }
        }
    }
    got
}

/// Work item sent to a worker thread.
struct WorkItem {
    chunk_id: u64,
    segment_id: usize,
    /// Raw pointer to the beginning of the slot data (carry + read data).
    data_ptr: *const u8,
    data_len: usize,
    start_bit: u64,
    end_bit: u64,
    max_blocksize: u32,
}

// Safety: data_ptr points into a slot that stays alive until all workers
// finish with this chunk_id (enforced by collector before recycling).
unsafe impl Send for WorkItem {}

/// Result from a worker.
struct SegmentResult {
    chunk_id: u64,
    segment_id: usize,
    output: Vec<u8>,
}

/// Messages to the collector — single channel, no polling.
enum CollectorMsg {
    NewSlot(InFlightSlot),
    Result(SegmentResult),
}

/// Tracks in-flight slot state for the collector.
struct InFlightSlot {
    chunk_id: u64,
    slot: Vec<u8>,
    decode_segments: usize,
    results: Vec<Option<Vec<u8>>>,
    done_count: usize,
    is_last: bool,
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("usage: lbunzip2 <input.bz2> [output]");
        std::process::exit(1);
    }

    let input_path = &args[1];
    let output_path = if args.len() > 2 {
        args[2].clone()
    } else if input_path.ends_with(".bz2") {
        input_path[..input_path.len() - 4].to_string()
    } else {
        format!("{input_path}.out")
    };

    let in_file = File::open(input_path).expect("open input");
    let in_size = in_file.metadata().map(|m| m.len()).unwrap_or(0);
    let mut reader = BufReader::with_capacity(BUF_CAP, in_file);

    // Read bz2 header (4 bytes: "BZhN")
    let mut header = [0u8; 4];
    reader.read_exact(&mut header).expect("read bz2 header");

    let bz2_level = header[3];
    if &header[..2] != b"BZ" || header[2] != b'h' || !(b'1'..=b'9').contains(&bz2_level) {
        eprintln!("invalid bzip2 header");
        std::process::exit(1);
    }
    let max_blocksize = 100_000 * (bz2_level - b'0') as u32;

    let n_workers: usize = std::env::var("LBZIP2_THREADS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| physical_cores().unwrap_or(4));

    eprintln!(
        "lbunzip2: {} ({} MB) → {}  [{} workers, {} slots]",
        input_path,
        in_size / (1024 * 1024),
        output_path,
        n_workers,
        RING_SLOTS,
    );

    let t0 = Instant::now();

    // ── Writer thread ───────────────────────────────────────────────
    let (write_tx, write_rx) = mpsc::sync_channel::<Vec<u8>>(RING_SLOTS);
    let writer_handle = {
        let output_path = output_path.clone();
        std::thread::spawn(move || -> u64 {
            let out_file = File::create(&output_path).expect("create output");
            let mut w = BufWriter::with_capacity(BUF_CAP, out_file);
            let mut total = 0u64;
            for chunk in write_rx {
                total += chunk.len() as u64;
                w.write_all(&chunk).expect("write output");
            }
            w.flush().expect("flush output");
            total
        })
    };

    // ── Slot pool ───────────────────────────────────────────────────
    let (slot_return_tx, slot_return_rx) = mpsc::sync_channel::<Vec<u8>>(RING_SLOTS);
    for _ in 0..RING_SLOTS {
        let mut slot = Vec::with_capacity(SLOT_SIZE);
        slot.resize(SLOT_SIZE, 0);
        slot_return_tx.send(slot).unwrap();
    }

    // ── Reader thread ───────────────────────────────────────────────
    let (filled_tx, filled_rx) = mpsc::sync_channel::<(Vec<u8>, usize, bool)>(RING_SLOTS);
    let reader_handle = std::thread::spawn(move || {
        loop {
            let mut slot = match slot_return_rx.recv() {
                Ok(s) => s,
                Err(_) => break,
            };
            let got = read_chunk(&mut reader, &mut slot[CARRY_HEADROOM..]);
            let is_last = got < CHUNK_SIZE;
            if filled_tx.send((slot, got, is_last)).is_err() {
                break;
            }
            if is_last {
                break;
            }
        }
    });

    // ── Worker threads ──────────────────────────────────────────────
    let (work_tx, work_rx) = mpsc::sync_channel::<WorkItem>(n_workers * 2);
    let (collector_tx, collector_rx) = mpsc::sync_channel::<CollectorMsg>(n_workers * 4);

    // Wrap work_rx in Arc<Mutex> so multiple workers can recv from it.
    let work_rx = std::sync::Arc::new(std::sync::Mutex::new(work_rx));

    let mut worker_handles = Vec::with_capacity(n_workers);
    for worker_id in 0..n_workers {
        let work_rx = work_rx.clone();
        let collector_tx = collector_tx.clone();
        worker_handles.push(
            std::thread::Builder::new()
                .name(format!("lbzip2-w{worker_id}"))
                .spawn(move || {
                    loop {
                        let item = {
                            let rx = work_rx.lock().unwrap();
                            match rx.recv() {
                                Ok(item) => item,
                                Err(_) => break, // channel closed
                            }
                        };

                        // Safety: data_ptr is valid — slot not recycled until
                        // collector sees all segments done for this chunk_id.
                        let data = unsafe {
                            std::slice::from_raw_parts(item.data_ptr, item.data_len)
                        };

                        let output = lbzip2::chunk::decode_segment(
                            data,
                            item.start_bit,
                            item.end_bit,
                            item.max_blocksize,
                        );

                        let _ = collector_tx.send(CollectorMsg::Result(SegmentResult {
                            chunk_id: item.chunk_id,
                            segment_id: item.segment_id,
                            output,
                        }));
                    }
                })
                .expect("spawn worker"),
        );
    }
    // Drop our copy so collector_rx will get Err when all senders are gone.
    drop(work_rx);

    // Main thread keeps a sender for NewSlot messages.
    let inflight_tx = collector_tx.clone();
    // Drop the extra clone — workers + main thread hold the only senders.
    drop(collector_tx);

    // ── Collector thread ────────────────────────────────────────────
    // Single channel, pure blocking recv — no polling, no timeout.
    let slot_return_for_collector = slot_return_tx.clone();

    let collector_handle = std::thread::spawn(move || {
        let mut in_flight: Vec<InFlightSlot> = Vec::new();
        let mut next_write_id: u64 = 0;

        while let Ok(msg) = collector_rx.recv() {
            match msg {
                CollectorMsg::NewSlot(slot_info) => {
                    in_flight.push(slot_info);
                }
                CollectorMsg::Result(result) => {
                    apply_result(&mut in_flight, result);
                }
            }
            // After every message, try to flush completed slots.
            flush_completed(
                &mut in_flight, &mut next_write_id,
                &write_tx, &slot_return_for_collector,
            );
        }

        // Channel closed — flush any remaining.
        flush_completed(
            &mut in_flight, &mut next_write_id,
            &write_tx, &slot_return_for_collector,
        );
    });

    // ── Main thread: carry + split + post work ──────────────────────
    let mut carry: Vec<u8> = header.to_vec();
    let mut chunk_id: u64 = 0;
    #[cfg(feature = "timing")]
    let mut t_recv_start = Instant::now();

    for (mut slot, read_len, is_last) in filled_rx {
        #[cfg(feature = "timing")]
        let recv_wait_ms = t_recv_start.elapsed().as_secs_f64() * 1000.0;

        if read_len == 0 && carry.len() <= 4 {
            slot_return_tx.send(slot).ok();
            break;
        }

        #[cfg(feature = "timing")]
        let t_carry = Instant::now();

        // Copy tiny carry into headroom.
        let carry_len = carry.len();
        assert!(carry_len <= CARRY_HEADROOM, "carry {} > headroom {}", carry_len, CARRY_HEADROOM);
        let data_start = CARRY_HEADROOM - carry_len;
        slot[data_start..CARRY_HEADROOM].copy_from_slice(&carry);
        let data_end = CARRY_HEADROOM + read_len;

        #[cfg(feature = "timing")]
        let carry_ms = t_carry.elapsed().as_secs_f64() * 1000.0;
        #[cfg(feature = "timing")]
        let t_split = Instant::now();

        // Split into segment boundaries.
        let data = &slot[data_start..data_end];
        let split = match lbzip2::chunk::split_chunk(data, n_workers, max_blocksize, is_last) {
            Some(s) => s,
            None => {
                slot_return_tx.send(slot).ok();
                if is_last { break; }
                continue;
            }
        };

        #[cfg(feature = "timing")]
        let split_ms = t_split.elapsed().as_secs_f64() * 1000.0;

        // Compute carry immediately — no need to wait for decode.
        carry.clear();
        carry.extend_from_slice(&data[split.consumed..]);

        let total_bits = data.len() as u64 * 8;
        let decode_segments = split.decode_segments;
        let n_segments = split.segment_starts.len();

        let segment_end = |i: usize| -> u64 {
            if i + 1 < n_segments {
                split.segment_starts[i + 1].bit_offset
            } else {
                total_bits
            }
        };

        // Register in-flight slot with collector.
        let inflight = InFlightSlot {
            chunk_id,
            slot,
            decode_segments,
            results: (0..decode_segments).map(|_| None).collect(),
            done_count: 0,
            is_last,
        };

        // Get pointer to data BEFORE moving slot into inflight.
        // Safety: the slot lives inside InFlightSlot until collector recycles it,
        // which only happens after all segments are done.
        let data_ptr = inflight.slot[data_start..].as_ptr();
        let data_len = data_end - data_start;

        inflight_tx.send(CollectorMsg::NewSlot(inflight)).expect("send inflight");

        // Post work items for all segments.
        for i in 0..decode_segments {
            let start_bit = split.segment_starts[i].bit_offset;
            let end_bit = segment_end(i);

            work_tx.send(WorkItem {
                chunk_id,
                segment_id: i,
                data_ptr,
                data_len,
                start_bit,
                end_bit,
                max_blocksize,
            }).expect("send work");
        }

        #[cfg(feature = "timing")]
        {
            eprintln!(
                "[timing] chunk {}: recv_wait={:.0}ms  carry={:.1}ms  split={:.1}ms  segments={}  posted_work={}",
                chunk_id, recv_wait_ms, carry_ms, split_ms, n_segments, decode_segments,
            );
            t_recv_start = Instant::now();
        }

        chunk_id += 1;

        if is_last {
            break;
        }
    }

    // Signal workers to stop (write_tx moved into collector — dropped when collector exits).
    drop(work_tx);
    drop(inflight_tx);
    drop(slot_return_tx);

    // Wait for pipeline to drain.
    for h in worker_handles {
        h.join().expect("worker panicked");
    }
    collector_handle.join().expect("collector panicked");
    reader_handle.join().expect("reader panicked");
    let total_out = writer_handle.join().expect("writer panicked");

    let elapsed = t0.elapsed().as_secs_f64();
    let out_mb = total_out / (1024 * 1024);
    let in_mb = in_size / (1024 * 1024);

    eprintln!(
        "done: {:.1}s  {} MB → {} MB  ({:.0} MB/s decompressed)",
        elapsed,
        in_mb,
        out_mb,
        out_mb as f64 / elapsed,
    );
}

// Collector helper functions.

fn apply_result(in_flight: &mut [InFlightSlot], result: SegmentResult) {
    for slot in in_flight.iter_mut() {
        if slot.chunk_id == result.chunk_id {
            slot.results[result.segment_id] = Some(result.output);
            slot.done_count += 1;
            return;
        }
    }
}

fn flush_completed(
    in_flight: &mut Vec<InFlightSlot>,
    next_write_id: &mut u64,
    write_tx: &mpsc::SyncSender<Vec<u8>>,
    slot_return: &mpsc::SyncSender<Vec<u8>>,
) {
    loop {
        let idx = in_flight.iter().position(|s| s.chunk_id == *next_write_id);
        let idx = match idx {
            Some(i) => i,
            None => break,
        };
        if in_flight[idx].done_count < in_flight[idx].decode_segments {
            break; // not complete yet
        }

        let mut completed = in_flight.remove(idx);

        // Send segments to writer in order.
        for seg in completed.results.drain(..) {
            if let Some(data) = seg {
                if !data.is_empty() {
                    write_tx.send(data).expect("send to writer");
                }
            }
        }

        // Recycle slot buffer back to reader.
        slot_return.send(completed.slot).ok();

        *next_write_id += 1;
    }
}
