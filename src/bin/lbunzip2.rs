//! CLI parallel bzip2 decompressor — pure Rust, all available cores.
//!
//! Usage: lbunzip2 <input.bz2> [output]
//!   If output is omitted, strips .bz2 extension.
//!
//! Zero-copy pipeline with 4-slot ring buffer:
//!
//!   ┌──────────┐    ┌────────────┐    ┌──────────┐
//!   │  Reader   │──→│   Decode   │──→│  Writer   │
//!   │  thread   │    │   (main)   │    │  thread   │
//!   └──────────┘    └────────────┘    └──────────┘
//!        ↑                │
//!        └── slot pool ───┘   (4 pre-allocated buffers recycled)
//!
//! Only the tiny carry (< 1 MB) is ever copied.
//! The 200 MB raw read stays in-place — never copied.

use std::{
    fs::File,
    io::{BufReader, BufWriter, Read, Write},
    sync::mpsc,
    time::Instant,
};

const CHUNK_SIZE: usize = 200 * 1024 * 1024;
const BUF_CAP: usize = 4 * 1024 * 1024;
const RING_SLOTS: usize = 4;
/// Headroom at the start of each slot for carry data.
/// Max carry ≈ CHUNK_SIZE / n_threads (one undecoded segment).
/// With 200 MB / 16 threads ≈ 12.5 MB, so 32 MB is safe.
const CARRY_HEADROOM: usize = 32 * 1024 * 1024;
const SLOT_SIZE: usize = CARRY_HEADROOM + CHUNK_SIZE;

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
    let decoder = lbzip2::chunk::ChunkDecoder::from_header(&header)
        .expect("invalid bz2 header");

    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);

    eprintln!(
        "lbunzip2: {} ({} MB) → {}  [{} cores]",
        input_path,
        in_size / (1024 * 1024),
        output_path,
        cores,
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
            #[cfg(feature = "timing")]
            let mut batch_bytes = 0u64;
            #[cfg(feature = "timing")]
            let mut batch_start = Instant::now();
            for chunk in write_rx {
                let len = chunk.len() as u64;
                total += len;
                w.write_all(&chunk).expect("write output");
                #[cfg(feature = "timing")]
                {
                    batch_bytes += len;
                    // report every ~500 MB written
                    if batch_bytes >= 500 * 1024 * 1024 {
                        let dt = batch_start.elapsed().as_secs_f64();
                        eprintln!(
                            "[timing] writer: {:.0} MB in {:.2}s = {:.0} MB/s",
                            batch_bytes as f64 / (1024.0 * 1024.0),
                            dt,
                            batch_bytes as f64 / (1024.0 * 1024.0) / dt,
                        );
                        batch_bytes = 0;
                        batch_start = Instant::now();
                    }
                }
            }
            w.flush().expect("flush output");
            total
        })
    };

    // ── Slot pool: pre-allocated buffers recycled between reader and decoder
    //
    //  Slot layout:  [CARRY_HEADROOM][────── CHUNK_SIZE ──────]
    //                 ↑ carry copied    ↑ reader fills here
    //                   into headroom     (zero copy — stays in place)
    //
    let (slot_return_tx, slot_return_rx) = mpsc::sync_channel::<Vec<u8>>(RING_SLOTS);
    for _ in 0..RING_SLOTS {
        let mut slot = Vec::with_capacity(SLOT_SIZE);
        slot.resize(SLOT_SIZE, 0);
        slot_return_tx.send(slot).unwrap();
    }

    // ── Reader thread: reads into pre-allocated slots ───────────────
    // Reads raw data into slot[CARRY_HEADROOM..], sends (slot, read_len, is_last).
    let (filled_tx, filled_rx) = mpsc::sync_channel::<(Vec<u8>, usize, bool)>(RING_SLOTS);
    let reader_handle = std::thread::spawn(move || {
        #[cfg(feature = "timing")]
        let mut chunk_n = 0u32;
        loop {
            #[cfg(feature = "timing")]
            let t_wait = Instant::now();

            let mut slot = match slot_return_rx.recv() {
                Ok(s) => s,
                Err(_) => break,
            };

            #[cfg(feature = "timing")]
            let wait_ms = t_wait.elapsed().as_secs_f64() * 1000.0;
            #[cfg(feature = "timing")]
            let t_read = Instant::now();

            let got = read_chunk(&mut reader, &mut slot[CARRY_HEADROOM..]);

            #[cfg(feature = "timing")]
            {
                let read_ms = t_read.elapsed().as_secs_f64() * 1000.0;
                let read_mb = got as f64 / (1024.0 * 1024.0);
                let mbps = if read_ms > 0.0 { read_mb / (read_ms / 1000.0) } else { 0.0 };
                eprintln!(
                    "[timing] reader chunk {}: wait={:.0}ms  read={:.0}ms ({:.0} MB, {:.0} MB/s)",
                    chunk_n, wait_ms, read_ms, read_mb, mbps,
                );
                chunk_n += 1;
            }

            let is_last = got < CHUNK_SIZE;
            if filled_tx.send((slot, got, is_last)).is_err() {
                break;
            }
            if is_last {
                break;
            }
        }
    });

    // ── Main thread: zero-copy carry + parallel decode ──────────────
    // Carry: the unconsumed tail from the previous chunk. Always small
    // (< 1 MB). Copied into the headroom area of the next slot so
    // decode_chunk sees one contiguous &[u8] without copying the 200 MB.
    let mut carry: Vec<u8> = header.to_vec();
    #[cfg(feature = "timing")]
    let mut chunk_n = 0u32;
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

        // Copy tiny carry into headroom just before the read data.
        let carry_len = carry.len();
        assert!(carry_len <= CARRY_HEADROOM, "carry {} > headroom {}", carry_len, CARRY_HEADROOM);
        let data_start = CARRY_HEADROOM - carry_len;
        slot[data_start..CARRY_HEADROOM].copy_from_slice(&carry);
        let data_end = CARRY_HEADROOM + read_len;

        let data = &slot[data_start..data_end];

        #[cfg(feature = "timing")]
        let carry_ms = t_carry.elapsed().as_secs_f64() * 1000.0;
        #[cfg(feature = "timing")]
        let t_decode = Instant::now();

        // Parallel decode → segments returned separately (no big memcpy).
        let (segments, consumed) = decoder
            .decode_chunk_segments(data, is_last)
            .expect("bz2 decode error");

        #[cfg(feature = "timing")]
        let decode_ms = t_decode.elapsed().as_secs_f64() * 1000.0;

        // Save new carry (tiny — just the unconsumed tail).
        carry.clear();
        carry.extend_from_slice(&data[consumed..]);

        // Recycle slot back to reader — no allocation.
        slot_return_tx.send(slot).ok();

        #[cfg(feature = "timing")]
        let t_send = Instant::now();

        let mut seg_bytes = 0usize;
        // Send each segment to writer individually — no single-thread
        // assembly of a giant Vec.  Writer writes them in order.
        for seg in segments {
            if !seg.is_empty() {
                seg_bytes += seg.len();
                write_tx.send(seg).expect("send to writer");
            }
        }

        #[cfg(feature = "timing")]
        {
            let send_ms = t_send.elapsed().as_secs_f64() * 1000.0;
            let in_mb = (read_len + carry_len) as f64 / (1024.0 * 1024.0);
            let out_mb = seg_bytes as f64 / (1024.0 * 1024.0);
            let decode_mbps = if decode_ms > 0.0 { out_mb / (decode_ms / 1000.0) } else { 0.0 };
            eprintln!(
                "[timing] decode chunk {}: recv_wait={:.0}ms  carry={:.1}ms ({:.1}MB)  decode={:.0}ms ({:.0}MB in, {:.0}MB out, {:.0} MB/s)  send={:.1}ms",
                chunk_n, recv_wait_ms, carry_ms, carry_len as f64 / (1024.0 * 1024.0),
                decode_ms, in_mb, out_mb, decode_mbps,
                send_ms,
            );
            chunk_n += 1;
            t_recv_start = Instant::now();
        }

        if is_last {
            break;
        }
    }

    drop(write_tx);
    drop(slot_return_tx);
    reader_handle.join().expect("reader thread panicked");
    let total_out = writer_handle.join().expect("writer thread panicked");

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
