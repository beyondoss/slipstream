//! Regression guard for `stream_sequence_from_ack` parse cost.
//!
//! The real function is private to `src/nats.rs`, so its algorithm is mirrored
//! here (keep the two in sync). This is the only benchmark coverage for the
//! scan-path parse cost — the live `scan()` path needs a NATS server, so the
//! per-message work is isolated here instead. An A/B against the old
//! `Vec`-collecting implementation measured ~3.1× (1.59 ms → 0.51 ms per 10k
//! parses); this guards against that regressing.

use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;

/// Mirror of `src/nats.rs::stream_sequence_from_ack`: first 8 tokens on the
/// stack, count the rest, no heap allocation.
fn stream_sequence_from_ack(reply: &str) -> Option<u64> {
    let mut head = [""; 8];
    let mut count = 0usize;
    for (i, token) in reply.split('.').enumerate() {
        if i < head.len() {
            head[i] = token;
        }
        count += 1;
    }
    if count < 9 || head[0] != "$JS" || head[1] != "ACK" {
        return None;
    }
    let stream_seq_idx = if count == 9 { 5 } else { 7 };
    head[stream_seq_idx].parse::<u64>().ok()
}

// Representative subjects: legacy (9 tokens) and modern (11) with domain/account.
const LEGACY: &str = "$JS.ACK.KV_certs.cons.1.42.7.1700000000000000000.0";
const MODERN: &str = "$JS.ACK.hub.AABBCC.KV_certs.cons.1.42.7.1700000000000000000.0";

fn bench_ack(c: &mut Criterion) {
    // Simulate one `scan()` of 10k keys: parse 10k ACK subjects back to back.
    c.bench_function("ack_parse_10k_scan", |b| {
        b.iter(|| {
            let mut acc = 0u64;
            for i in 0..10_000 {
                let subj = if i % 2 == 0 { LEGACY } else { MODERN };
                acc = acc.wrapping_add(stream_sequence_from_ack(black_box(subj)).unwrap_or(0));
            }
            black_box(acc)
        })
    });
}

criterion_group!(benches, bench_ack);
criterion_main!(benches);
