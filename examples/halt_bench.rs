//! Bulk-apply bench for fjall's write-halt wedge (J11).
//!
//! Applies `ENTRIES` puts through `FjallSnapshot::apply` in 1024-entry batches
//! as fast as possible, printing one line per `REPORT` entries: entries,
//! interval writes/s, L0 tables, active compactions, watchdog rescues. Exits 2
//! with `WEDGED` when the interval rate stays under `WEDGE_RATE` writes/s for
//! `WEDGE_SECS` seconds.
//!
//! Env: ENTRIES, KEYS=rand|seq, DIR, REPORT, WEDGE_RATE, WEDGE_SECS,
//! WORKERS and MEMTABLE_MB (0 = shipped tuning; 4 and 64 is the config that
//! wedged raw fjall 3.1.5 at 257M entries).
//!
//!     cargo run --release --example halt_bench --features fjall

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

use slipstream::{
    FjallConfig, FjallSnapshot, KvEntry, KvUpdate, SnapshotStore, VersionToken, WatchCursor,
};

const BATCH: usize = 1024;

fn env<T: std::str::FromStr>(k: &str, d: T) -> T {
    std::env::var(k)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(d)
}

fn mix(mut x: u64) -> u64 {
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
    x ^= x >> 33;
    x = x.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    x ^ (x >> 33)
}

fn key(i: u64, rand: bool) -> String {
    let k = if rand { mix(i) } else { i };
    format!("route.svc-{:08}.{:08}", k / 1000, k % 1000)
}

fn main() {
    let entries: u64 = env("ENTRIES", 300_000_000);
    let rand = env::<String>("KEYS", "rand".into()) == "rand";
    let report: u64 = env("REPORT", 5_000_000);
    let wedge_rate: f64 = env("WEDGE_RATE", 2000.0);
    let wedge_secs: u64 = env("WEDGE_SECS", 120);
    let dir: PathBuf = env::<String>("DIR", "/tmp/halt_bench".into()).into();
    let _ = std::fs::remove_dir_all(&dir);
    let mut value = vec![0u8; 200];
    let mut s = 0x5EEDu64;
    for b in &mut value {
        s = mix(s);
        *b = s as u8;
    }

    let config = FjallConfig {
        worker_threads: env("WORKERS", 0),
        memtable_bytes: env::<u64>("MEMTABLE_MB", 0) << 20,
        ..FjallConfig::default()
    };
    let (_, mut snap) = FjallSnapshot::open(&dir, config).unwrap();
    let rd = snap.reader();
    let applied = Arc::new(AtomicU64::new(0));

    let a2 = applied.clone();
    let t0 = Instant::now();
    std::thread::spawn(move || {
        let (mut last_n, mut last_t, mut next) = (0u64, Instant::now(), report);
        let mut slow_since = None::<Instant>;
        loop {
            std::thread::sleep(Duration::from_secs(1));
            let n = a2.load(Relaxed);
            let now = Instant::now();
            let rate = (n - last_n) as f64 / now.duration_since(last_t).as_secs_f64();
            let st = rd.stats();
            let line = format!(
                "entries={n} writes/s={rate:.0} l0={} active_compactions={} halt_rescues={} t={}s",
                st.l0_tables,
                st.active_compactions,
                st.halt_rescues,
                t0.elapsed().as_secs()
            );
            if n >= next {
                println!("{line}");
                next += report;
            }
            if n >= entries {
                println!("DONE {line} wedged=no");
                std::process::exit(0);
            }
            if rate < wedge_rate {
                let since = *slow_since.get_or_insert(now);
                if now.duration_since(since).as_secs() >= wedge_secs {
                    println!("WEDGED {line} wedged=yes");
                    std::process::exit(2);
                }
            } else {
                slow_since = None;
            }
            last_n = n;
            last_t = now;
        }
    });

    let mut batch = Vec::with_capacity(BATCH);
    let mut i = 0u64;
    while i < entries {
        batch.clear();
        let end = (i + BATCH as u64).min(entries);
        for j in i..end {
            batch.push(KvUpdate::Put(KvEntry {
                key: key(j, rand),
                value: value.clone(),
                version: VersionToken::from_u64(j + 1),
            }));
        }
        snap.apply(&batch, &WatchCursor::from_u64(end)).unwrap();
        applied.store(end, Relaxed);
        i = end;
    }
    std::thread::sleep(Duration::from_secs(2));
}
