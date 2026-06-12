//! Batch throughput for the [`watch_applied`] combinator.
//!
//! Measures the per-update cost of the cursor-after-apply loop — receive off the
//! channel, track the high-water cursor, parse, batch, flush (apply +
//! checkpoint) — with a no-op `apply` and no snapshot, so what's left is the
//! combinator's own batching overhead. A scripted in-process watcher feeds N
//! updates and closes; there is no NATS server in the loop.

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use slipstream::{
    BatchConfig, KvEntry, KvError, KvUpdate, KvWatcher, VersionToken, WatchCursor, WatchScope,
    watch_applied,
};
use tokio::sync::mpsc::Sender;
use tokio::sync::watch;

/// Delivers a fixed batch of updates, then closes the channel.
struct ScriptedWatcher {
    updates: Mutex<Option<Vec<KvUpdate>>>,
}

#[async_trait]
impl KvWatcher for ScriptedWatcher {
    async fn watch_all(&self, tx: Sender<KvUpdate>) -> Result<(), KvError> {
        let updates = self.updates.lock().unwrap().take().unwrap_or_default();
        for u in updates {
            if tx.send(u).await.is_err() {
                break;
            }
        }
        Ok(())
    }

    async fn watch_prefix(&self, _prefix: &str, tx: Sender<KvUpdate>) -> Result<(), KvError> {
        self.watch_all(tx).await
    }

    async fn watch_prefixes(
        &self,
        _prefixes: &[&str],
        tx: Sender<KvUpdate>,
    ) -> Result<(), KvError> {
        self.watch_all(tx).await
    }
}

fn put(i: u64) -> KvUpdate {
    KvUpdate::Put(KvEntry {
        key: format!("node.region-{i}"),
        value: vec![0x42; 256],
        version: VersionToken::from_u64(i),
    })
}

fn bench_batch_throughput(c: &mut Criterion) {
    const N: u64 = 1000;

    // Multi-thread runtime so the spawned watch task and the combinator loop run
    // on separate threads — the realistic deployment shape.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    let template: Vec<KvUpdate> = (0..N).map(put).collect();

    let mut group = c.benchmark_group("watch_applied");
    group.throughput(Throughput::Elements(N));
    group.bench_function("batch_1000_updates", |b| {
        b.iter_batched(
            // Setup (not measured): fresh scripted watcher with its own copy of
            // the updates, since each run drains the channel.
            || {
                Arc::new(ScriptedWatcher {
                    updates: Mutex::new(Some(template.clone())),
                })
            },
            // Measured: run the combinator to completion over all N updates.
            |watcher| {
                rt.block_on(async move {
                    let (_sd_tx, sd_rx) = watch::channel(false);
                    watch_applied(
                        watcher as Arc<dyn KvWatcher>,
                        WatchScope::All,
                        None,
                        None, // reader: cursor-expired resync not exercised here
                        None::<slipstream::AppendLogSnapshot>,
                        None,
                        BatchConfig {
                            window: Duration::from_millis(10),
                            max: 100,
                            ..BatchConfig::default()
                        },
                        // parse: keep every put.
                        |u: &KvUpdate| match u {
                            KvUpdate::Put(_) => Some(()),
                            _ => None,
                        },
                        // apply: no-op — isolate the batching overhead.
                        |_batch: Vec<()>| {},
                        |_cursor: WatchCursor| {},
                        sd_rx,
                    )
                    .await
                    .unwrap()
                })
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

criterion_group!(benches, bench_batch_throughput);
criterion_main!(benches);
