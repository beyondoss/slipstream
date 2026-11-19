//! A raw non-`_from` watch on a bucket that evicts current values logs ONE
//! warning (its re-list is only what the stream still holds); `discard: new`
//! buckets and slipstream's own watches (`watch_applied`) log none.
//!
//! Each test drives a real `nats-server` and counts warnings with a
//! hand-rolled `tracing` subscriber (no extra dev-dependency). Buckets have
//! unique names: the warning is once per bucket per process.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common::TestNats;
use slipstream::{
    BatchConfig, Connection, DiscardPolicy, ExpiryRepair, KvStore, KvUpdate, NatsConnection,
    NatsConnectionConfig, StoreConfig, WatchCursor, WatchScope, watch_applied,
};
use tokio::sync::{mpsc, watch};
use tracing::{Level, Metadata, span};

/// Counts WARN events whose `bucket` field equals `bucket`.
struct WarnCounter {
    bucket: &'static str,
    count: Arc<AtomicUsize>,
}

struct BucketField<'a>(&'a str, bool);
impl tracing::field::Visit for BucketField<'_> {
    fn record_str(&mut self, f: &tracing::field::Field, v: &str) {
        if f.name() == "bucket" && v == self.0 {
            self.1 = true;
        }
    }
    fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
}

impl tracing::Subscriber for WarnCounter {
    fn enabled(&self, m: &Metadata<'_>) -> bool {
        *m.level() <= Level::WARN
    }
    fn new_span(&self, _: &span::Attributes<'_>) -> span::Id {
        span::Id::from_u64(1)
    }
    fn record(&self, _: &span::Id, _: &span::Record<'_>) {}
    fn record_follows_from(&self, _: &span::Id, _: &span::Id) {}
    fn event(&self, e: &tracing::Event<'_>) {
        if *e.metadata().level() == Level::WARN {
            let mut v = BucketField(self.bucket, false);
            e.record(&mut v);
            if v.1 {
                self.count.fetch_add(1, Ordering::SeqCst);
            }
        }
    }
    fn enter(&self, _: &span::Id) {}
    fn exit(&self, _: &span::Id) {}
}

async fn open(nats: &TestNats, name: &str, discard: DiscardPolicy) -> Arc<dyn KvStore> {
    let conn = NatsConnection::new(NatsConnectionConfig {
        url: nats.url.clone(),
        creds: None,
        creds_file: None,
    });
    conn.connect().await.expect("connect");
    conn.store_with_config(StoreConfig {
        name: name.into(),
        max_bytes: Some(8 * 1024 * 1024),
        discard,
        ..Default::default()
    })
    .await
    .expect("open bucket")
}

/// Open a raw prefix watch and drop it after it attaches.
async fn raw_watch(store: &Arc<dyn KvStore>) {
    let w = store.watcher().expect("watcher");
    let (tx, _rx) = mpsc::channel::<KvUpdate>(16);
    let _ = tokio::time::timeout(Duration::from_millis(500), w.watch_prefix("a.", tx)).await;
}

fn counter(bucket: &'static str) -> (Arc<AtomicUsize>, tracing::subscriber::DefaultGuard) {
    let count = Arc::new(AtomicUsize::new(0));
    let guard = tracing::subscriber::set_default(WarnCounter {
        bucket,
        count: Arc::clone(&count),
    });
    (count, guard)
}

#[tokio::test(flavor = "current_thread")]
async fn raw_watch_on_evicting_bucket_warns_once() {
    let nats = TestNats::start().await;
    let store = open(&nats, "rw_evicting", DiscardPolicy::Old).await;
    let (count, _g) = counter("rw_evicting");
    raw_watch(&store).await;
    raw_watch(&store).await;
    assert_eq!(
        count.load(Ordering::SeqCst),
        1,
        "one warning across two raw watches"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn raw_watch_on_non_evicting_bucket_is_silent() {
    let nats = TestNats::start().await;
    let store = open(&nats, "rw_keeping", DiscardPolicy::New).await;
    let (count, _g) = counter("rw_keeping");
    raw_watch(&store).await;
    assert_eq!(count.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn watch_applied_on_evicting_bucket_is_silent() {
    let nats = TestNats::start().await;
    let store = open(&nats, "rw_applied", DiscardPolicy::Old).await;
    let (count, _g) = counter("rw_applied");
    let (sd_tx, sd_rx) = watch::channel(false);
    let task = tokio::spawn(watch_applied(
        store.watcher().expect("watcher"),
        WatchScope::All,
        None::<WatchCursor>,
        ExpiryRepair::None,
        None::<slipstream::AppendLogSnapshot>,
        None,
        BatchConfig::default(),
        |_: &KvUpdate| Some(()),
        |_: Vec<()>| {},
        |_: WatchCursor| {},
        sd_rx,
    ));
    tokio::time::sleep(Duration::from_millis(500)).await;
    let _ = sd_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(5), task).await;
    assert_eq!(count.load(Ordering::SeqCst), 0);
}
