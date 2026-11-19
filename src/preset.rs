//! Use-case presets for [`watch_applied_owned`]: choose the use case, not six
//! settings.
//!
//! The low-level API takes a scope, a repair, a store, an export channel and a
//! batch policy, and the combinations that are wrong still compile. Two
//! combinations are real, and a [`Preset`] fixes each one's scope and repair
//! so they can't be mixed up, and demands the pieces the use case can't do
//! without (a store, an export channel). Nothing here is a new mechanism:
//! [`Preset::run`] is a call to [`watch_applied_owned`], and everything it
//! does not fix stays a visible argument.
//!
//! | | [`Preset::full_replica`] | [`Preset::node_slice`] |
//! |---|---|---|
//! | Scope | [`WatchScope::All`] | [`WatchScope::Prefix`] |
//! | On cursor expiry | [`ExpiryRepair::Auto`] | [`ExpiryRepair::None`] |
//! | Fold stays exact across expiry | yes, or the watch fails | **no**: gap deletes may persist |
//! | Needs a restore source | yes | no |
//! | Lag in the health snapshot | the true backlog | a trend only |

use std::sync::Arc;

use tokio::sync::{mpsc, watch};

use crate::applied::{BatchConfig, ExportRequest, WatchEnd, WatchScope, watch_applied_owned};
use crate::health::WatchHealth;
use crate::kv::{KvReader, KvUpdate, KvWatcher, WatchCursor};
use crate::repair::{ExpiryRepair, RestoreSource};
use crate::snapshot::SnapshotStore;

/// A use case for [`watch_applied_owned`]. Build one with
/// [`full_replica`](Self::full_replica) or [`node_slice`](Self::node_slice),
/// then [`run`](Self::run) it. A preset is reusable: call `run` again, with
/// the store and cursor the last [`WatchEnd`] handed back, to restart.
pub struct Preset<S> {
    scope: WatchScope,
    // `Some` for the replica's `Auto` repair; the slice repairs nothing.
    auto: Option<(Arc<dyn KvReader>, Arc<dyn RestoreSource<S>>)>,
    config: BatchConfig,
}

impl<S: SnapshotStore + Send + 'static> Preset<S> {
    /// A **full replica**: every key of the bucket, in a durable fold that is
    /// bootstrapped from an artifact, exported to one, and repaired from one.
    /// The use case of fleet-wide route and assignment tables (every edge,
    /// tunnel and orchestrator node folds the same buckets).
    ///
    /// # What it does on cursor expiry
    ///
    /// [`ExpiryRepair::Auto`], decided at the moment of expiry by the bucket's
    /// live retention:
    ///
    /// - A bucket that has never evicted a current value: the key-listing diff
    ///   (synthetic deletes for keys the bucket no longer lists), then the full
    ///   re-list. The fold ends exact.
    /// - A bucket that evicts current values (`max_age`, `discard: old`), or
    ///   whose fold ever saw it do so: restore from the newest artifact
    ///   through `restore`, which must be ahead of the fold, cover
    ///   [`WatchScope::All`], and still be inside the log's retention. The
    ///   fold ends exact.
    /// - Otherwise the watch **fails** (logged at `error`), because there is
    ///   no safe recovery. The fold stays stale while the caller's restart
    ///   retries the same expired cursor. The health snapshot shows
    ///   [`RepairState::Refused`](crate::RepairState::Refused) or
    ///   [`WaitingForArtifact`](crate::RepairState::WaitingForArtifact).
    ///
    /// A cursor-less start on a bucket that has already evicted is repaired
    /// the same way: it seeds from the artifact, never from the bucket alone.
    ///
    /// # What the caller still owns
    ///
    /// - Publishing artifacts well inside the bucket's retention turnover
    ///   (`run_export_round`, driven through the
    ///   `exports` channel of [`run`](Self::run)), from a fold that exports
    ///   scope `All`. An artifact older than retention is refused.
    /// - Opening the fold, including the first-boot import of an artifact
    ///   (`import_remote` on the backend) before the first `run`.
    /// - A durable `store`. A restore replaces the fold, so a replica without
    ///   one cannot repair a bounded log, and `run` takes the store by value.
    pub fn full_replica(reader: Arc<dyn KvReader>, restore: Arc<dyn RestoreSource<S>>) -> Self {
        Self {
            scope: WatchScope::All,
            auto: Some((reader, restore)),
            config: BatchConfig::default(),
        }
    }

    /// A **per-node slice**: only the keys under `prefix`, in a durable fold,
    /// exported so a replaced node cold-boots from an artifact instead of
    /// re-listing. The use case of per-node assignments, where the prefix is
    /// the node's own.
    ///
    /// # What it does on cursor expiry
    ///
    /// [`ExpiryRepair::None`]: the re-list alone, with a warning. Keys written
    /// during the gap are delivered; **keys deleted during the gap whose
    /// delete markers were evicted stay in the fold**. This is correct only
    /// when the bucket's resume window outlives any downtime of the node, or
    /// when a stale key is harmless to the consumer (it is reconciled against
    /// something else). If neither holds, use the low-level API with
    /// [`ExpiryRepair::Restore`] and a source whose artifacts cover this
    /// prefix.
    ///
    /// Every expiry is counted in
    /// [`HealthSnapshot::cursor_expiries`](crate::HealthSnapshot::cursor_expiries).
    /// Alert on any increase: for this preset it means the fold may hold a
    /// ghost key.
    ///
    /// A prefix scope also cannot detect, in-band, retention overrunning a
    /// live watch (only the resume-time check runs on a restart), so retention
    /// must outlive the watch's worst lag.
    ///
    /// # What the caller still owns
    ///
    /// Opening the fold (including the artifact import on first boot), and
    /// exporting from it. The slice's artifacts record the prefix as their
    /// scope, so only a watcher inside that prefix may restore from them.
    pub fn node_slice(prefix: impl Into<String>) -> Self {
        Self {
            scope: WatchScope::Prefix(prefix.into()),
            auto: None,
            config: BatchConfig::default(),
        }
    }

    /// Record this watch's health on `health` (see [`WatchHealth`]). Use one
    /// handle per watch, and the same one across its restarts.
    pub fn health(mut self, health: WatchHealth) -> Self {
        self.config.health = Some(health);
        self
    }

    /// Replace the batching policy (window, max, channel capacity). The health
    /// handle set by [`health`](Self::health) is kept unless `config` brings
    /// its own.
    pub fn batch(mut self, config: BatchConfig) -> Self {
        let health = self.config.health.take();
        self.config = BatchConfig {
            health: config.health.or(health),
            ..config
        };
        self
    }

    /// The scope this preset watches.
    pub fn scope(&self) -> &WatchScope {
        &self.scope
    }

    /// Run the watch (see [`watch_applied_owned`], whose contract this keeps:
    /// the store and the applied cursor come back in the [`WatchEnd`], error
    /// or not). `exports` is required: both use cases export their fold, so a
    /// replacement node can boot from an artifact; pass the receiver that the
    /// export loop's sender feeds.
    #[allow(clippy::too_many_arguments)]
    pub async fn run<U, P, A, O>(
        &self,
        watcher: Arc<dyn KvWatcher>,
        resume: Option<WatchCursor>,
        store: S,
        exports: mpsc::Receiver<ExportRequest>,
        parse: P,
        apply: A,
        on_applied: O,
        shutdown: watch::Receiver<bool>,
    ) -> WatchEnd<S>
    where
        U: Send,
        P: FnMut(&KvUpdate) -> Option<U> + Send,
        A: FnMut(Vec<U>) + Send,
        O: FnMut(WatchCursor) + Send,
    {
        let repair = match &self.auto {
            Some((reader, restore)) => ExpiryRepair::Auto {
                reader: Arc::clone(reader),
                restore: Arc::clone(restore),
            },
            None => ExpiryRepair::None,
        };
        watch_applied_owned(
            watcher,
            self.scope.clone(),
            resume,
            repair,
            Some(store),
            Some(exports),
            self.config.clone(),
            parse,
            apply,
            on_applied,
            shutdown,
        )
        .await
    }
}
