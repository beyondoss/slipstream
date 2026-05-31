use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;

use crate::kv::{KvError, KvReader, KvWatcher, KvWriter};

/// Storage type for a store.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum StorageType {
    /// In-memory storage (fast, lost on restart).
    Memory,
    /// Persistent storage (survives restarts).
    #[default]
    Persistent,
}

/// Configuration for creating a store.
#[derive(Debug, Clone, Default)]
pub struct StoreConfig {
    /// Store name/bucket identifier.
    pub name: String,
    /// Storage type (memory or persistent).
    pub storage: StorageType,
    /// Maximum history/versions to keep (NATS-specific, ignored by other stores).
    pub max_history: Option<u32>,
    /// Maximum age for entries in the bucket (bucket-level TTL).
    /// Entries older than this are automatically removed.
    /// NATS: maps to `max_age` on bucket config.
    pub max_age: Option<Duration>,
    /// Maximum bytes for the bucket (required by Synadia Cloud).
    /// NATS: maps to `max_bytes` on bucket config.
    pub max_bytes: Option<i64>,
    /// Number of stream replicas for the bucket (NATS cluster mode).
    /// Defaults to 1 (single replica). Set to 3 for production HA clusters.
    pub num_replicas: Option<usize>,
}

/// A named KV store (bucket/namespace/database).
pub trait KvStore: Send + Sync {
    /// The store's name/bucket identifier.
    fn name(&self) -> &str;

    /// Get the reader interface.
    fn reader(&self) -> Arc<dyn KvReader>;

    /// Get the watcher interface (if supported).
    fn watcher(&self) -> Option<Arc<dyn KvWatcher>> {
        None
    }

    /// Get the writer interface (if supported).
    fn writer(&self) -> Option<Arc<dyn KvWriter>> {
        None
    }
}

/// Capabilities a store connection may support.
#[derive(Debug, Clone, Default)]
pub struct ConnectionCapabilities {
    /// Supports streaming watch (continuous updates). NATS: true, FDB: false.
    pub streaming_watch: bool,
    /// Supports native prefix watch. NATS: true, FDB: false (uses sentinel pattern).
    pub prefix_watch: bool,
    /// Supports TTL on keys.
    pub ttl: bool,
    /// Supports atomic compare-and-swap.
    pub cas: bool,
    /// Supports multi-key transactions.
    pub transactions: bool,
    /// Maximum value size in bytes (0 = unlimited).
    pub max_value_size: usize,
    /// Global ordering via versionstamps. FDB: true, NATS: false.
    pub global_ordering: bool,
}

/// Store connection lifecycle and store factory.
#[async_trait]
pub trait Connection: Send + Sync {
    /// Connect to the store.
    async fn connect(&self) -> Result<(), KvError>;

    /// Graceful shutdown.
    async fn shutdown(&self) -> Result<(), KvError>;

    /// Health check - fast, non-blocking.
    fn is_healthy(&self) -> bool;

    /// Get or create a named store with default configuration.
    async fn store(&self, name: &str) -> Result<Arc<dyn KvStore>, KvError>;

    /// Get or create a named store with custom configuration.
    ///
    /// Use this when you need to specify bucket-level settings like TTL or history.
    ///
    /// Config applies only at **creation**. If the bucket already exists, the
    /// existing one is returned as-is and `config` (max_bytes, num_replicas,
    /// max_history, max_age, …) is ignored — there is no reconciliation. To change
    /// settings on a live bucket (e.g. raising `num_replicas` for HA), alter the
    /// underlying stream out-of-band; calling this with new values is a no-op.
    async fn store_with_config(&self, config: StoreConfig) -> Result<Arc<dyn KvStore>, KvError>;

    /// Store capabilities for runtime feature detection.
    fn capabilities(&self) -> ConnectionCapabilities;
}
