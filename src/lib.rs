//! Shared KV store abstraction for Beyond services.
//!
//! This crate provides a backend-agnostic interface for key-value storage,
//! with NATS JetStream as the primary implementation.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │              KvReader │ KvWatcher │ KvWriter                │
//! │                 (core KV operations)                        │
//! ├─────────────────────────────────────────────────────────────┤
//! │                        KvStore                              │
//! │            (named bucket with reader/watcher/writer)        │
//! ├─────────────────────────────────────────────────────────────┤
//! │                       Connection                            │
//! │              (lifecycle, store factory, capabilities)       │
//! ├─────────────────────────────────────────────────────────────┤
//! │                    NatsConnection                           │
//! │               (concrete implementation)                     │
//! └─────────────────────────────────────────────────────────────┘
//! ```

#![deny(unsafe_code)]
#![deny(unused_must_use)]

mod applied;
mod artifact;
mod export_lease;
mod kv;
mod nats;
pub mod protocol;
pub mod snapshot;
#[cfg(feature = "fjall")]
mod snapshot_fjall;
#[cfg(any(feature = "fjall", feature = "rocksdb"))]
mod snapshot_record;
#[cfg(feature = "rocksdb")]
mod snapshot_rocksdb;
mod stores;
#[cfg(feature = "transport")]
mod transport;

pub use applied::{BatchConfig, ExportRequest, WatchScope, watch_applied};
pub use artifact::{ARTIFACT_SCHEMA_VERSION, ArtifactFile, ExportManifest, MANIFEST_FILE};
pub use export_lease::{ExportLease, LeaseGuard, LeaseRecord};
pub use kv::{
    KvEntry, KvError, KvPurge, KvReader, KvTtl, KvUpdate, KvWatcher, KvWriter, VersionToken,
    WatchCursor,
};
pub use nats::{NatsConnection, NatsConnectionConfig, nats_connect};
pub use snapshot::{AppendLogSnapshot, SnapshotStore};
#[cfg(feature = "fjall")]
pub use snapshot_fjall::{FjallConfig, FjallReader, FjallSnapshot};
#[cfg(feature = "rocksdb")]
pub use snapshot_rocksdb::{RocksDbConfig, RocksDbReader, RocksDbSnapshot};
pub use stores::{
    Connection, ConnectionCapabilities, DiscardPolicy, KvStore, StorageType, StoreConfig,
};
#[cfg(feature = "transport")]
pub use transport::{ArtifactTransport, ObjectStoreTransport, PublishOutcome, run_export_round};
