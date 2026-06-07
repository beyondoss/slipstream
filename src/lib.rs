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

mod applied;
mod kv;
mod nats;
pub mod snapshot;
#[cfg(feature = "fjall")]
mod snapshot_fjall;
mod stores;

pub use applied::{BatchConfig, WatchScope, watch_applied};
pub use kv::{
    KvEntry, KvError, KvReader, KvTtl, KvUpdate, KvWatcher, KvWriter, VersionToken, WatchCursor,
};
pub use nats::{NatsConnection, NatsConnectionConfig, nats_connect};
pub use snapshot::{AppendLogSnapshot, SnapshotStore};
#[cfg(feature = "fjall")]
pub use snapshot_fjall::{FjallConfig, FjallSnapshot};
pub use stores::{Connection, ConnectionCapabilities, KvStore, StorageType, StoreConfig};
