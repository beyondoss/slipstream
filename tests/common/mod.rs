//! Shared integration-test infrastructure: throwaway `nats-server` and
//! `minio` instances, one per test, killed on drop, and the transport tiers'
//! crash injection.
//!
//! All binaries come from mise (`mise install`); env overrides
//! `NATS_SERVER_BIN` / `MINIO_BIN` / `MC_BIN` point at explicit paths when
//! running outside an activated mise shell. No Docker anywhere.
#![allow(dead_code, unused_imports)] // each test crate uses a subset of this harness

pub mod minio;
pub mod nats;

#[cfg(feature = "transport")]
pub mod crash;

#[cfg(feature = "transport")]
pub use crash::ManifestPutCrash;
pub use minio::{MINIO_BUCKET, MINIO_PASSWORD, MINIO_USER, TestMinio};
pub use nats::{TestNats, free_port};
