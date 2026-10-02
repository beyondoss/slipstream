//! A throwaway `nats-server` with JetStream, one per test, killed on drop.
//!
//! THE one copy: the integration tests reach it through `tests/common`, and
//! the crate's own unit tests through `#[path]` (`src/nats.rs`), so it
//! depends only on crates both can see (`async-nats`, `tempfile`, `tokio`).
//! `nats-server` comes from mise (`mise install`); `NATS_SERVER_BIN` points
//! at an explicit path outside an activated mise shell.
#![allow(dead_code)] // each test crate uses a subset of this harness

use std::process::{Child, Command, Stdio};
use std::time::Duration;

use tempfile::TempDir;

pub fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral port")
        .local_addr()
        .expect("read local addr")
        .port()
}

/// A running `nats-server` with JetStream enabled. Killed on drop.
pub struct TestNats {
    child: Child,
    pub url: String,
    _store_dir: TempDir,
}

impl TestNats {
    pub async fn start() -> TestNats {
        let bin = std::env::var("NATS_SERVER_BIN").unwrap_or_else(|_| "nats-server".to_string());
        let port = free_port();
        let store_dir = tempfile::tempdir().expect("create jetstream store dir");
        let child = Command::new(&bin)
            .args([
                "--jetstream",
                "--addr",
                "127.0.0.1",
                "--port",
                &port.to_string(),
                "--store_dir",
                store_dir.path().to_str().expect("utf-8 store path"),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap_or_else(|e| {
                panic!(
                    "failed to spawn `{bin}`: {e}. Is nats-server installed? \
                     Run `mise install` or set NATS_SERVER_BIN."
                )
            });
        // Build the guard FIRST: if readiness polling panics, Drop reaps the
        // child instead of leaking a zombie.
        let guard = TestNats {
            child,
            url: format!("nats://127.0.0.1:{port}"),
            _store_dir: store_dir,
        };
        for _ in 0..100 {
            if async_nats::connect(&guard.url).await.is_ok() {
                return guard;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("nats-server at {} never became ready", guard.url);
    }
}

impl Drop for TestNats {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
