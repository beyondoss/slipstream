//! Shared integration-test infrastructure: throwaway `nats-server` and
//! `minio` instances, one per test, killed on drop.
//!
//! All binaries come from mise (`mise install`); env overrides
//! `NATS_SERVER_BIN` / `MINIO_BIN` / `MC_BIN` point at explicit paths when
//! running outside an activated mise shell. No Docker anywhere.
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

// --- NATS ----------------------------------------------------------------------

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

// --- MinIO ---------------------------------------------------------------------

pub const MINIO_USER: &str = "minioadmin";
pub const MINIO_PASSWORD: &str = "minioadmin";
pub const MINIO_BUCKET: &str = "test-bucket";

/// A running `minio` with [`MINIO_BUCKET`] pre-created via `mc`. Killed on
/// drop. The `mc mb` retry loop doubles as the readiness probe.
pub struct TestMinio {
    child: Child,
    pub endpoint: String,
    _data_dir: TempDir,
}

impl TestMinio {
    pub async fn start() -> TestMinio {
        let minio_bin = std::env::var("MINIO_BIN").unwrap_or_else(|_| "minio".to_string());
        let mc_bin = std::env::var("MC_BIN").unwrap_or_else(|_| "mc".to_string());
        let api_port = free_port();
        let console_port = free_port();
        let data_dir = tempfile::tempdir().expect("create minio data dir");

        let child = Command::new(&minio_bin)
            .args([
                "server",
                data_dir.path().to_str().expect("utf-8 data path"),
                "--address",
                &format!("127.0.0.1:{api_port}"),
                "--console-address",
                &format!("127.0.0.1:{console_port}"),
            ])
            .env("MINIO_ROOT_USER", MINIO_USER)
            .env("MINIO_ROOT_PASSWORD", MINIO_PASSWORD)
            .env("MINIO_BROWSER", "off")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap_or_else(|e| {
                panic!(
                    "failed to spawn `{minio_bin}`: {e}. Is minio installed? \
                     Run `mise install` or set MINIO_BIN."
                )
            });

        // Build the guard FIRST so a panicking readiness loop reaps the child.
        let guard = TestMinio {
            child,
            endpoint: format!("http://127.0.0.1:{api_port}"),
            _data_dir: data_dir,
        };
        let endpoint = guard.endpoint.clone();

        // Create the test bucket with mc, retrying until the server is up.
        // A throwaway --config-dir keeps ~/.mc untouched.
        let mc_cfg = tempfile::tempdir().expect("create mc config dir");
        let cfg = mc_cfg.path().to_str().expect("utf-8 mc config path");
        let mut ready = false;
        for _ in 0..100 {
            let alias = Command::new(&mc_bin)
                .args([
                    "--config-dir",
                    cfg,
                    "alias",
                    "set",
                    "t",
                    &endpoint,
                    MINIO_USER,
                    MINIO_PASSWORD,
                ])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .unwrap_or_else(|e| panic!("failed to spawn `{mc_bin}`: {e}. Run `mise install`."));
            if alias.success() {
                let mb = Command::new(&mc_bin)
                    .args([
                        "--config-dir",
                        cfg,
                        "mb",
                        "--ignore-existing",
                        &format!("t/{MINIO_BUCKET}"),
                    ])
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status()
                    .expect("run mc mb");
                if mb.success() {
                    ready = true;
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        assert!(
            ready,
            "minio at {endpoint} never became ready (mc mb failed)"
        );
        guard
    }

    /// Builder options for `ObjectStoreTransport::from_url_opts` pointing at
    /// this instance (path-style, http, root credentials).
    pub fn s3_options(&self) -> Vec<(&'static str, String)> {
        vec![
            ("aws_access_key_id", MINIO_USER.to_string()),
            ("aws_secret_access_key", MINIO_PASSWORD.to_string()),
            ("aws_endpoint", self.endpoint.clone()),
            ("aws_allow_http", "true".to_string()),
            ("aws_region", "us-east-1".to_string()),
        ]
    }
}

impl Drop for TestMinio {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

// --- Crash injection for the transport tiers ---------------------------------

/// An [`object_store::ObjectStore`] that, once armed, fails the next PUT of a
/// `*.manifest.json` object — the process crash in the window between
/// `upload`'s payload multipart (already complete) and its sibling-manifest
/// PUT. Everything else delegates to the wrapped store.
#[derive(Debug)]
pub struct ManifestPutCrash {
    pub inner: std::sync::Arc<dyn object_store::ObjectStore>,
    pub armed: std::sync::atomic::AtomicBool,
}

impl std::fmt::Display for ManifestPutCrash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ManifestPutCrash({})", self.inner)
    }
}

#[async_trait::async_trait]
impl object_store::ObjectStore for ManifestPutCrash {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        if location.as_ref().ends_with(".manifest.json")
            && self.armed.swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(object_store::Error::Generic {
                store: "ManifestPutCrash",
                source: "injected crash between payload and manifest PUT".into(),
            });
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<
            'static,
            object_store::Result<object_store::path::Path>,
        >,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
