//! A throwaway `minio` with a pre-created bucket, one per test, killed on
//! drop. `MINIO_BIN` / `MC_BIN` point at explicit paths outside an activated
//! mise shell.

use std::process::{Child, Command, Stdio};
use std::time::Duration;

use tempfile::TempDir;

use super::nats::free_port;

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
