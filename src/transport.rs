//! Artifact transport: ship export artifacts to object storage and fetch them
//! back for bootstrap. Feature `transport`.
//!
//! The wire format is a **plain tar** of the artifact directory
//! (`MANIFEST.json` + `data/…`) at `<prefix>/<key>`, with the manifest
//! duplicated as a sibling object `<key>.manifest.json` so a node can peek at
//! an artifact's cursor/backend without downloading the payload. No
//! compression layer: fjall/RocksDB payload files are already lz4/zstd
//! compressed. The sibling manifest is uploaded **last**, so its presence
//! means the payload object is complete — the remote twin of the local
//! artifact's manifest-written-last discipline.
//!
//! Transport is **untrusted**: [`download`](ArtifactTransport::download)
//! cross-checks the tar's embedded manifest against the sibling object, and
//! the backend `import` re-verifies every payload file hash regardless.
//!
//! [`run_export_round`] composes the whole at-most-once round:
//! lease → export (through the [`watch_applied`](crate::watch_applied)
//! [`ExportRequest`] channel) → upload → publish completion → **delete the
//! local artifact** (artifacts hardlink fold files and pin storage if they
//! linger — transience is enforced here, not hoped for).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, WriteMultipart};
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, oneshot};
use tracing::warn;

use crate::applied::ExportRequest;
use crate::artifact::{ExportManifest, MANIFEST_FILE, check_dest_available, manifest_from_slice};
use crate::export_lease::ExportLease;
use crate::kv::WatchCursor;
use crate::snapshot::SnapshotError;

/// Buffered chunk size for uploads/downloads (also the multipart part size).
/// 8 MiB clears S3's 5 MiB minimum part size with headroom.
const CHUNK: usize = 8 << 20;

/// Concurrent in-flight multipart parts (bounds upload memory to
/// `CHUNK × MAX_CONCURRENT_PARTS`).
const MAX_CONCURRENT_PARTS: usize = 8;

/// Ship artifacts to durable storage and fetch them back. See the module docs
/// for the wire format.
#[async_trait]
pub trait ArtifactTransport: Send + Sync {
    /// Tar `artifact_dir` and upload it at `key`, then upload the manifest as
    /// the sibling object `<key>.manifest.json` (the completeness marker).
    /// Re-uploading the same `key` overwrites — "latest" keys are
    /// last-write-wins by design.
    async fn upload(&self, key: &str, artifact_dir: &Path) -> Result<(), SnapshotError>;

    /// Fetch only the sibling manifest — peek at an artifact's cursor and
    /// backend before committing to a payload download.
    async fn manifest(&self, key: &str) -> Result<ExportManifest, SnapshotError>;

    /// Download and unpack the artifact at `key` into `dest_dir` (which must
    /// not exist or be an empty directory), returning its manifest. The
    /// unpacked directory is a local artifact, ready for the backend's
    /// `import` — which re-verifies every file hash; this method only
    /// cross-checks the embedded manifest against the sibling object.
    async fn download(&self, key: &str, dest_dir: &Path) -> Result<ExportManifest, SnapshotError>;
}

/// [`ArtifactTransport`] over any [`object_store::ObjectStore`] — S3, GCS,
/// Azure, or local filesystem — under a key prefix.
pub struct ObjectStoreTransport {
    store: Arc<dyn ObjectStore>,
    prefix: ObjPath,
}

impl ObjectStoreTransport {
    /// Wrap an already-configured store. Keys are placed under `prefix`.
    pub fn new(store: Arc<dyn ObjectStore>, prefix: impl AsRef<str>) -> Self {
        Self {
            store,
            prefix: ObjPath::from(prefix.as_ref()),
        }
    }

    /// Build from a URL (`s3://bucket/prefix`, `file:///path`, …) plus
    /// explicit builder options (e.g. `aws_endpoint`, `aws_access_key_id`,
    /// `aws_virtual_hosted_style_request`).
    ///
    /// NOTE: [`object_store::parse_url_opts`] does **not** read process env
    /// vars — every non-default setting (credentials, endpoint, path-style)
    /// must be passed in `options`. To use env-based configuration, build the
    /// store yourself (e.g. `AmazonS3Builder::from_env()`) and use
    /// [`new`](Self::new).
    pub fn from_url_opts<I, K, V>(url: &str, options: I) -> Result<Self, SnapshotError>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<String>,
    {
        let url = url::Url::parse(url)
            .map_err(|e| SnapshotError::Backend(format!("invalid transport url: {e}")))?;
        let (store, prefix) = object_store::parse_url_opts(&url, options).map_err(map_obj)?;
        Ok(Self {
            store: Arc::from(store),
            prefix,
        })
    }

    // `Path::from` parses `/` separators, so multi-segment keys
    // (`edge-origins/us-east/latest`) land as real object hierarchy.
    fn payload_path(&self, key: &str) -> ObjPath {
        ObjPath::from(format!("{}/{key}", self.prefix))
    }

    fn manifest_path(&self, key: &str) -> ObjPath {
        ObjPath::from(format!("{}/{key}.manifest.json", self.prefix))
    }
}

#[async_trait]
impl ArtifactTransport for ObjectStoreTransport {
    async fn upload(&self, key: &str, artifact_dir: &Path) -> Result<(), SnapshotError> {
        // Read the manifest first — it doubles as the artifact-completeness
        // check (export writes it last).
        let manifest_bytes = tokio::fs::read(artifact_dir.join(MANIFEST_FILE))
            .await
            .map_err(SnapshotError::Io)?;
        // Validate before shipping: never upload an artifact we couldn't read back.
        manifest_from_slice(&manifest_bytes)?;

        // Tar the artifact into a temp file on a blocking task. A temp file
        // (rather than streaming the tar straight into the upload) keeps the
        // blocking tar writer and the async multipart writer decoupled; the
        // disk cost is one tar's worth, transient.
        let src = artifact_dir.to_path_buf();
        let tar_file = tokio::task::spawn_blocking(
            move || -> Result<tempfile::NamedTempFile, SnapshotError> {
                let tmp = tempfile::NamedTempFile::new()?;
                let mut builder = tar::Builder::new(std::io::BufWriter::new(tmp.reopen()?));
                builder.append_dir_all(".", &src)?;
                builder
                    .into_inner()?
                    .into_inner()
                    .map_err(|e| SnapshotError::Io(e.into_error()))?;
                Ok(tmp)
            },
        )
        .await
        .map_err(|e| SnapshotError::Backend(format!("tar task panicked: {e}")))??;

        // Stream the tar up as multipart, memory-bounded.
        let mut file = tokio::fs::File::open(tar_file.path())
            .await
            .map_err(SnapshotError::Io)?;
        let upload = self
            .store
            .put_multipart(&self.payload_path(key))
            .await
            .map_err(map_obj)?;
        let mut wm = WriteMultipart::new_with_chunk_size(upload, CHUNK);
        let mut buf = vec![0u8; CHUNK];
        loop {
            use tokio::io::AsyncReadExt;
            let n = file.read(&mut buf).await.map_err(SnapshotError::Io)?;
            if n == 0 {
                break;
            }
            wm.wait_for_capacity(MAX_CONCURRENT_PARTS)
                .await
                .map_err(map_obj)?;
            wm.write(&buf[..n]);
        }
        wm.finish().await.map_err(map_obj)?;

        // Manifest sibling LAST: its presence marks the payload complete.
        self.store
            .put(&self.manifest_path(key), PutPayload::from(manifest_bytes))
            .await
            .map_err(map_obj)?;
        Ok(())
    }

    async fn manifest(&self, key: &str) -> Result<ExportManifest, SnapshotError> {
        let bytes = self
            .store
            .get(&self.manifest_path(key))
            .await
            .map_err(map_obj)?
            .bytes()
            .await
            .map_err(map_obj)?;
        manifest_from_slice(&bytes)
    }

    async fn download(&self, key: &str, dest_dir: &Path) -> Result<ExportManifest, SnapshotError> {
        check_dest_available(dest_dir)?;
        let parent = dest_dir
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .ok_or_else(|| {
                SnapshotError::ArtifactInvalid(format!(
                    "destination {} has no parent directory",
                    dest_dir.display()
                ))
            })?;

        // Sibling manifest first — it is the completeness marker and the value
        // we cross-check the tar against.
        let sibling = self
            .store
            .get(&self.manifest_path(key))
            .await
            .map_err(map_obj)?
            .bytes()
            .await
            .map_err(map_obj)?;
        let manifest = manifest_from_slice(&sibling)?;

        // Stream the tar to a temp file.
        let tar_tmp = tempfile::NamedTempFile::new_in(parent)?;
        let mut tar_writer = tokio::fs::File::create(tar_tmp.path())
            .await
            .map_err(SnapshotError::Io)?;
        let mut stream = self
            .store
            .get(&self.payload_path(key))
            .await
            .map_err(map_obj)?
            .into_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(map_obj)?;
            tar_writer
                .write_all(&chunk)
                .await
                .map_err(SnapshotError::Io)?;
        }
        tar_writer.flush().await.map_err(SnapshotError::Io)?;
        drop(tar_writer);

        // Unpack into a stage beside the destination, cross-check the embedded
        // manifest against the sibling, and atomically rename into place — all
        // blocking work, offloaded.
        let dest = dest_dir.to_path_buf();
        let parent = parent.to_path_buf();
        tokio::task::spawn_blocking(move || -> Result<(), SnapshotError> {
            let stage = tempfile::Builder::new()
                .prefix(".slipstream-download-")
                .tempdir_in(&parent)?;
            let file = std::fs::File::open(tar_tmp.path())?;
            let mut archive = tar::Archive::new(std::io::BufReader::new(file));
            // tar's unpack refuses entries that escape the destination, on top
            // of the manifest path validation import performs again.
            archive.unpack(stage.path())?;

            let embedded = std::fs::read(stage.path().join(MANIFEST_FILE)).map_err(|_| {
                SnapshotError::ArtifactInvalid(
                    "downloaded artifact tar has no embedded manifest".into(),
                )
            })?;
            if embedded != sibling.as_ref() {
                return Err(SnapshotError::ArtifactInvalid(
                    "embedded manifest disagrees with the sibling manifest object".into(),
                ));
            }

            check_dest_available(&dest)?;
            if dest.is_dir() {
                std::fs::remove_dir(&dest)?;
            }
            let root = stage.keep();
            std::fs::rename(&root, &dest)?;
            Ok(())
        })
        .await
        .map_err(|e| SnapshotError::Backend(format!("untar task panicked: {e}")))??;

        Ok(manifest)
    }
}

fn map_obj(e: object_store::Error) -> SnapshotError {
    match e {
        object_store::Error::NotFound { path, .. } => {
            SnapshotError::ArtifactInvalid(format!("remote artifact object not found: {path}"))
        }
        other => SnapshotError::Backend(format!("object store: {other}")),
    }
}

// ---------------------------------------------------------------------------
// The composed round
// ---------------------------------------------------------------------------

/// Run one complete at-most-once export round:
///
/// 1. [`ExportLease::try_acquire`] — `Ok(None)` means another node owns this
///    round; nothing else happens.
/// 2. An [`ExportRequest`] into the live [`watch_applied`](crate::watch_applied)
///    loop (pending batch flushed first; artifact cursor == applied cursor).
/// 3. [`ArtifactTransport::upload`].
/// 4. [`LeaseGuard::complete`](crate::LeaseGuard::complete) — only after the
///    upload succeeded, so a published completion never lies.
/// 5. Delete the local artifact (transience: artifacts hardlink fold files and
///    pin storage if they linger).
///
/// On any failure after winning the lease, the artifact is cleaned up and the
/// lease **abandoned** so the fleet can retry promptly instead of waiting out
/// the ttl. `scratch_dir` must exist and should be on the same filesystem as
/// the fold (the export stages beside it; hardlinks degrade to copies across
/// filesystems).
pub async fn run_export_round(
    lease: &ExportLease,
    ttl: Duration,
    exports: &mpsc::Sender<ExportRequest>,
    transport: &dyn ArtifactTransport,
    key: &str,
    scratch_dir: &Path,
) -> Result<Option<ExportManifest>, SnapshotError> {
    let Some(guard) = lease
        .try_acquire(ttl)
        .await
        .map_err(|e| SnapshotError::Backend(format!("export lease: {e}")))?
    else {
        return Ok(None);
    };

    // The artifact lives inside a TempDir for the duration of the round —
    // dropped on every path out of this function, success or failure.
    let round_dir = match tempfile::Builder::new()
        .prefix(".slipstream-export-round-")
        .tempdir_in(scratch_dir)
    {
        Ok(d) => d,
        Err(e) => {
            guard.abandon().await;
            return Err(SnapshotError::Io(e));
        }
    };
    let artifact_dir = round_dir.path().join("artifact");

    // Export through the watch loop.
    let (reply_tx, reply_rx) = oneshot::channel();
    let request = ExportRequest {
        dest_dir: artifact_dir.clone(),
        reply: reply_tx,
    };
    if exports.send(request).await.is_err() {
        guard.abandon().await;
        return Err(SnapshotError::Backend(
            "watch loop is gone; export request not delivered".into(),
        ));
    }
    let manifest = match reply_rx.await {
        Ok(Ok(m)) => m,
        Ok(Err(e)) => {
            guard.abandon().await;
            return Err(e);
        }
        Err(_) => {
            guard.abandon().await;
            return Err(SnapshotError::Backend(
                "watch loop dropped the export reply".into(),
            ));
        }
    };

    // Upload; only then publish completion.
    if let Err(e) = transport.upload(key, &artifact_dir).await {
        guard.abandon().await;
        return Err(e);
    }
    if let Err(e) = guard.complete(&manifest.cursor).await {
        // The artifact IS uploaded — the round succeeded. Losing the
        // completion record costs observability, not correctness.
        warn!(key, error = %e, "export round uploaded but completion record failed");
    }

    drop(round_dir); // enforce artifact transience
    Ok(Some(manifest))
}

// ---------------------------------------------------------------------------
// Per-backend remote-import conveniences
// ---------------------------------------------------------------------------

/// Download `key` into a throwaway dir under `scratch_dir`, returning the
/// artifact path and the guard keeping it alive.
async fn download_to_scratch(
    transport: &dyn ArtifactTransport,
    key: &str,
    scratch_dir: &Path,
) -> Result<(tempfile::TempDir, PathBuf), SnapshotError> {
    let tmp = tempfile::Builder::new()
        .prefix(".slipstream-bootstrap-")
        .tempdir_in(scratch_dir)?;
    let artifact = tmp.path().join("artifact");
    transport.download(key, &artifact).await?;
    Ok((tmp, artifact))
}

impl crate::AppendLogSnapshot {
    /// Fetch the artifact at `key` and import it as a new fold at `dest_path`
    /// (download → full verification → open), resuming from the embedded
    /// cursor. The downloaded artifact is deleted afterwards.
    pub async fn import_remote(
        transport: &dyn ArtifactTransport,
        key: &str,
        scratch_dir: &Path,
        dest_path: &Path,
        compact_threshold: u64,
    ) -> Result<(WatchCursor, Self), SnapshotError> {
        let (_guard, artifact) = download_to_scratch(transport, key, scratch_dir).await?;
        let dest = dest_path.to_path_buf();
        tokio::task::spawn_blocking(move || Self::import(&artifact, &dest, compact_threshold))
            .await
            .map_err(|e| SnapshotError::Backend(format!("import task panicked: {e}")))?
    }
}

#[cfg(feature = "fjall")]
impl crate::FjallSnapshot {
    /// Fetch the artifact at `key` and import it as a new fold at `dest_dir`
    /// (download → full verification → verify-open → rename), resuming from
    /// the embedded cursor. The downloaded artifact is deleted afterwards.
    pub async fn import_remote(
        transport: &dyn ArtifactTransport,
        key: &str,
        scratch_dir: &Path,
        dest_dir: &Path,
        config: crate::FjallConfig,
    ) -> Result<(WatchCursor, Self), SnapshotError> {
        let (_guard, artifact) = download_to_scratch(transport, key, scratch_dir).await?;
        let dest = dest_dir.to_path_buf();
        tokio::task::spawn_blocking(move || Self::import(&artifact, &dest, config))
            .await
            .map_err(|e| SnapshotError::Backend(format!("import task panicked: {e}")))?
    }
}

#[cfg(feature = "rocksdb")]
impl crate::RocksDbSnapshot {
    /// Fetch the artifact at `key` and import it as a new fold at `dest_dir`
    /// (download → full verification → verify-open → rename), resuming from
    /// the embedded cursor. The downloaded artifact is deleted afterwards.
    pub async fn import_remote(
        transport: &dyn ArtifactTransport,
        key: &str,
        scratch_dir: &Path,
        dest_dir: &Path,
        config: crate::RocksDbConfig,
    ) -> Result<(WatchCursor, Self), SnapshotError> {
        let (_guard, artifact) = download_to_scratch(transport, key, scratch_dir).await?;
        let dest = dest_dir.to_path_buf();
        tokio::task::spawn_blocking(move || Self::import(&artifact, &dest, config))
            .await
            .map_err(|e| SnapshotError::Backend(format!("import task panicked: {e}")))?
    }
}
