//! Artifact transport: ship export artifacts to object storage and fetch them
//! back for bootstrap. Feature `transport`.
//!
//! ## Wire format: content-addressed payload + monotonic pointer
//!
//! The payload is a **plain tar** of the artifact directory (`MANIFEST.json`
//! plus `data/…`) at a **content-addressed** key —
//! `<prefix>/<key>.payloads/<blake3(manifest)[..8] hex>.tar` — so payload
//! objects are write-once: two different artifacts can never collide on a
//! key, and re-uploading the same artifact is an idempotent overwrite. No
//! compression layer: fjall/RocksDB payload files are already lz4/zstd
//! compressed.
//!
//! The manifest doubles as the **pointer**: it is published at
//! `<prefix>/<key>.manifest.json` LAST, via a conditional put (create-only or
//! compare-and-swap on the object version) that only ever moves the cursor
//! FORWARD. This single atomic object is what readers trust; the payload key
//! is derived from its bytes. The discipline is machine-checked: the pointer
//! protocol is the `pointer_swap` model in `tests/model.rs`, where the
//! checker proves torn payload/pointer pairs and cursor regression are
//! structurally impossible — the two hazards the legacy two-register layout
//! (payload and manifest as independent last-write-wins objects at fixed
//! keys) provably had.
//!
//! Consequences, each pinned by `tests/multi_export.rs`:
//! - A slow exporter whose round overran its lease CANNOT clobber a newer
//!   published artifact: its swap is refused
//!   ([`PublishOutcome::SupersededByNewer`]).
//! - A crash between the payload upload and the pointer swap leaves the OLD
//!   pointer fully consistent — bootstrap stays available throughout.
//!
//! Old payload objects linger after their pointer moves on; [`run_export_round`]
//! prunes unreferenced payloads older than a grace period (never the one the
//! current pointer targets, and never young objects a concurrent publisher or
//! an in-flight bootstrap may still reference).
//!
//! Transport is **untrusted**: [`download`](ArtifactTransport::download)
//! cross-checks the tar's embedded manifest against the pointer bytes (the
//! content address makes a mismatch unreachable short of hash breakage or
//! store corruption — kept as defense in depth), and the backend `import`
//! re-verifies every payload file hash regardless.
//!
//! [`run_export_round`] composes the whole at-most-once round:
//! lease → export (through the [`watch_applied`](crate::watch_applied)
//! [`ExportRequest`] channel) → upload + pointer swap → publish completion →
//! prune stale payloads → **delete the local artifact** (artifacts hardlink
//! fold files and pin storage if they linger — transience is enforced here,
//! not hoped for).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use object_store::path::Path as ObjPath;
use object_store::{
    ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion, WriteMultipart,
};
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, oneshot};
use tracing::warn;

use crate::applied::ExportRequest;
use crate::artifact::{
    ExportManifest, MANIFEST_FILE, check_dest_available, hex_encode, manifest_from_slice,
    rename_into_place,
};
use crate::export_lease::ExportLease;
use crate::kv::WatchCursor;
use crate::protocol::{PointerState, payload_prunable, pointer_publish_allowed};
use crate::snapshot::SnapshotError;

/// Buffered chunk size for uploads/downloads (also the multipart part size).
/// 8 MiB clears S3's 5 MiB minimum part size with headroom.
const CHUNK: usize = 8 << 20;

/// Concurrent in-flight multipart parts (bounds upload memory to
/// `CHUNK × MAX_CONCURRENT_PARTS`).
const MAX_CONCURRENT_PARTS: usize = 8;

/// Cap on a sibling-manifest object's size. The transport is untrusted, and a
/// manifest read buffers the whole object — without a cap, a hostile or
/// corrupted object at the manifest key is an OOM vector. Real manifests are
/// a few KB per payload file; 1 MiB is orders of magnitude of headroom.
const MAX_MANIFEST_BYTES: usize = 1 << 20;

/// Per-await timeout on every object-store operation (each request, chunk, or
/// part — not the whole transfer, which is legitimately unbounded for large
/// artifacts). A half-dead TCP connection otherwise parks the `await` forever;
/// same hazard and same 30 s bound as the NATS layer's `timed()`.
const OP_TIMEOUT: Duration = Duration::from_secs(30);

/// Bound on pointer-swap CAS retries. Each retry means another publisher won
/// the race in the read→swap window; with rounds minutes apart, more than a
/// couple of iterations indicates something pathological, and an unbounded
/// loop would livelock against it.
const MAX_SWAP_ATTEMPTS: usize = 8;

/// Outcome of [`ArtifactTransport::upload`]'s pointer swap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublishOutcome {
    /// This artifact is now the published "latest": its payload is uploaded
    /// and the pointer references it.
    Published,
    /// The pointer already references a STRICTLY newer artifact, so the
    /// monotonic swap refused to regress it. This round's payload was
    /// uploaded but is unreferenced (the next prune collects it). Routine
    /// under lease overrun: the fleet has moved on, nothing was lost.
    SupersededByNewer {
        /// The newer published artifact's cursor.
        remote_cursor: crate::kv::WatchCursor,
    },
}

/// Total order for the monotonic pointer guard. Revisionless cursors rank 0:
/// a real cursor always supersedes an empty one, never the reverse.
fn cursor_rank(c: &WatchCursor) -> u64 {
    c.as_u64().unwrap_or(0)
}

/// Bound one object-store await by `limit`.
async fn timed_by<T>(
    what: &str,
    limit: Duration,
    fut: impl std::future::Future<Output = T>,
) -> Result<T, SnapshotError> {
    tokio::time::timeout(limit, fut).await.map_err(|_| {
        SnapshotError::Backend(format!(
            "object store: {what} timed out after {}s",
            limit.as_secs()
        ))
    })
}

/// Bound one object-store await by [`OP_TIMEOUT`].
async fn timed<T>(
    what: &str,
    fut: impl std::future::Future<Output = T>,
) -> Result<T, SnapshotError> {
    timed_by(what, OP_TIMEOUT, fut).await
}

/// Ship artifacts to durable storage and fetch them back. See the module docs
/// for the wire format.
#[async_trait]
pub trait ArtifactTransport: Send + Sync {
    /// Tar `artifact_dir`, upload it at its content-addressed payload key,
    /// then publish the manifest as the pointer `<key>.manifest.json` via a
    /// monotonic conditional swap. The pointer only ever moves the cursor
    /// forward: an older artifact gets
    /// [`PublishOutcome::SupersededByNewer`], never a regression.
    async fn upload(&self, key: &str, artifact_dir: &Path)
    -> Result<PublishOutcome, SnapshotError>;

    /// Fetch only the pointer manifest — peek at an artifact's cursor and
    /// backend before committing to a payload download.
    async fn manifest(&self, key: &str) -> Result<ExportManifest, SnapshotError>;

    /// Delete payload objects under `key` that the current pointer does not
    /// reference and that are older than `grace`. Best-effort housekeeping —
    /// returns the number deleted. The grace period protects concurrent
    /// publishers mid-swap and in-flight bootstraps holding an older pointer
    /// read. Default: no-op (transports with nothing to prune).
    async fn prune(&self, key: &str, grace: Duration) -> Result<usize, SnapshotError> {
        let _ = (key, grace);
        Ok(0)
    }

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
    /// Accept stores without conditional-put support (see
    /// [`with_non_atomic_pointer_fallback`](Self::with_non_atomic_pointer_fallback)).
    allow_non_atomic_pointer: bool,
}

impl ObjectStoreTransport {
    /// Wrap an already-configured store. Keys are placed under `prefix`.
    pub fn new(store: Arc<dyn ObjectStore>, prefix: impl AsRef<str>) -> Self {
        Self {
            store,
            prefix: ObjPath::from(prefix.as_ref()),
            allow_non_atomic_pointer: false,
        }
    }

    /// Accept stores that lack conditional puts (`PutMode::Update` →
    /// `NotImplemented`, e.g. `object_store`'s `LocalFileSystem`): the
    /// pointer publish degrades to read-check-then-unconditional-put.
    ///
    /// OUTSIDE the verified protocol: the monotonic refusal still runs, but
    /// the write is not atomic, so two publishers racing the read→write
    /// window are last-write-wins — the legacy regression hazard
    /// (`tests/model.rs`, legacy configuration) survives on such a store.
    /// Without this opt-in, a swap on a non-CAS store FAILS the round
    /// instead of silently degrading. Dev/test `file://` use only; every
    /// deployment-grade store (S3/GCS/Azure/MinIO/R2) supports the atomic
    /// path.
    pub fn with_non_atomic_pointer_fallback(mut self) -> Self {
        self.allow_non_atomic_pointer = true;
        self
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
            allow_non_atomic_pointer: false,
        })
    }

    // `Path::from` parses `/` separators, so multi-segment keys
    // (`edge-origins/us-east/latest`) land as real object hierarchy.
    //
    /// The directory of `key`'s content-addressed payload objects.
    fn payloads_dir(&self, key: &str) -> ObjPath {
        ObjPath::from(format!("{}/{key}.payloads", self.prefix))
    }

    /// A payload object's address: `<cursor, 16 hex>-<blake3(manifest)[..8],
    /// 16 hex>.tar`.
    ///
    /// The hash half is the content address (the manifest embeds a BLAKE3
    /// digest per payload file, so it commits to the artifact's content; 64
    /// bits — a birthday collision needs ~2^32 artifacts under ONE key, and
    /// even then the embedded-manifest cross-check at download detects the
    /// mix). The cursor half is LOAD-BEARING for prune: it lets prune apply
    /// the strictly-below-the-pointer rule without fetching each payload
    /// (see [`prune`](Self::prune) — the rule is what makes a
    /// pruned-then-published dangling pointer structurally impossible, a
    /// hazard the model checker found at zero grace).
    fn payload_path(&self, key: &str, cursor: &WatchCursor, pointer_bytes: &[u8]) -> ObjPath {
        let digest = blake3::hash(pointer_bytes);
        let hex = hex_encode(&digest.as_bytes()[..8]);
        ObjPath::from(format!(
            "{}/{key}.payloads/{:016x}-{hex}.tar",
            self.prefix,
            cursor_rank(cursor)
        ))
    }

    fn manifest_path(&self, key: &str) -> ObjPath {
        ObjPath::from(format!("{}/{key}.manifest.json", self.prefix))
    }

    /// Publish `pointer_bytes` (the manifest) at the pointer key with a
    /// monotonic conditional swap: create-only when absent, compare-and-swap
    /// against the observed object version when present, and REFUSED when
    /// the present pointer's cursor is strictly newer. An unparseable
    /// pointer is replaced (CAS against its version) — one corrupt object
    /// must not wedge publishing, mirroring the lease's corrupt-steal rule.
    ///
    /// This is the `Publish` transition of the `pointer_swap` model in
    /// `tests/model.rs`; the conditional-put semantics it relies on are
    /// verified against real S3 (MinIO) by `tests/transport_s3.rs`.
    async fn swap_pointer(
        &self,
        key: &str,
        new_cursor: &WatchCursor,
        pointer_bytes: &[u8],
    ) -> Result<PublishOutcome, SnapshotError> {
        let path = self.manifest_path(key);
        for _ in 0..MAX_SWAP_ATTEMPTS {
            // Read the current pointer (bytes + object version for the CAS).
            let current = match timed("pointer get", self.store.get(&path)).await? {
                Ok(get) => {
                    let meta = get.meta.clone();
                    let mut stream = get.into_stream();
                    let mut buf = Vec::new();
                    while let Some(chunk) = timed("pointer read", stream.next()).await? {
                        let chunk = chunk.map_err(map_obj)?;
                        if buf.len() + chunk.len() > MAX_MANIFEST_BYTES {
                            return Err(SnapshotError::ArtifactInvalid(format!(
                                "remote pointer for {key:?} exceeds {MAX_MANIFEST_BYTES} bytes"
                            )));
                        }
                        buf.extend_from_slice(&chunk);
                    }
                    Some((meta, buf))
                }
                Err(object_store::Error::NotFound { .. }) => None,
                Err(e) => return Err(map_obj(e)),
            };

            match current {
                None => {
                    // Open slot: create-only — exactly one concurrent
                    // publisher can win it.
                    let opts = PutOptions::from(PutMode::Create);
                    match timed(
                        "pointer create",
                        self.store
                            .put_opts(&path, PutPayload::from(pointer_bytes.to_vec()), opts),
                    )
                    .await?
                    {
                        Ok(_) => return Ok(PublishOutcome::Published),
                        Err(object_store::Error::AlreadyExists { .. }) => continue, // lost the race; re-read
                        Err(e) => return Err(map_obj(e)),
                    }
                }
                Some((meta, bytes)) => {
                    // THE monotonic guard — the shared protocol kernel, the
                    // same function the model checker's Publish transition
                    // executes (`crate::protocol`). An unparseable pointer is
                    // rank-less and replaced.
                    let existing = manifest_from_slice(&bytes).ok();
                    let observed = PointerState::Present {
                        rank: existing.as_ref().map(|m| cursor_rank(&m.cursor)),
                    };
                    if !pointer_publish_allowed(&observed, cursor_rank(new_cursor)) {
                        let existing =
                            existing.expect("refusal implies a parseable, newer pointer");
                        return Ok(PublishOutcome::SupersededByNewer {
                            remote_cursor: existing.cursor,
                        });
                    }
                    let opts = PutOptions::from(PutMode::Update(UpdateVersion {
                        e_tag: meta.e_tag,
                        version: meta.version,
                    }));
                    match timed(
                        "pointer swap",
                        self.store
                            .put_opts(&path, PutPayload::from(pointer_bytes.to_vec()), opts),
                    )
                    .await?
                    {
                        Ok(_) => return Ok(PublishOutcome::Published),
                        Err(object_store::Error::Precondition { .. }) => continue, // raced; re-read
                        Err(object_store::Error::NotFound { .. }) => continue, // deleted under us; re-read
                        Err(object_store::Error::NotImplemented { .. }) => {
                            // Store without compare-and-swap (object_store's
                            // LocalFileSystem). FAIL CLOSED unless the caller
                            // explicitly opted in: a silently degraded swap
                            // would reintroduce the legacy regression hazard
                            // (tests/model.rs proves it reachable) with a log
                            // line as the only witness — the same violated-
                            // obligation pattern the resync fail-stop change
                            // eliminated.
                            if !self.allow_non_atomic_pointer {
                                return Err(SnapshotError::Backend(format!(
                                    "object store lacks conditional puts (PutMode::Update \
                                     unimplemented); the pointer swap for {key:?} cannot be \
                                     atomic and this store is outside the verified protocol. \
                                     For dev/test stores (file://), opt in explicitly with \
                                     ObjectStoreTransport::with_non_atomic_pointer_fallback()"
                                )));
                            }
                            // Opted in: the monotonic REFUSAL above still
                            // ran, but the write is unconditional — two
                            // publishers racing this window are
                            // last-write-wins, on this store only.
                            warn!(
                                key,
                                "non-atomic pointer fallback (explicit opt-in): publish is \
                                 read-check-then-put; concurrent publishers may race"
                            );
                            timed(
                                "pointer put (non-atomic fallback)",
                                self.store
                                    .put(&path, PutPayload::from(pointer_bytes.to_vec())),
                            )
                            .await?
                            .map_err(map_obj)?;
                            return Ok(PublishOutcome::Published);
                        }
                        Err(e) => return Err(map_obj(e)),
                    }
                }
            }
        }
        Err(SnapshotError::Backend(format!(
            "pointer swap for {key:?} lost {MAX_SWAP_ATTEMPTS} consecutive CAS races; giving up"
        )))
    }

    /// Fetch the sibling manifest object, enforcing [`MAX_MANIFEST_BYTES`].
    async fn fetch_manifest_bytes(&self, key: &str) -> Result<Vec<u8>, SnapshotError> {
        let mut stream = timed("manifest get", self.store.get(&self.manifest_path(key)))
            .await?
            .map_err(map_obj)?
            .into_stream();
        let mut buf = Vec::new();
        while let Some(chunk) = timed("manifest read", stream.next()).await? {
            let chunk = chunk.map_err(map_obj)?;
            if buf.len() + chunk.len() > MAX_MANIFEST_BYTES {
                return Err(SnapshotError::ArtifactInvalid(format!(
                    "remote manifest for {key:?} exceeds {MAX_MANIFEST_BYTES} bytes"
                )));
            }
            buf.extend_from_slice(&chunk);
        }
        Ok(buf)
    }
}

#[async_trait]
impl ArtifactTransport for ObjectStoreTransport {
    async fn upload(
        &self,
        key: &str,
        artifact_dir: &Path,
    ) -> Result<PublishOutcome, SnapshotError> {
        // Read the manifest first — it doubles as the artifact-completeness
        // check (export writes it last) and, as the pointer bytes, derives
        // the payload's content address.
        let manifest_bytes = tokio::fs::read(artifact_dir.join(MANIFEST_FILE))
            .await
            .map_err(SnapshotError::Io)?;
        // Validate before shipping: never upload an artifact we couldn't read back.
        let manifest = manifest_from_slice(&manifest_bytes)?;

        // Tar the artifact into a temp file on a blocking task. A temp file
        // (rather than streaming the tar straight into the upload) keeps the
        // blocking tar writer and the async multipart writer decoupled; the
        // disk cost is one tar's worth, transient. Staged BESIDE the artifact,
        // not in the system temp dir — /tmp is often a size-bounded tmpfs that
        // a multi-GB artifact tar would exhaust.
        let src = artifact_dir.to_path_buf();
        let tar_parent = artifact_dir
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .ok_or_else(|| {
                SnapshotError::ArtifactInvalid(format!(
                    "artifact {} has no parent directory to stage the tar in",
                    artifact_dir.display()
                ))
            })?
            .to_path_buf();
        let tar_file = tokio::task::spawn_blocking(
            move || -> Result<tempfile::NamedTempFile, SnapshotError> {
                let tmp = tempfile::NamedTempFile::new_in(&tar_parent)?;
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
        let upload = timed(
            "multipart create",
            self.store
                .put_multipart(&self.payload_path(key, &manifest.cursor, &manifest_bytes)),
        )
        .await?
        .map_err(map_obj)?;
        let mut wm = WriteMultipart::new_with_chunk_size(upload, CHUNK);
        let mut buf = vec![0u8; CHUNK];
        loop {
            use tokio::io::AsyncReadExt;
            let n = file.read(&mut buf).await.map_err(SnapshotError::Io)?;
            if n == 0 {
                break;
            }
            timed("part upload", wm.wait_for_capacity(MAX_CONCURRENT_PARTS))
                .await?
                .map_err(map_obj)?;
            wm.write(&buf[..n]);
        }
        // finish() drains every in-flight part (up to MAX_CONCURRENT_PARTS ×
        // CHUNK bytes) plus the completion request, so it gets a proportionally
        // larger stall bound than a single-request await.
        timed_by(
            "multipart finish",
            OP_TIMEOUT * MAX_CONCURRENT_PARTS as u32,
            wm.finish(),
        )
        .await?
        .map_err(map_obj)?;

        // Pointer LAST, by monotonic conditional swap: its presence marks the
        // payload complete, and it can never regress past a newer round.
        self.swap_pointer(key, &manifest.cursor, &manifest_bytes)
            .await
    }

    async fn manifest(&self, key: &str) -> Result<ExportManifest, SnapshotError> {
        let bytes = self.fetch_manifest_bytes(key).await?;
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

        // Pointer first — it is the completeness marker, the source of the
        // payload's content address, and the value we cross-check the tar
        // against.
        let sibling = self.fetch_manifest_bytes(key).await?;
        let manifest = manifest_from_slice(&sibling)?;

        // Stream the tar to a temp file.
        let tar_tmp = tempfile::NamedTempFile::new_in(parent)?;
        let mut tar_writer = tokio::fs::File::create(tar_tmp.path())
            .await
            .map_err(SnapshotError::Io)?;
        let mut stream = timed(
            "payload get",
            self.store
                .get(&self.payload_path(key, &manifest.cursor, &sibling)),
        )
        .await?
        .map_err(map_obj)?
        .into_stream();
        while let Some(chunk) = timed("payload read", stream.next()).await? {
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
            // The tar came from an untrusted transport: never adopt its mode
            // bits (a crafted archive could plant world-writable or setuid
            // entries) or mtimes — content is what import verifies.
            archive.set_preserve_permissions(false);
            archive.set_preserve_mtime(false);
            // tar's unpack refuses entries that escape the destination, on top
            // of the manifest path validation import performs again.
            archive.unpack(stage.path())?;

            let embedded = std::fs::read(stage.path().join(MANIFEST_FILE)).map_err(|_| {
                SnapshotError::ArtifactInvalid(
                    "downloaded artifact tar has no embedded manifest".into(),
                )
            })?;
            if embedded != sibling {
                return Err(SnapshotError::ArtifactInvalid(
                    "embedded manifest disagrees with the sibling manifest object".into(),
                ));
            }

            check_dest_available(&dest)?;
            let root = stage.keep();
            rename_into_place(&root, &dest)?;
            Ok(())
        })
        .await
        .map_err(|e| SnapshotError::Backend(format!("untar task panicked: {e}")))??;

        Ok(manifest)
    }

    async fn prune(&self, key: &str, grace: Duration) -> Result<usize, SnapshotError> {
        // No pointer (or an unparseable one — it will be replaced by the
        // next publish) → nothing is provably stale, so prune nothing.
        let pointer = match self.fetch_manifest_bytes(key).await {
            Ok(b) => b,
            Err(_) => return Ok(0),
        };
        let Ok(current) = manifest_from_slice(&pointer) else {
            return Ok(0);
        };
        let keep = self.payload_path(key, &current.cursor, &pointer);
        let pointer_rank = cursor_rank(&current.cursor);
        let cutoff_millis = std::time::SystemTime::now()
            .checked_sub(grace)
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map_or(0, |d| d.as_millis() as i64);

        let mut deleted = 0usize;
        let mut listing = self.store.list(Some(&self.payloads_dir(key)));
        while let Some(meta) = timed("payload list", listing.next()).await? {
            let meta = meta.map_err(map_obj)?;
            // THE prune guard — the shared protocol kernel, the same
            // function the model checker's Prune transition executes
            // (`crate::protocol::payload_prunable`, where the
            // strictly-below-the-pointer rule and its dangling-pointer
            // impossibility argument live). The grace period remains as
            // defense in depth for in-flight downloads holding stale
            // pointer reads.
            let payload_rank = meta
                .location
                .filename()
                .and_then(|f| f.split('-').next())
                .and_then(|h| u64::from_str_radix(h, 16).ok());
            if payload_prunable(
                payload_rank,
                pointer_rank,
                meta.location == keep,
                meta.last_modified.timestamp_millis() <= cutoff_millis,
            ) {
                timed("payload delete", self.store.delete(&meta.location))
                    .await?
                    .map_err(map_obj)?;
                deleted += 1;
            }
        }
        Ok(deleted)
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

    // Upload + pointer swap; only then publish completion.
    let outcome = match transport.upload(key, &artifact_dir).await {
        Ok(o) => o,
        Err(e) => {
            guard.abandon().await;
            return Err(e);
        }
    };
    if let PublishOutcome::SupersededByNewer { remote_cursor } = &outcome {
        // Routine under lease overrun: another node published a newer round
        // while this one ran. The monotonic swap refused the regression —
        // the published "latest" is intact, this round's payload awaits the
        // next prune.
        warn!(
            key,
            local = ?manifest.cursor,
            remote = ?remote_cursor,
            "export round superseded by a newer published artifact; pointer left untouched"
        );
    }
    if let Err(e) = guard.complete(&manifest.cursor).await {
        // The artifact IS uploaded — the round succeeded. Losing the
        // completion record costs observability, not correctness.
        warn!(key, error = %e, "export round uploaded but completion record failed");
    }

    if matches!(outcome, PublishOutcome::Published) {
        // Housekeeping: collect payloads no pointer references. Grace of 4
        // round periods comfortably outlives any concurrent publisher's
        // upload→swap window and any in-flight bootstrap holding an older
        // pointer read (both are bounded by round-period timescales).
        if let Err(e) = transport.prune(key, ttl.saturating_mul(4)).await {
            warn!(key, error = %e, "stale payload prune failed; retried next round");
        }
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
