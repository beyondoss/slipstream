//! Artifact codec and staging for snapshot export/import (replica bootstrap).
//!
//! An **artifact** is a transferable copy of a durable fold: a directory holding
//! the backend's files under `data/` plus a [`MANIFEST_FILE`] recording the
//! backend's identity, its on-disk format generation, per-file checksums, and —
//! the load-bearing part — the **watch cursor the files are consistent with**.
//! Export produces one; import verifies and installs one; the consumer resumes
//! its KV watch from the embedded cursor and replays only the log tail.
//!
//! This module owns what is common across backends: the manifest wire format,
//! checksum verification, and the stage-then-atomic-rename discipline that keeps
//! half-written artifacts and half-imported folds from ever being observable at
//! their final paths. The backend-specific parts (how fjall/RocksDB/the append
//! log get a consistent copy of their files) live with each backend.
//!
//! ## Crash safety
//!
//! - **Export**: payload and manifest are assembled in a hidden temp directory
//!   beside `dest`, fsynced, and renamed into place. A crash mid-export leaves a
//!   `.slipstream-artifact-*` temp dir (cleaned up by the next tempdir reaper or
//!   operator) and **no** artifact at `dest`. An artifact that exists is complete.
//! - **Import**: the payload is copied-and-verified into a temp directory beside
//!   the destination, then renamed. A crash mid-import leaves no fold at the
//!   destination; a crash after the rename leaves a fully valid fold (a retried
//!   import then refuses the non-empty destination — the caller should simply
//!   open it).
//!
//! ## Blocking I/O
//!
//! Everything here is synchronous file I/O, same discipline as
//! [`crate::snapshot`]: async callers must offload to `spawn_blocking` (the
//! [`watch_applied`](crate::watch_applied) export path does).

use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::kv::{VersionToken, WatchCursor};
use crate::snapshot::SnapshotError;

/// Version of the artifact layout itself (`MANIFEST.json` schema + `data/`
/// payload convention). Bumped only when the artifact shape changes; the
/// *payload* format is governed separately by [`ExportManifest::backend_version`].
pub const ARTIFACT_SCHEMA_VERSION: u32 = 1;

/// Manifest file name at the artifact root. Written last and fsynced, so its
/// presence (after the atomic rename) means the artifact is complete.
pub const MANIFEST_FILE: &str = "MANIFEST.json";

/// Directory under the artifact root holding the backend's payload files.
pub(crate) const PAYLOAD_DIR: &str = "data";

/// Streaming-hash buffer size.
const HASH_BUF: usize = 1 << 20;

/// Manifest of an exported artifact: what is in it, which backend wrote it, and
/// the cursor its payload is consistent with.
#[derive(Debug, Clone)]
pub struct ExportManifest {
    /// Artifact layout version ([`ARTIFACT_SCHEMA_VERSION`]).
    pub schema_version: u32,
    /// Backend identity: `"append-log"`, `"fjall"`, or `"rocksdb"`.
    pub backend: String,
    /// The backend's **on-disk format generation** (not a crate semver): the
    /// append log's `FORMAT_VERSION` (`"2"`), fjall's format marker (`"3"`),
    /// the rust-rocksdb binding version (`"0.50"`, informational — RocksDB
    /// reads older formats and its own open is the arbiter).
    pub backend_version: String,
    /// The resume cursor the payload is exactly consistent with. Resuming the
    /// watch from here replays only the post-export tail.
    pub cursor: WatchCursor,
    /// Export wall-clock time, seconds since the Unix epoch. Informational.
    pub created_at_unix: u64,
    /// Every payload file, with size and BLAKE3 digest. Import verifies all of
    /// them and rejects undeclared extras.
    pub files: Vec<ArtifactFile>,
}

/// One payload file in an [`ExportManifest`].
#[derive(Debug, Clone)]
pub struct ArtifactFile {
    /// Path relative to the artifact root, `/`-separated, always under `data/`.
    pub path: String,
    /// Size in bytes.
    pub size: u64,
    /// Lowercase-hex BLAKE3 digest of the file contents. (BLAKE3 over SHA-256:
    /// there is no interop constraint — slipstream writes and reads its own
    /// manifests — and artifacts reach GBs, where BLAKE3's SIMD hashing is
    /// several times faster.)
    pub blake3: String,
}

// ---------------------------------------------------------------------------
// Wire format (serde) — kept separate from the public types so the public
// surface can hold a real WatchCursor while the JSON stays a stable hex string.
// ---------------------------------------------------------------------------

// `deny_unknown_fields` on both: the manifest is the trust boundary for a
// remote-supplied artifact, and an unrecognized field is far more likely a
// corrupted/hostile manifest or a schema mismatch than benign noise — reject
// loudly rather than skip silently. Forward compatibility is governed by
// `schema_version`, which is checked first in `manifest_from_slice`; a future
// schema that adds fields must bump it.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestWire {
    schema_version: u32,
    backend: String,
    backend_version: String,
    /// Raw cursor bytes as lowercase hex; empty string = no cursor
    /// ([`WatchCursor::none`]).
    cursor_hex: String,
    created_at_unix: u64,
    files: Vec<FileWire>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileWire {
    path: String,
    size: u64,
    blake3: String,
}

fn invalid(msg: impl Into<String>) -> SnapshotError {
    SnapshotError::ArtifactInvalid(msg.into())
}

// ---------------------------------------------------------------------------
// Hex (cursor encoding) — two tiny helpers instead of a dependency.
// ---------------------------------------------------------------------------

pub(crate) fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write as _;
        let _ = write!(out, "{b:02x}");
    }
    out
}

pub(crate) fn hex_decode(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok())
        .collect()
}

fn cursor_to_hex(cursor: &WatchCursor) -> String {
    hex_encode(cursor.version().as_bytes())
}

fn cursor_from_hex(s: &str) -> Result<WatchCursor, SnapshotError> {
    let bytes = hex_decode(s).ok_or_else(|| invalid(format!("malformed cursor_hex: {s:?}")))?;
    let token = VersionToken::from_raw(&bytes).ok_or_else(|| {
        invalid(format!(
            "cursor_hex decodes to {} bytes, exceeds version token capacity",
            bytes.len()
        ))
    })?;
    Ok(WatchCursor::from_version(token))
}

// ---------------------------------------------------------------------------
// Manifest read/write
// ---------------------------------------------------------------------------

/// Serialize and write `MANIFEST.json` at the artifact root: tempfile in the
/// same directory, fsync, atomic rename — the manifest is the artifact's
/// completeness marker, so it must never be observable half-written.
pub(crate) fn write_manifest(
    artifact_root: &Path,
    manifest: &ExportManifest,
) -> Result<(), SnapshotError> {
    let wire = ManifestWire {
        schema_version: manifest.schema_version,
        backend: manifest.backend.clone(),
        backend_version: manifest.backend_version.clone(),
        cursor_hex: cursor_to_hex(&manifest.cursor),
        created_at_unix: manifest.created_at_unix,
        files: manifest
            .files
            .iter()
            .map(|f| FileWire {
                path: f.path.clone(),
                size: f.size,
                blake3: f.blake3.clone(),
            })
            .collect(),
    };
    let json = serde_json::to_vec_pretty(&wire)
        .map_err(|e| SnapshotError::Backend(format!("manifest serialization failed: {e}")))?;

    let mut tmp = tempfile::NamedTempFile::new_in(artifact_root)?;
    tmp.write_all(&json)?;
    tmp.as_file().sync_all()?;
    tmp.persist(artifact_root.join(MANIFEST_FILE))
        .map_err(|e| SnapshotError::Io(e.error))?;
    Ok(())
}

/// Read and validate `MANIFEST.json` from an artifact directory.
pub(crate) fn read_manifest(artifact_dir: &Path) -> Result<ExportManifest, SnapshotError> {
    let path = artifact_dir.join(MANIFEST_FILE);
    let data = fs::read(&path).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            invalid(format!("no {MANIFEST_FILE} in {}", artifact_dir.display()))
        } else {
            SnapshotError::Io(e)
        }
    })?;
    manifest_from_slice(&data)
}

/// Parse and validate manifest JSON bytes.
///
/// Validates the schema version and every file path (relative, `/`-separated,
/// no `..`, no `\`, under `data/`) so a hostile or corrupted manifest can never
/// direct a copy outside the staging area (zip-slip).
pub(crate) fn manifest_from_slice(data: &[u8]) -> Result<ExportManifest, SnapshotError> {
    let wire: ManifestWire =
        serde_json::from_slice(data).map_err(|e| invalid(format!("malformed manifest: {e}")))?;

    if wire.schema_version != ARTIFACT_SCHEMA_VERSION {
        return Err(invalid(format!(
            "unsupported artifact schema_version {} (this build supports {})",
            wire.schema_version, ARTIFACT_SCHEMA_VERSION
        )));
    }
    for f in &wire.files {
        validate_payload_path(&f.path)?;
    }

    Ok(ExportManifest {
        schema_version: wire.schema_version,
        backend: wire.backend,
        backend_version: wire.backend_version,
        cursor: cursor_from_hex(&wire.cursor_hex)?,
        created_at_unix: wire.created_at_unix,
        files: wire
            .files
            .into_iter()
            .map(|f| ArtifactFile {
                path: f.path,
                size: f.size,
                blake3: f.blake3,
            })
            .collect(),
    })
}

/// Reject any manifest path that could escape the artifact when joined: it must
/// be relative, `/`-separated, contain no `..`/`.` components, and live under
/// `data/`.
fn validate_payload_path(p: &str) -> Result<(), SnapshotError> {
    let prefix = format!("{PAYLOAD_DIR}/");
    if !p.starts_with(&prefix) || p.len() == prefix.len() {
        return Err(invalid(format!(
            "manifest path {p:?} is not under {PAYLOAD_DIR}/"
        )));
    }
    if p.contains('\\') {
        return Err(invalid(format!("manifest path {p:?} contains a backslash")));
    }
    let path = Path::new(p);
    for comp in path.components() {
        match comp {
            Component::Normal(_) => {}
            _ => {
                return Err(invalid(format!(
                    "manifest path {p:?} contains a non-normal component"
                )));
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Payload hashing
// ---------------------------------------------------------------------------

/// Streaming BLAKE3 of one file, returning `(size, hex_digest)` plus the open
/// handle so the caller can `sync_all` without a second `open(2)`.
fn hash_file(path: &Path, buf: &mut [u8]) -> Result<(File, u64, String), SnapshotError> {
    let mut file = File::open(path)?;
    let mut hasher = blake3::Hasher::new();
    let mut size = 0u64;
    loop {
        let n = file.read(buf)?;
        if n == 0 {
            break;
        }
        size += n as u64;
        hasher.update(&buf[..n]);
    }
    Ok((file, size, hasher.finalize().to_hex().to_string()))
}

/// Every regular file under `root/data/`, relative `/`-separated paths, sorted.
fn list_payload_files(root: &Path) -> Result<Vec<PathBuf>, SnapshotError> {
    let payload = root.join(PAYLOAD_DIR);
    let mut out = Vec::new();
    let mut stack = vec![payload.clone()];
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let ty = entry.file_type()?;
            if ty.is_dir() {
                stack.push(entry.path());
            } else if ty.is_file() {
                out.push(entry.path());
            } else {
                // Symlinks etc. have no place in an artifact: a symlink would
                // hash as its target's bytes but restore as a link (or escape
                // the payload entirely). Refuse at export so import never has
                // to trust one.
                return Err(invalid(format!(
                    "payload contains a non-regular file: {}",
                    entry.path().display()
                )));
            }
        }
    }
    out.sort();
    Ok(out)
}

/// Hash every payload file under `root/data/` and fsync the payload tree
/// (files + directories), returning the manifest file list in sorted order.
pub(crate) fn hash_payload(root: &Path) -> Result<Vec<ArtifactFile>, SnapshotError> {
    let mut files = Vec::new();
    let mut buf = vec![0u8; HASH_BUF];
    for abs in list_payload_files(root)? {
        let (file, size, blake3) = hash_file(&abs, &mut buf)?;
        // Durability before the rename: the artifact's completeness contract is
        // "exists ⇒ verifiable", which only holds if the hashed bytes are the
        // on-disk bytes.
        file.sync_all()?;
        let rel = abs
            .strip_prefix(root)
            .map_err(|_| SnapshotError::Backend("payload path escaped artifact root".into()))?;
        let rel = rel
            .to_str()
            .ok_or_else(|| invalid(format!("non-UTF-8 payload path: {}", rel.display())))?;
        files.push(ArtifactFile {
            path: rel.to_string(),
            size,
            blake3,
        });
    }
    fsync_dir_tree(&root.join(PAYLOAD_DIR))?;
    Ok(files)
}

fn fsync_dir(path: &Path) -> Result<(), SnapshotError> {
    File::open(path)?.sync_all()?;
    Ok(())
}

fn fsync_dir_tree(root: &Path) -> Result<(), SnapshotError> {
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        fsync_dir(&dir)?;
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                stack.push(entry.path());
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Destination preconditions
// ---------------------------------------------------------------------------

/// A destination is available when it does not exist, or is an empty directory
/// (removed just before the final rename). Anything else is refused — never
/// overwrite a fold or an artifact in place.
pub(crate) fn check_dest_available(dest: &Path) -> Result<(), SnapshotError> {
    match fs::metadata(dest) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(SnapshotError::Io(e)),
        Ok(meta) if meta.is_dir() => {
            if fs::read_dir(dest)?.next().is_some() {
                Err(invalid(format!(
                    "destination {} exists and is not empty",
                    dest.display()
                )))
            } else {
                Ok(())
            }
        }
        Ok(_) => Err(invalid(format!(
            "destination {} already exists",
            dest.display()
        ))),
    }
}

/// Remove `dest` if it is an (already-verified-empty) directory so a rename can
/// land on its path, then rename `from` onto it and fsync the parent.
///
/// The is_dir → remove_dir → rename sequence has a TOCTOU window: a concurrent
/// writer can recreate `dest` between the remove and the rename. That race
/// fails closed — `remove_dir` errors on a non-empty dir and `rename` errors
/// when `dest` reappears as a file or non-empty dir — so the worst case is an
/// error return, never a silent overwrite. Don't "fix" this with
/// remove-then-retry; refusing the round is the intended behavior.
pub(crate) fn rename_into_place(from: &Path, dest: &Path) -> Result<(), SnapshotError> {
    if dest.is_dir() {
        fs::remove_dir(dest)?;
    }
    fs::rename(from, dest)?;
    if let Some(parent) = dest.parent() {
        fsync_dir(parent)?;
    }
    Ok(())
}

fn stage_dir_in(parent: &Path) -> Result<tempfile::TempDir, SnapshotError> {
    // Same directory as the destination so the final rename is a same-filesystem
    // atomic rename (mirrors `compact_to_file`'s tempfile discipline). The
    // hidden prefix keeps half-built stages out of an operator's way.
    Ok(tempfile::Builder::new()
        .prefix(".slipstream-artifact-")
        .tempdir_in(parent)?)
}

fn dest_parent(dest: &Path) -> Result<&Path, SnapshotError> {
    dest.parent()
        .filter(|p| !p.as_os_str().is_empty())
        .ok_or_else(|| {
            invalid(format!(
                "destination {} has no parent directory",
                dest.display()
            ))
        })
}

// ---------------------------------------------------------------------------
// Export staging
// ---------------------------------------------------------------------------

/// Assembles an artifact in a temp directory beside `dest`, then seals it
/// (hash → manifest → fsync) and atomically renames it into place.
///
/// Backends write their payload into [`payload`](Self::payload), then call
/// [`seal_and_finalize`](Self::seal_and_finalize).
pub(crate) struct ExportStage {
    dir: tempfile::TempDir,
    dest: PathBuf,
}

impl ExportStage {
    /// Create a stage for an artifact that will land at `dest_dir`. Fails fast
    /// if `dest_dir` is unavailable (exists non-empty).
    pub(crate) fn new(dest_dir: &Path) -> Result<Self, SnapshotError> {
        check_dest_available(dest_dir)?;
        let parent = dest_parent(dest_dir)?;
        let dir = stage_dir_in(parent)?;
        Ok(Self {
            dir,
            dest: dest_dir.to_path_buf(),
        })
    }

    /// The payload directory the backend writes its files into.
    ///
    /// Deliberately NOT pre-created: RocksDB's checkpoint API requires its
    /// target to not exist, so each backend creates (or lets the engine create)
    /// this path itself.
    pub(crate) fn payload(&self) -> PathBuf {
        self.dir.path().join(PAYLOAD_DIR)
    }

    /// Hash the payload, write the manifest, fsync, and atomically rename the
    /// stage to the destination. Returns the sealed manifest.
    pub(crate) fn seal_and_finalize(
        self,
        backend: &str,
        backend_version: &str,
        cursor: &WatchCursor,
    ) -> Result<ExportManifest, SnapshotError> {
        let root = self.dir.path();
        let files = hash_payload(root)?;
        let manifest = ExportManifest {
            schema_version: ARTIFACT_SCHEMA_VERSION,
            backend: backend.to_string(),
            backend_version: backend_version.to_string(),
            cursor: cursor.clone(),
            created_at_unix: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
            files,
        };
        write_manifest(root, &manifest)?;
        fsync_dir(root)?;

        // Re-check the destination (it may have appeared since `new`), then
        // rename. `keep()` disarms the TempDir destructor — the path has been
        // renamed away, so there is nothing left for it to delete.
        check_dest_available(&self.dest)?;
        let dest = self.dest.clone();
        let root = self.dir.keep();
        rename_into_place(&root, &dest)?;
        Ok(manifest)
    }
}

// ---------------------------------------------------------------------------
// Import staging
// ---------------------------------------------------------------------------

/// A verified copy of an artifact's payload, staged beside the destination and
/// ready to be renamed into place.
pub(crate) struct ImportStage {
    dir: tempfile::TempDir,
    dest: PathBuf,
}

impl ImportStage {
    /// The staged payload root (mirrors the artifact's `data/` layout).
    pub(crate) fn payload(&self) -> PathBuf {
        self.dir.path().join(PAYLOAD_DIR)
    }

    /// Rename the whole staged payload directory onto `dest` (directory-shaped
    /// backends: fjall, RocksDB).
    #[cfg(any(feature = "fjall", feature = "rocksdb"))]
    pub(crate) fn finalize_dir(self) -> Result<(), SnapshotError> {
        check_dest_available(&self.dest)?;
        rename_into_place(&self.payload(), &self.dest)
        // TempDir drop removes the now-payload-less stage directory.
    }

    /// Rename a single staged payload file onto `dest` (file-shaped backends:
    /// the append log).
    pub(crate) fn finalize_file(self, rel: &str) -> Result<(), SnapshotError> {
        check_dest_available(&self.dest)?;
        rename_into_place(&self.payload().join(rel), &self.dest)
    }
}

/// Validate an artifact against its manifest and stage a verified copy of its
/// payload beside `dest`.
///
/// Checks, in order: manifest well-formedness ([`read_manifest`]), backend
/// identity, backend version (via the caller's policy closure), destination
/// availability, then every payload file (copied while hashing — size and
/// BLAKE3 digest must match the manifest, and the payload must contain no undeclared
/// extra files). The transport that delivered the artifact is untrusted; this
/// re-verification is the trust boundary.
pub(crate) fn verify_and_stage_import(
    artifact_dir: &Path,
    dest: &Path,
    expected_backend: &str,
    check_backend_version: impl Fn(&str) -> Result<(), SnapshotError>,
) -> Result<(ExportManifest, ImportStage), SnapshotError> {
    let manifest = read_manifest(artifact_dir)?;

    if manifest.backend != expected_backend {
        return Err(invalid(format!(
            "artifact backend is {:?}, expected {:?}",
            manifest.backend, expected_backend
        )));
    }
    check_backend_version(&manifest.backend_version)?;
    check_dest_available(dest)?;

    // Undeclared extras: a file in the payload that the manifest doesn't list
    // was never hashed at export — it cannot be trusted.
    let declared: BTreeSet<&str> = manifest.files.iter().map(|f| f.path.as_str()).collect();
    for abs in list_payload_files(artifact_dir)? {
        let rel = abs
            .strip_prefix(artifact_dir)
            .map_err(|_| SnapshotError::Backend("payload path escaped artifact dir".into()))?;
        let rel = rel
            .to_str()
            .ok_or_else(|| invalid(format!("non-UTF-8 payload path: {}", rel.display())))?;
        if !declared.contains(rel) {
            return Err(invalid(format!("payload contains undeclared file: {rel}")));
        }
    }

    let parent = dest_parent(dest)?;
    let dir = stage_dir_in(parent)?;
    let stage = ImportStage {
        dir,
        dest: dest.to_path_buf(),
    };

    // Copy-while-hashing: one read pass per file serves both the copy and the
    // verification. One buffer for the whole loop — a fresh 1 MiB zero-init per
    // file would be O(files) wasted work on multi-hundred-file artifacts.
    let mut buf = vec![0u8; HASH_BUF];
    for f in &manifest.files {
        let src_path = artifact_dir.join(&f.path);
        let dst_path = stage.dir.path().join(&f.path);
        if let Some(p) = dst_path.parent() {
            fs::create_dir_all(p)?;
        }
        let mut src = File::open(&src_path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                invalid(format!("payload file missing: {}", f.path))
            } else {
                SnapshotError::Io(e)
            }
        })?;
        let mut dst = File::create(&dst_path)?;
        let mut hasher = blake3::Hasher::new();
        let mut size = 0u64;
        loop {
            let n = src.read(&mut buf)?;
            if n == 0 {
                break;
            }
            size += n as u64;
            hasher.update(&buf[..n]);
            dst.write_all(&buf[..n])?;
        }
        dst.sync_all()?;
        if size != f.size {
            return Err(invalid(format!(
                "payload file {} is {size} bytes, manifest says {}",
                f.path, f.size
            )));
        }
        let digest = hasher.finalize().to_hex().to_string();
        if digest != f.blake3 {
            return Err(invalid(format!(
                "payload file {} checksum mismatch (got {digest}, manifest says {})",
                f.path, f.blake3
            )));
        }
    }
    fsync_dir_tree(&stage.payload())?;

    Ok((manifest, stage))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn manifest_with(files: Vec<ArtifactFile>, cursor: WatchCursor) -> ExportManifest {
        ExportManifest {
            schema_version: ARTIFACT_SCHEMA_VERSION,
            backend: "append-log".into(),
            backend_version: "2".into(),
            cursor,
            created_at_unix: 1_765_400_000,
            files,
        }
    }

    #[test]
    fn manifest_round_trips() {
        let dir = TempDir::new().unwrap();
        let m = manifest_with(
            vec![ArtifactFile {
                path: "data/fold.snap".into(),
                size: 42,
                blake3: "ab".repeat(32),
            }],
            WatchCursor::from_u64(184_467),
        );
        write_manifest(dir.path(), &m).unwrap();
        let got = read_manifest(dir.path()).unwrap();
        assert_eq!(got.schema_version, m.schema_version);
        assert_eq!(got.backend, m.backend);
        assert_eq!(got.backend_version, m.backend_version);
        assert_eq!(got.cursor, m.cursor);
        assert_eq!(got.created_at_unix, m.created_at_unix);
        assert_eq!(got.files.len(), 1);
        assert_eq!(got.files[0].path, "data/fold.snap");
        assert_eq!(got.files[0].size, 42);
    }

    #[test]
    fn manifest_round_trips_none_cursor() {
        let dir = TempDir::new().unwrap();
        let m = manifest_with(vec![], WatchCursor::none());
        write_manifest(dir.path(), &m).unwrap();
        let got = read_manifest(dir.path()).unwrap();
        assert!(got.cursor.is_none(), "none cursor survives the round trip");
    }

    #[test]
    fn manifest_round_trips_fdb_width_cursor() {
        // 10-byte tokens have no u64 form; the hex path must carry them intact.
        let raw = [1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let cursor = WatchCursor::from_version(VersionToken::from_raw(&raw).unwrap());
        let dir = TempDir::new().unwrap();
        write_manifest(dir.path(), &manifest_with(vec![], cursor.clone())).unwrap();
        let got = read_manifest(dir.path()).unwrap();
        assert_eq!(got.cursor, cursor);
    }

    fn write_raw_manifest(dir: &Path, json: &str) {
        fs::write(dir.join(MANIFEST_FILE), json).unwrap();
    }

    fn wire_json(cursor_hex: &str, files: &str, schema: u32) -> String {
        format!(
            r#"{{"schema_version":{schema},"backend":"append-log","backend_version":"2",
                 "cursor_hex":"{cursor_hex}","created_at_unix":0,"files":{files}}}"#
        )
    }

    #[test]
    fn rejects_bad_cursor_hex() {
        let dir = TempDir::new().unwrap();
        for bad in ["zz", "abc", "0102030405060708090a0b"] {
            // non-hex, odd length, 11 bytes (> token capacity)
            write_raw_manifest(dir.path(), &wire_json(bad, "[]", ARTIFACT_SCHEMA_VERSION));
            match read_manifest(dir.path()) {
                Err(SnapshotError::ArtifactInvalid(_)) => {}
                other => panic!("cursor_hex {bad:?}: expected ArtifactInvalid, got {other:?}"),
            }
        }
    }

    #[test]
    fn rejects_wrong_schema_version() {
        let dir = TempDir::new().unwrap();
        write_raw_manifest(
            dir.path(),
            &wire_json("", "[]", ARTIFACT_SCHEMA_VERSION + 1),
        );
        match read_manifest(dir.path()) {
            Err(SnapshotError::ArtifactInvalid(msg)) => {
                assert!(msg.contains("schema_version"), "{msg}");
            }
            other => panic!("expected ArtifactInvalid, got {other:?}"),
        }
    }

    #[test]
    fn rejects_path_traversal() {
        let dir = TempDir::new().unwrap();
        for bad in [
            "../escape",
            "/abs/path",
            "data/../escape",
            "data/a\\b",
            "nondata/x",
            "data/",
            "data",
        ] {
            let files = format!(
                r#"[{{"path":"{}","size":0,"blake3":""}}]"#,
                bad.replace('\\', "\\\\")
            );
            write_raw_manifest(dir.path(), &wire_json("", &files, ARTIFACT_SCHEMA_VERSION));
            match read_manifest(dir.path()) {
                Err(SnapshotError::ArtifactInvalid(_)) => {}
                other => panic!("path {bad:?}: expected ArtifactInvalid, got {other:?}"),
            }
        }
    }

    #[test]
    fn rejects_malformed_manifest_json() {
        // Truly unparseable bytes (not merely wrong field values) must surface
        // as ArtifactInvalid, never an Io/Backend error or a panic.
        let dir = TempDir::new().unwrap();
        write_raw_manifest(dir.path(), "not json at all {{{");
        match read_manifest(dir.path()) {
            Err(SnapshotError::ArtifactInvalid(msg)) => {
                assert!(msg.contains("malformed"), "{msg}");
            }
            other => panic!("expected ArtifactInvalid, got {other:?}"),
        }
    }

    /// A symlink in the payload is refused at export: it would hash as its
    /// target's bytes but restore as a link (or escape the payload entirely),
    /// so `hash_payload` must reject it before a manifest is ever written.
    #[cfg(unix)]
    #[test]
    fn hash_payload_rejects_symlink() {
        let dir = TempDir::new().unwrap();
        let payload = dir.path().join(PAYLOAD_DIR);
        fs::create_dir(&payload).unwrap();
        fs::write(payload.join("real"), b"data").unwrap();
        let target = dir.path().join("outside");
        fs::write(&target, b"outside the payload").unwrap();
        std::os::unix::fs::symlink(&target, payload.join("link")).unwrap();

        match hash_payload(dir.path()) {
            Err(SnapshotError::ArtifactInvalid(msg)) => {
                assert!(msg.contains("non-regular"), "{msg}");
            }
            other => panic!("expected ArtifactInvalid, got {other:?}"),
        }
    }

    /// The TOCTOU window documented on `rename_into_place`: the destination
    /// appears (non-empty) between `ExportStage::new` and `seal_and_finalize`.
    /// The race must fail closed — an error return, never a silent overwrite.
    #[test]
    fn export_stage_fails_closed_when_dest_appears_before_seal() {
        let dir = TempDir::new().unwrap();
        let dest = dir.path().join("artifact");
        let stage = ExportStage::new(&dest).unwrap();
        fs::create_dir(stage.payload()).unwrap();
        fs::write(stage.payload().join("fold.snap"), b"data").unwrap();

        // A concurrent writer lands a non-empty directory at the destination.
        fs::create_dir(&dest).unwrap();
        fs::write(dest.join("stray"), b"x").unwrap();

        let err = stage
            .seal_and_finalize("append-log", "2", &WatchCursor::from_u64(1))
            .unwrap_err();
        assert!(matches!(err, SnapshotError::ArtifactInvalid(_)));
        assert!(
            dest.join("stray").exists(),
            "occupied destination is untouched"
        );
    }

    #[test]
    fn hex_round_trips() {
        for bytes in [&[][..], &[0u8][..], &[0xde, 0xad, 0xbe, 0xef][..]] {
            assert_eq!(hex_decode(&hex_encode(bytes)).unwrap(), bytes);
        }
        assert!(hex_decode("0g").is_none());
        assert!(hex_decode("a").is_none());
    }

    #[test]
    fn dest_preconditions() {
        let dir = TempDir::new().unwrap();
        // Absent: fine.
        check_dest_available(&dir.path().join("absent")).unwrap();
        // Empty dir: fine.
        let empty = dir.path().join("empty");
        fs::create_dir(&empty).unwrap();
        check_dest_available(&empty).unwrap();
        // Non-empty dir: refused.
        let full = dir.path().join("full");
        fs::create_dir(&full).unwrap();
        fs::write(full.join("x"), b"x").unwrap();
        assert!(matches!(
            check_dest_available(&full),
            Err(SnapshotError::ArtifactInvalid(_))
        ));
        // Existing file: refused.
        let file = dir.path().join("file");
        fs::write(&file, b"x").unwrap();
        assert!(matches!(
            check_dest_available(&file),
            Err(SnapshotError::ArtifactInvalid(_))
        ));
    }
}
