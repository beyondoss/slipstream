use async_nats::jetstream::kv::Store;
use async_trait::async_trait;
use futures::StreamExt;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;
use tokio::sync::{RwLock, mpsc::Sender};
use tracing::{debug, error, info, warn};

use crate::kv::{
    KvEntry, KvError, KvReader, KvUpdate, KvWatcher, KvWriter, VersionToken, WatchCursor,
};
use crate::stores::{Connection, ConnectionCapabilities, KvStore, StoreConfig};

/// Default per-operation timeout for NATS KV ops. async-nats's request/response
/// futures don't fail in-flight requests when the underlying TCP connection
/// goes half-dead (CLOSE_WAIT) — they just await forever. Without a timeout
/// here, ANY hung NATS connection translates into a tokio runtime deadlock as
/// soon as enough callers queue behind the dead connection. 30s is generous
/// for legitimate slow ops (cold JetStream stream sync, leader election under
/// load) and short enough that a dead connection recovers within reasonable
/// human-debug latency.
const KV_OP_TIMEOUT: Duration = Duration::from_secs(30);

/// Server-side inactivity reaper for the ephemeral consumers `scan()`/`keys()`
/// create. Our code deletes each consumer explicitly when the drain finishes,
/// but that delete is best-effort: on a half-dead (CLOSE_WAIT) connection it
/// times out, orphaning the consumer server-side where it counts against the
/// per-stream consumer limit. Setting `inactive_threshold` makes JetStream reap
/// any consumer that sees no activity for this long, so a failed explicit delete
/// self-heals instead of accumulating until the limit is hit. Comfortably longer
/// than [`KV_OP_TIMEOUT`] so it never reaps a legitimately slow in-flight drain
/// (each delivery resets the inactivity timer).
const CONSUMER_INACTIVE_THRESHOLD: Duration = Duration::from_secs(300);

/// Run a future under [`KV_OP_TIMEOUT`], returning [`KvError::Timeout`] if it
/// doesn't complete in time. Preserves the inner future's `Result` so callers
/// keep their existing error-mapping logic.
async fn timed<F, T>(fut: F) -> Result<T, KvError>
where
    F: std::future::Future<Output = T>,
{
    tokio::time::timeout(KV_OP_TIMEOUT, fut)
        .await
        .map_err(|_| KvError::Timeout)
}

/// Build NATS connect options (with auth applied) and resolve the URL to dial.
///
/// Split out from [`nats_connect`] so the connection lifecycle can attach an
/// event callback (for health tracking) to the *same* options before dialing,
/// without duplicating the auth-priority logic. Returns the options plus the URL
/// to connect to (which may differ from `url` when credentials are stripped out
/// of a `user:pass@host` URL).
///
/// Auth priority (first match wins):
/// 1. Inline credentials (base64-encoded .creds content)
/// 2. Credentials file (if provided)
/// 3. URL-embedded credentials (user:pass@host)
/// 4. No authentication
async fn build_connect_options(
    url: &str,
    creds: Option<&str>,
    creds_file: Option<&str>,
) -> Result<(async_nats::ConnectOptions, String), async_nats::ConnectError> {
    // Priority 1: Inline credentials (base64-encoded)
    if let Some(encoded) = creds {
        use base64::Engine;
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        let content = String::from_utf8(decoded)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        return Ok((
            async_nats::ConnectOptions::with_credentials(&content)?,
            url.to_string(),
        ));
    }

    // Priority 2: Credentials file
    if let Some(path) = creds_file {
        return Ok((
            async_nats::ConnectOptions::with_credentials_file(path).await?,
            url.to_string(),
        ));
    }

    // Priority 3: URL-embedded credentials
    if let Ok(parsed) = url::Url::parse(url)
        && !parsed.username().is_empty()
    {
        let user = parsed.username().to_string();
        let pass = parsed.password().unwrap_or("").to_string();
        // Rebuild URL without credentials. If the scheme doesn't support
        // userinfo, these calls fail and the credentials would remain embedded
        // in the URL we later log — warn loudly rather than silently leak them.
        let mut clean_url = parsed.clone();
        if clean_url.set_username("").is_err() || clean_url.set_password(None).is_err() {
            warn!("could not strip credentials from NATS URL; they may appear in logs");
        }
        return Ok((
            async_nats::ConnectOptions::with_user_and_password(user, pass),
            clean_url.as_str().to_string(),
        ));
    }

    // Priority 4: No authentication
    Ok((async_nats::ConnectOptions::new(), url.to_string()))
}

/// Connect to NATS with various authentication methods.
///
/// Supports the auth-priority order documented on [`build_connect_options`].
/// This is the standalone helper; the [`Connection`] impl builds options the
/// same way but also installs a health-tracking event callback.
pub async fn nats_connect(
    url: &str,
    creds: Option<&str>,
    creds_file: Option<&str>,
) -> Result<async_nats::Client, async_nats::ConnectError> {
    let (opts, dial_url) = build_connect_options(url, creds, creds_file).await?;
    opts.connect(dial_url).await
}

/// Render an untrusted server payload for logging: borrowed as-is when valid
/// UTF-8, lowercase hex otherwise. `from_utf8_lossy` would mash every invalid
/// byte into U+FFFD — exactly the bytes an incident needs to see — so the
/// fallback preserves them losslessly instead.
fn payload_for_log(payload: &[u8]) -> std::borrow::Cow<'_, str> {
    match std::str::from_utf8(payload) {
        Ok(s) => std::borrow::Cow::Borrowed(s),
        Err(_) => std::borrow::Cow::Owned(format!("0x{}", crate::artifact::hex_encode(payload))),
    }
}

/// Configuration for NATS connection.
///
/// `Debug` is hand-written, not derived: `creds` holds decoded credential
/// material and `creds_file` a filesystem path to secrets. A derived `Debug`
/// would print both verbatim the moment anyone `{:?}`-formats the config (a
/// `tracing` span, an error context, a test dump), leaking credentials into
/// logs. The redacting impl below keeps that from being one careless format
/// string away.
#[derive(Clone)]
pub struct NatsConnectionConfig {
    pub url: String,
    /// Base64-encoded .creds file content (for ECS / containerized environments).
    pub creds: Option<String>,
    /// Path to .creds file on disk (for bare-metal / local development).
    pub creds_file: Option<String>,
}

impl std::fmt::Debug for NatsConnectionConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NatsConnectionConfig")
            .field("url", &self.url)
            // Presence, never content: enough to debug "are creds set?" without
            // ever rendering the secret itself. The same applies to `creds_file`:
            // a path like `/run/secrets/prod.creds` leaks the secrets layout into
            // logs/traces, so redact it to presence too.
            .field("creds", &self.creds.as_ref().map(|_| "[redacted]"))
            .field(
                "creds_file",
                &self.creds_file.as_ref().map(|_| "[redacted]"),
            )
            .finish()
    }
}

/// Create a KV bucket using raw JetStream API (bypasses async-nats response parsing issues).
///
/// Synadia Cloud returns responses that `async_nats` can't parse. This function
/// uses the raw JetStream API directly, bypassing the client's response deserialization.
///
/// `pub(crate)`: this is an internal Synadia Cloud workaround invoked by
/// `get_or_create_bucket`, not a stable entry point. Exposing it would pin a
/// vendor quirk into the crate's semver surface.
pub(crate) async fn create_kv_bucket_raw(
    client: &async_nats::Client,
    bucket: &str,
    max_bytes: i64,
    history: i64,
    max_age_nanos: i64,
    num_replicas: usize,
) -> Result<(), KvError> {
    let stream_name = format!("KV_{}", bucket);
    let subject = format!("$KV.{}.>", bucket);

    // JetStream stream config for KV bucket
    let config = serde_json::json!({
        "name": stream_name,
        "subjects": [subject],
        "max_msgs_per_subject": history,
        "max_bytes": max_bytes,
        "max_age": max_age_nanos,
        "storage": "file",
        "allow_rollup_hdrs": true,
        "deny_delete": false,
        "deny_purge": false,
        "allow_direct": true,
        "discard": "new",
        "num_replicas": num_replicas,
        "retention": "limits"
    });

    let payload = serde_json::to_vec(&config)
        .map_err(|e| KvError::ConnectionFailed(format!("failed to serialize config: {}", e)))?;

    let response = client
        .request(
            format!("$JS.API.STREAM.CREATE.{}", stream_name),
            payload.into(),
        )
        .await
        .map_err(|e| KvError::ConnectionFailed(format!("failed to send create request: {}", e)))?;

    debug!(bucket, response = %payload_for_log(&response.payload), "raw JetStream response");

    match classify_raw_create_response(&response.payload) {
        RawCreateOutcome::AlreadyExists => {
            info!(bucket, "bucket already exists");
            Ok(())
        }
        RawCreateOutcome::StreamLimit => {
            info!(bucket, "stream limit reached, bucket may already exist");
            Ok(())
        }
        RawCreateOutcome::Created => {
            info!(bucket, "bucket created successfully via raw API");
            Ok(())
        }
        RawCreateOutcome::Failed { code, description } => Err(KvError::ConnectionFailed(format!(
            "JetStream error {}: {}",
            code, description
        ))),
    }
}

/// Classification of a raw `$JS.API.STREAM.CREATE` response payload.
///
/// Separated from the I/O in [`create_kv_bucket_raw`] so the Synadia Cloud
/// error-code logic — the reason this raw path exists — is unit-testable
/// without a live NATS server.
#[derive(Debug, PartialEq, Eq)]
enum RawCreateOutcome {
    /// No error in the response: the bucket was created.
    Created,
    /// `10058` — stream name already in use; the bucket exists. Non-fatal.
    AlreadyExists,
    /// `400` "maximum number of streams"; Synadia Cloud returns this at the
    /// stream limit even when the bucket already exists. Non-fatal.
    StreamLimit,
    /// Any other JetStream error — fatal.
    Failed { code: i64, description: String },
}

fn classify_raw_create_response(payload: &[u8]) -> RawCreateOutcome {
    // Unparseable payloads are treated as success: the caller re-verifies the
    // bucket with a follow-up `get_key_value`, so a garbled body here does not
    // mask a real failure. Warn so the fallback assumption is auditable — if the
    // re-verify step is ever refactored away, this log is the breadcrumb.
    //
    // INVARIANT: this `Created`-on-garbage path is only sound because every
    // caller re-verifies the bucket exists after `create_kv_bucket_raw` returns
    // Ok. The sole caller — `get_or_create_bucket` — does so via the
    // `timed(js.get_key_value(...))` immediately after the raw-create fallback.
    // Do not remove that re-verify without making this path return `Failed`.
    let Ok(json) = serde_json::from_slice::<serde_json::Value>(payload) else {
        warn!(
            response = %payload_for_log(payload),
            "unparseable STREAM.CREATE response; assuming created (caller re-verifies via get_key_value)"
        );
        return RawCreateOutcome::Created;
    };

    let Some(err) = json.get("error") else {
        return RawCreateOutcome::Created;
    };

    // JetStream splits its error codes: `code` is the HTTP-style status (400,
    // 404, 500) while `err_code` carries the granular code (e.g. 10058). The
    // already-exists code can surface in either field depending on the server
    // (standard NATS puts 10058 in `err_code` with `code` = 400; some managed
    // deployments echo it in `code`), so we accept it in either.
    let code = err.get("code").and_then(|c| c.as_i64()).unwrap_or(0);
    let err_code = err.get("err_code").and_then(|c| c.as_i64()).unwrap_or(0);
    let description = err
        .get("description")
        .and_then(|d| d.as_str())
        .unwrap_or("unknown error");

    // 10058 = stream name already in use (bucket exists) - that's OK
    if code == 10058 || err_code == 10058 {
        return RawCreateOutcome::AlreadyExists;
    }

    // 400 "maximum number of streams reached" may also mean bucket exists
    // (Synadia Cloud returns this when at stream limit but bucket exists)
    if code == 400 && description.contains("maximum number of streams") {
        return RawCreateOutcome::StreamLimit;
    }

    RawCreateOutcome::Failed {
        code,
        description: description.to_string(),
    }
}

struct NatsHandle {
    // Held to keep the NATS connection alive for the lifetime of the handle.
    // The `jetstream` context clones an internal reference, but this field is
    // the authoritative owner — dropping the handle drops the connection.
    // `dead_code` because we never read it directly after construction.
    #[allow(dead_code)]
    client: async_nats::Client,
    jetstream: async_nats::jetstream::Context,
}

/// NATS JetStream KV connection.
pub struct NatsConnection {
    config: NatsConnectionConfig,
    handle: RwLock<Option<NatsHandle>>,
    // Shared with the installed client's event callback so `is_healthy()`
    // tracks real connection state (Connected/Disconnected) rather than staying
    // pinned at its connect-time value. `Arc` because the callback outlives this
    // struct's borrow — it runs on the client's event-loop task.
    healthy: Arc<AtomicBool>,
    // Set only for connections built via `from_client`, where no health-tracking
    // event callback could be installed (the client was already connected).
    // `is_healthy()` consults this client's live `connection_state()` instead of
    // the callback-driven `healthy` flag. `None` for the `new()` + `connect()`
    // path, whose flag is kept current by the installed event callback.
    //
    // `Some(_)` is also the marker that this connection borrows a caller-owned
    // client: it carries no URL or credentials of its own (see `from_client`),
    // so it cannot redial. `connect()` refuses to reconnect such a connection
    // rather than dialing the empty config URL and surfacing a cryptic error.
    state_probe: Option<async_nats::Client>,
}

impl NatsConnection {
    pub fn new(config: NatsConnectionConfig) -> Self {
        Self {
            config,
            handle: RwLock::new(None),
            healthy: Arc::new(AtomicBool::new(false)),
            state_probe: None,
        }
    }

    /// Create a NatsConnection from an existing NATS client.
    ///
    /// This is useful when the caller already has a NATS connection and wants
    /// to reuse it for KV stores instead of creating a new connection.
    pub fn from_client(client: async_nats::Client) -> Self {
        let jetstream = async_nats::jetstream::new(client.clone());
        let config = NatsConnectionConfig {
            url: String::new(), // Not used when pre-connected
            creds: None,
            creds_file: None,
        };

        // Clone a probe handle before the client moves into `NatsHandle`.
        // `async_nats::Client` is cheap to clone (internally an `Arc`), and
        // `connection_state()` just reads a watch channel — no I/O.
        let state_probe = Some(client.clone());
        let handle = NatsHandle { client, jetstream };

        Self {
            config,
            handle: RwLock::new(Some(handle)),
            // A pre-connected client carries no health-tracking callback (we
            // didn't build its options), so `is_healthy()` reads the client's
            // live `connection_state()` via `state_probe`. The flag below only
            // gates explicit `shutdown()`.
            healthy: Arc::new(AtomicBool::new(true)),
            state_probe,
        }
    }

    async fn get_or_create_bucket(
        client: &async_nats::Client,
        js: &async_nats::jetstream::Context,
        config: &StoreConfig,
    ) -> Result<Store, KvError> {
        // Try to get existing bucket first. Bound the call so a slow/dead
        // NATS connection at startup can't park the daemon's init thread
        // forever — the rest of startup (HTTP listener bind, etc.) happens
        // after this. Without the timeout, a single bad NATS round-trip
        // here held HTTP bind for 30s+ in observed cases.
        //
        // A failure here (permission denied, JetStream disabled, timeout) is not
        // necessarily fatal — the bucket may simply not exist yet, so we fall
        // through to create. But surface the original error first: otherwise a
        // later create failure (e.g. "permission denied on STREAM.CREATE") masks
        // the real cause ("permission denied on STREAM.INFO") and makes the
        // failure undebuggable under load.
        match timed(js.get_key_value(&config.name)).await {
            Ok(Ok(kv)) => return Ok(kv),
            Ok(Err(e)) => {
                debug!(bucket = %config.name, error = ?e, "get_key_value failed; attempting create");
            }
            Err(_) => {
                warn!(bucket = %config.name, timeout = ?KV_OP_TIMEOUT, "get_key_value timed out; attempting create");
            }
        }

        // Bucket doesn't exist, create it
        let mut kv_config = async_nats::jetstream::kv::Config {
            bucket: config.name.clone(),
            num_replicas: config.num_replicas.unwrap_or(1),
            ..Default::default()
        };

        // Apply max_age (bucket-level TTL) if specified. `as_nanos()` is u128;
        // saturate to i64::MAX rather than `as i64`, which would silently wrap a
        // >292-year duration into a negative (and thus meaningless) TTL.
        let max_age_nanos = if let Some(max_age) = config.max_age {
            kv_config.max_age = max_age;
            i64::try_from(max_age.as_nanos()).unwrap_or(i64::MAX)
        } else {
            0
        };

        // Apply max_history if specified. `i64::from` is lossless for a u32 and
        // states the widening intent, where `as i64` would quietly mask a future
        // type change that no longer fits.
        let history = if let Some(history) = config.max_history {
            let history = i64::from(history);
            kv_config.history = history;
            history
        } else {
            1
        };

        // Apply max_bytes if specified (required by Synadia Cloud)
        let max_bytes = config.max_bytes.unwrap_or(10 * 1024 * 1024); // Default 10MB for Synadia Cloud
        kv_config.max_bytes = max_bytes;

        // Try normal create first, fall back to raw API if it fails (Synadia Cloud compatibility)
        match timed(js.create_key_value(kv_config)).await? {
            Ok(kv) => Ok(kv),
            Err(e) => {
                warn!(
                    bucket = config.name,
                    error = ?e,
                    "create_key_value failed, trying raw JetStream API"
                );

                // Try raw JetStream API as fallback
                create_kv_bucket_raw(
                    client,
                    &config.name,
                    max_bytes,
                    history,
                    max_age_nanos,
                    config.num_replicas.unwrap_or(1),
                )
                .await?;

                // Re-verify the bucket exists. This upholds the INVARIANT in
                // `classify_raw_create_response`: the raw path reports `Created`
                // on an unparseable response, so this round-trip is what actually
                // confirms the bucket — do not remove it.
                timed(js.get_key_value(&config.name))
                    .await?
                    .map_err(|e| {
                        error!(bucket = config.name, error = ?e, "failed to get bucket after raw create");
                        KvError::ConnectionFailed(format!("get bucket after raw create: {:?}", e))
                    })
            }
        }
    }
}

#[async_trait]
impl Connection for NatsConnection {
    async fn connect(&self) -> Result<(), KvError> {
        // Fast path: skip if already connected.
        if self.healthy.load(Ordering::Acquire) {
            return Ok(());
        }

        // A `from_client` connection borrows a caller-owned client and kept no
        // URL or credentials, so it cannot redial. Refuse here with an actionable
        // message instead of dialing the empty config URL (which fails with an
        // opaque parse/connect error). This is reachable only after `shutdown()`
        // cleared the fast-path flag above — a live borrowed client short-circuits
        // there. The caller must construct a `NatsConnection::new(config)` if it
        // needs reconnect semantics.
        if self.state_probe.is_some() {
            return Err(KvError::ConnectionFailed(
                "connection was built via NatsConnection::from_client and cannot \
                 reconnect (no URL or credentials retained); construct \
                 NatsConnection::new(config) for a reconnectable connection"
                    .to_string(),
            ));
        }

        let (opts, dial_url) = build_connect_options(
            &self.config.url,
            self.config.creds.as_deref(),
            self.config.creds_file.as_deref(),
        )
        .await
        .map_err(|e| KvError::ConnectionFailed(e.to_string()))?;

        // Drive `healthy` from the client's own connection events so it reflects
        // reality through async-nats's transparent reconnects — without this the
        // flag stays `true` straight through a NATS outage, and a readiness probe
        // built on `is_healthy()` keeps routing traffic to a node that can't
        // reach NATS.
        //
        // `installed` gates the callback: a caller that loses the connect race
        // (see the double-check below) tears down its freshly built client, and
        // that teardown fires `Disconnected`. Without the gate, the loser's drop
        // would clobber the *winner's* `healthy` flag. Only the client we
        // actually install ever flips `installed` to `true`, so the losers'
        // callbacks are inert.
        let installed = Arc::new(AtomicBool::new(false));
        let cb_healthy = Arc::clone(&self.healthy);
        let cb_installed = Arc::clone(&installed);
        let opts = opts.event_callback(move |event| {
            let cb_healthy = Arc::clone(&cb_healthy);
            let cb_installed = Arc::clone(&cb_installed);
            async move {
                if !cb_installed.load(Ordering::Acquire) {
                    return;
                }
                match event {
                    async_nats::Event::Connected => cb_healthy.store(true, Ordering::Release),
                    async_nats::Event::Disconnected => cb_healthy.store(false, Ordering::Release),
                    _ => {}
                }
            }
        });

        let client = opts
            .connect(dial_url)
            .await
            .map_err(|e| KvError::ConnectionFailed(e.to_string()))?;

        let jetstream = async_nats::jetstream::new(client.clone());

        let conn = NatsHandle { client, jetstream };

        // Re-check under the write lock: a concurrent caller may have connected
        // while we were awaiting the dial. If so, drop our freshly built handle
        // (closing its connection) instead of replacing the live one, which would
        // orphan a connection the first caller still believes is installed.
        // Leaving `installed = false` keeps our about-to-drop client's teardown
        // events from touching `healthy`.
        let mut handle = self.handle.write().await;
        if handle.is_some() {
            return Ok(());
        }
        installed.store(true, Ordering::Release);
        *handle = Some(conn);
        self.healthy.store(true, Ordering::Release);

        Ok(())
    }

    async fn shutdown(&self) -> Result<(), KvError> {
        self.healthy.store(false, Ordering::Release);
        *self.handle.write().await = None;
        Ok(())
    }

    fn is_healthy(&self) -> bool {
        // `healthy` is the shutdown gate for both construction paths: once
        // `shutdown()` clears it the connection is down regardless of socket
        // state, so check it first.
        if !self.healthy.load(Ordering::Acquire) {
            return false;
        }
        match &self.state_probe {
            // `from_client`: no event callback could be installed, so consult the
            // client's live connection state instead of a stale connect-time
            // value. A dead or reconnecting socket reports Pending/Disconnected,
            // so a readiness probe correctly sees the node as unhealthy. A
            // borrowed client is never replaced (connect() refuses to reconnect
            // it), so this probe never goes stale.
            Some(client) => matches!(
                client.connection_state(),
                async_nats::connection::State::Connected
            ),
            // `new()` + `connect()`: `healthy` is kept current by the installed
            // Connected/Disconnected event callback.
            None => true,
        }
    }

    async fn store(&self, name: &str) -> Result<Arc<dyn KvStore>, KvError> {
        let config = StoreConfig {
            name: name.to_string(),
            ..Default::default()
        };
        self.store_with_config(config).await
    }

    async fn store_with_config(&self, config: StoreConfig) -> Result<Arc<dyn KvStore>, KvError> {
        // Clone the client/jetstream out from under the read lock before the
        // (up to 60s) bucket get-or-create. Holding the read guard across that
        // await would block `shutdown()`'s `write().await` for the full
        // duration, stalling graceful shutdown behind an in-flight store call.
        let (client, js) = {
            let conn = self.handle.read().await;
            let conn = conn.as_ref().ok_or(KvError::NotConnected)?;
            (conn.client.clone(), conn.jetstream.clone())
        };

        let kv = Self::get_or_create_bucket(&client, &js, &config).await?;

        Ok(Arc::new(NatsKvStore {
            name: config.name,
            client,
            js,
            kv,
        }))
    }

    fn capabilities(&self) -> ConnectionCapabilities {
        ConnectionCapabilities {
            streaming_watch: true,
            prefix_watch: true,
            // `KvTtl` is not implemented for the NATS backend yet (only `KvWriter`
            // is vended by `writer()`), so advertising `ttl: true` would lead
            // callers that branch on this flag down a path that can never
            // succeed. Flip to `true` together with the `KvTtl` impl.
            ttl: false,
            cas: true,
            transactions: false,
            // 0 = unlimited from this layer's perspective: we impose no cap, but
            // the NATS server still enforces its own max payload (~1MB by
            // default). Callers that branch on this must not read 0 as "any size
            // is safe" — an oversized value is rejected server-side at write time.
            max_value_size: 0,
            global_ordering: false,
        }
    }
}

struct NatsKvStore {
    name: String,
    kv: Store,
    client: async_nats::Client,
    js: async_nats::jetstream::Context,
}

impl KvStore for NatsKvStore {
    fn name(&self) -> &str {
        &self.name
    }

    fn reader(&self) -> Arc<dyn KvReader> {
        Arc::new(NatsKvReader {
            kv: self.kv.clone(),
            client: self.client.clone(),
            js: self.js.clone(),
            bucket: self.name.clone(),
        })
    }

    fn watcher(&self) -> Option<Arc<dyn KvWatcher>> {
        Some(Arc::new(NatsKvWatcher {
            kv: self.kv.clone(),
            client: self.client.clone(),
            js: self.js.clone(),
            bucket: self.name.clone(),
        }))
    }

    fn writer(&self) -> Option<Arc<dyn KvWriter>> {
        Some(Arc::new(NatsKvWriterImpl {
            kv: self.kv.clone(),
        }))
    }
}

struct NatsKvReader {
    kv: Store,
    client: async_nats::Client,
    js: async_nats::jetstream::Context,
    // The bucket name is known at construction (it's the store's name), so
    // `consume_last_per_subject` builds its subject filters from this field
    // instead of issuing a `kv.status()` round-trip per `scan()`/`keys()` call
    // just to read it back from the server.
    bucket: String,
}

#[async_trait]
impl KvReader for NatsKvReader {
    async fn get(&self, key: &str) -> Result<Option<KvEntry>, KvError> {
        // Empty value → treat as absent. This unifies a real stored `b""` and a
        // `delete_with_version` tombstone (empty-value Put) under one "absent =
        // None" contract, consistent with `scan()`/`keys()`. Callers needing
        // zero-length semantics use `entry()`. See the `KvReader::get` trait doc.
        match self.entry(key).await? {
            Some(entry) if entry.value.is_empty() => Ok(None),
            other => Ok(other),
        }
    }

    async fn entry(&self, key: &str) -> Result<Option<KvEntry>, KvError> {
        use async_nats::jetstream::kv::Operation;
        // Use entry() instead of get() to access revision.
        // Return Put entries even with empty values — delete_with_version
        // writes empty bytes as a tombstone and callers need the version
        // for CAS conflict detection. Only filter real Delete/Purge markers.
        match timed(self.kv.entry(key)).await? {
            Ok(Some(entry)) if entry.operation == Operation::Put => Ok(Some(KvEntry {
                key: key.to_string(),
                value: entry.value.to_vec(),
                version: VersionToken::from_u64(entry.revision),
            })),
            Ok(Some(_)) => Ok(None), // Delete/Purge marker
            Ok(None) => Ok(None),
            Err(e) => Err(KvError::OperationFailed(e.to_string())),
        }
    }

    async fn keys(&self, prefix: &str) -> Result<Vec<String>, KvError> {
        debug!(prefix = %prefix, "listing keys with prefix");

        let mut keys = Vec::new();
        self.consume_last_per_subject(prefix, true, |msg, key| {
            // Skip both real KV deletes and CAS tombstones (empty-value Puts
            // written by delete_with_version). get()/scan() hide the latter, so
            // keys() must too — otherwise a list-then-get returns phantom keys.
            // With headers_only the payload is stripped, but NATS adds a
            // `Nats-Msg-Size` header we use to detect the empty value.
            if !is_kv_delete(&msg) && !is_empty_value(&msg) {
                keys.push(key);
            }
        })
        .await?;

        debug!(prefix = %prefix, keys = keys.len(), "keys listing complete");
        Ok(keys)
    }

    async fn scan(&self, prefix: &str) -> Result<Vec<KvEntry>, KvError> {
        let mut entries = Vec::new();
        self.consume_last_per_subject(prefix, false, |msg, key| {
            if !is_kv_delete(&msg) && !msg.payload.is_empty() {
                // The KV revision is the stream sequence, carried in the JetStream
                // ACK subject (the message's reply subject). A revision of 0 means
                // we couldn't parse it; callers treat that as "unknown version".
                let revision = msg
                    .reply
                    .as_deref()
                    .and_then(stream_sequence_from_ack)
                    .unwrap_or(0);

                entries.push(KvEntry {
                    key,
                    value: msg.payload.to_vec(),
                    version: VersionToken::from_u64(revision),
                });
            }
        })
        .await?;

        debug!(prefix = %prefix, entries = entries.len(), "scan complete");
        Ok(entries)
    }
}

/// Extract the stream sequence (== KV revision) from a JetStream ACK subject.
///
/// The ACK subject — delivered as a push message's reply subject — comes in two
/// shapes, and the stream sequence sits at different offsets in each:
///
/// ```text
/// legacy (9 tokens):  $JS.ACK.<stream>.<consumer>.<delivered>.<stream_seq>.<consumer_seq>.<ts>.<pending>
/// modern (11–12):     $JS.ACK.<domain>.<account>.<stream>.<consumer>.<delivered>.<stream_seq>.<consumer_seq>.<ts>.<pending>[.<token>]
/// ```
///
/// The previous implementation took the *last* token, which is `num_pending`
/// (typically 0 on the final delivery), not the sequence — corrupting the
/// version on every scanned entry. We instead parse from the front, accounting
/// for the optional `<domain>.<account>` prefix that modern servers prepend.
fn stream_sequence_from_ack(reply: &str) -> Option<u64> {
    // The stream-seq field sits at index 5 (legacy) or 7 (modern), so we only
    // ever read the first 8 tokens. Keep those in a stack array and count the
    // remainder with the iterator — no heap `Vec`, which on a large `scan()`
    // would be one allocation per delivered message.
    let mut head = [""; 8];
    let mut count = 0usize;
    for (i, token) in reply.split('.').enumerate() {
        if i < head.len() {
            head[i] = token;
        }
        count += 1;
    }
    if count < 9 || head[0] != "$JS" || head[1] != "ACK" {
        return None;
    }
    // Legacy form has exactly 9 tokens with no domain/account; anything longer
    // carries the two-token `<domain>.<account>` prefix, shifting fields right.
    let stream_seq_idx = if count == 9 { 5 } else { 7 };
    head[stream_seq_idx].parse::<u64>().ok()
}

/// Check if a NATS message represents a KV delete/purge operation.
fn is_kv_delete(msg: &async_nats::Message) -> bool {
    msg.headers
        .as_ref()
        .and_then(|h| h.get("KV-Operation"))
        .is_some()
}

/// Check if a `headers_only` delivery carries an empty value (a CAS tombstone
/// written by `delete_with_version`).
///
/// When a consumer is created with `headers_only`, NATS strips the body and adds
/// a `Nats-Msg-Size` header with the original payload length. Size 0 means the
/// stored value is empty, which `get()`/`scan()` treat as absent. Messages
/// without the header (e.g. non-`headers_only` deliveries) are not classified as
/// empty here — callers on that path inspect the payload directly instead.
fn is_empty_value(msg: &async_nats::Message) -> bool {
    msg.headers
        .as_ref()
        .and_then(|h| h.get("Nats-Msg-Size"))
        .map(|v| v.as_str() == "0")
        .unwrap_or(false)
}

impl NatsKvReader {
    /// Subscribe to last-per-subject messages for a KV prefix, calling `on_msg`
    /// for each delivered message. Handles the subscribe-first race workaround,
    /// consumer lifecycle, and cleanup.
    async fn consume_last_per_subject(
        &self,
        prefix: &str,
        headers_only: bool,
        mut on_msg: impl FnMut(async_nats::Message, String),
    ) -> Result<(), KvError> {
        use async_nats::jetstream::consumer::push;
        use async_nats::jetstream::consumer::{AckPolicy, DeliverPolicy};

        // The bucket name is known at construction, so the subject filters are
        // built directly from `self.bucket` — no `kv.status()` round-trip just
        // to read it back. Every *remaining* setup await below is still bounded
        // by `timed()`: a half-dead NATS connection (CLOSE_WAIT) would otherwise
        // park here before the per-message drain timer downstream ever starts,
        // hanging scan()/keys() indefinitely — the same failure `timed()` guards
        // on the write path.
        let bucket = self.bucket.as_str();

        let nats_filter = if prefix.is_empty() {
            format!("$KV.{bucket}.>")
        } else {
            format!("$KV.{bucket}.{prefix}>")
        };

        // Work around async-nats <=0.46 subscribe-after-create race:
        // subscribe to the inbox FIRST, then create the consumer.
        let inbox = self.client.new_inbox();
        let mut sub = timed(self.client.subscribe(inbox.clone()))
            .await?
            .map_err(|e| KvError::OperationFailed(format!("subscribe inbox: {e}")))?;

        let stream = timed(self.js.get_stream(format!("KV_{bucket}")))
            .await?
            .map_err(|e| KvError::OperationFailed(format!("get KV stream: {e}")))?;

        let consumer = timed(stream.create_consumer(push::Config {
            deliver_subject: inbox,
            deliver_policy: DeliverPolicy::LastPerSubject,
            filter_subject: nats_filter,
            headers_only,
            // This is a one-shot point-in-time drain — we never ack. Under
            // the default `AckPolicy::Explicit`, JetStream stops delivering
            // once `max_ack_pending` (default 1000) messages sit unacked,
            // which would silently truncate scan()/keys() to the first ~1000
            // keys (or stall waiting for deliveries that never come) on any
            // larger bucket. `None` removes the ack-pending gate entirely.
            ack_policy: AckPolicy::None,
            // Safety net for the best-effort `delete_consumer` below: if that
            // cleanup times out on a half-dead connection, JetStream still reaps
            // this consumer after `CONSUMER_INACTIVE_THRESHOLD` of inactivity, so
            // repeated timed-out scans can't pile orphaned consumers up against
            // the per-stream limit.
            inactive_threshold: CONSUMER_INACTIVE_THRESHOLD,
            ..Default::default()
        }))
        .await?
        .map_err(|e| KvError::OperationFailed(format!("create consumer: {e}")))?;

        let num_pending = consumer.cached_info().num_pending;

        // Drain exactly `num_pending` messages, but bound each await: a half-dead
        // connection (CLOSE_WAIT) would otherwise park this loop forever, the same
        // failure `timed()` guards on the write path. On timeout we still fall
        // through to consumer cleanup, then surface `Timeout`.
        let mut timed_out = false;
        if num_pending > 0 {
            let mut delivered = 0u64;
            let kv_prefix = format!("$KV.{bucket}.");

            while delivered < num_pending {
                match tokio::time::timeout(KV_OP_TIMEOUT, sub.next()).await {
                    Ok(Some(msg)) => {
                        let key = msg
                            .subject
                            .strip_prefix(&kv_prefix)
                            .unwrap_or(msg.subject.as_str())
                            .to_string();

                        on_msg(msg, key);
                        delivered += 1;
                    }
                    Ok(None) => break, // subscription closed early
                    Err(_) => {
                        timed_out = true;
                        break;
                    }
                }
            }
        }

        // Clean up ephemeral consumer (best-effort), even on timeout — a stalled
        // scan shouldn't also leak a server-side consumer. A leaked consumer
        // lingers on the server and counts against per-stream limits, so surface
        // failures in observability without failing the operation. Bound the
        // delete with `timed()`: on the same half-dead (CLOSE_WAIT) connection
        // that tripped the drain timeout above, an unbounded delete would re-park
        // here forever, defeating the timeout recovery we just performed.
        match timed(stream.delete_consumer(&consumer.cached_info().name)).await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                // `warn!`, not `debug!`: a leaked ephemeral consumer lingers on
                // the server and counts against per-stream limits. Under a flaky
                // NATS connection every scan()/keys() leaks one, so this must be
                // visible in default spans before the pile-up hits the limit.
                warn!(error = %e, "failed to delete ephemeral consumer (best-effort)");
            }
            Err(_) => {
                warn!("timed out deleting ephemeral consumer (best-effort)");
            }
        }

        if timed_out {
            return Err(KvError::Timeout);
        }
        Ok(())
    }
}

/// Convert a NATS KV entry to a KvUpdate.
///
/// Takes the entry by value so the key `String` moves into the `KvUpdate`
/// instead of allocating a fresh copy per watch event.
fn nats_entry_to_kv_update(entry: async_nats::jetstream::kv::Entry) -> KvUpdate {
    use async_nats::jetstream::kv::Operation;
    let version = VersionToken::from_u64(entry.revision);
    match entry.operation {
        Operation::Put => KvUpdate::Put(KvEntry {
            key: entry.key,
            value: entry.value.to_vec(),
            version,
        }),
        Operation::Delete => KvUpdate::Delete {
            key: entry.key,
            version,
        },
        Operation::Purge => KvUpdate::Purge {
            key: entry.key,
            version,
        },
    }
}

/// Stream updates from a NATS Watch into a channel until it ends or the receiver drops.
async fn stream_watch(
    mut watcher: async_nats::jetstream::kv::Watch,
    tx: &Sender<KvUpdate>,
) -> Result<(), KvError> {
    while let Some(entry) = watcher.next().await {
        match entry {
            Ok(entry) => {
                let update = nats_entry_to_kv_update(entry);
                if tx.send(update).await.is_err() {
                    debug!("watch receiver closed");
                    break;
                }
            }
            Err(e) => {
                error!(error = %e, "NATS KV watch error");
                return Err(KvError::WatchError(e.to_string()));
            }
        }
    }
    Ok(())
}

/// Cadence of the floor guard's no-traffic backstop probe (one stream-info
/// RPC per interval per guarded watch). The PRIMARY detection is in-band —
/// the gapped-delivery check fires the moment evidence surfaces — so this
/// interval only bounds detection latency when NOTHING is being delivered;
/// it is not load-bearing for eventual detection.
const FLOOR_GUARD_INTERVAL: Duration = Duration::from_secs(30);

/// [`stream_watch`] for the dense ALL-scope resume path, with the LIVE
/// retention floor guard (`tests/model_live_watch.rs` — the live twin of
/// [`NatsKvWatcher::check_resume_window`]).
///
/// The hazard: retention overrunning a live consumer makes JetStream
/// silently skip evicted messages — delete markers included — with no error
/// anywhere (the same clamp behavior as resumes, mid-stream). Unguarded,
/// that is PERMANENT silent fold divergence; the model proves it reachable.
///
/// Detection is primarily **in-band**: an unfiltered `ByStartSequence`
/// consumer sees every retained message, so a delivered revision that jumps
/// the frontier by more than one is evidence of eviction inside the gap.
/// The model checker REJECTED a periodic-only design with exactly the trace
/// this closes — deliveries can catch the frontier up past the gap between
/// probes, erasing the evidence — so the check runs AT the gapped delivery,
/// before the entry is processed: fetch `first_sequence` and apply the
/// shared kernel (`protocol::resume_window_ok`) to the frontier. A benign
/// gap (interior per-subject eviction with the floor still at or below the
/// frontier) passes; head eviction past the frontier fails the watch, and
/// the caller's restart routes into the verified resume → `CursorExpired` →
/// resync repair path. The periodic probe backstops the no-traffic case.
///
/// Scope: sound only where density holds — the unfiltered resume watch.
/// Prefix-scoped watches deliver sparse revisions by design and cannot
/// distinguish benign from hazardous eviction client-side; they retain the
/// (narrowed) retention-outlives-lag operating axiom plus the resume-time
/// check on every restart (model axiom 5).
///
/// The guarantee split, precisely: the SAFETY half — never folding past
/// unexamined evidence of loss — is unconditional in this loop (the gap
/// check precedes processing, and a stalled downstream stalls folding too).
/// The REPAIR half is conditional on the caller restarting the failed watch
/// (standard supervision; same posture as the resync fail-stop): a trip
/// with no restart is a loudly dead watch, never a silently wrong one.
async fn stream_watch_floor_guarded(
    mut watcher: async_nats::jetstream::kv::Watch,
    tx: &Sender<KvUpdate>,
    resume_revision: u64,
    js: &async_nats::jetstream::Context,
    bucket: &str,
) -> Result<(), KvError> {
    let stream_name = format!("KV_{bucket}");
    let first_sequence = || async {
        let stream = timed(js.get_stream(&stream_name))
            .await?
            .map_err(|e| KvError::OperationFailed(format!("floor guard stream lookup: {e}")))?;
        Ok::<u64, KvError>(stream.cached_info().state.first_sequence)
    };
    fn trip(frontier: u64, first: u64, bucket: &str) -> KvError {
        warn!(
            frontier,
            first_sequence = first,
            bucket,
            "stream retention overran this live watch; failing so the restart can resync \
             (messages in the gap were evicted unseen)"
        );
        KvError::WatchError(format!(
            "stream retention overran live watch (first_sequence {first} > delivered \
             frontier {frontier} + 1); restart will resync"
        ))
    }

    let mut frontier = resume_revision;
    let mut backstop = tokio::time::interval(FLOOR_GUARD_INTERVAL);
    backstop.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    backstop.tick().await; // consume the immediate first tick

    loop {
        tokio::select! {
            entry = watcher.next() => {
                let Some(entry) = entry else { break };
                match entry {
                    Ok(entry) => {
                        let revision = entry.revision;
                        // In-band gap check BEFORE processing: never fold
                        // past unexamined evidence of eviction.
                        if revision > frontier.saturating_add(1) {
                            let first = first_sequence().await?;
                            if !crate::protocol::resume_window_ok(frontier, first) {
                                return Err(trip(frontier, first, bucket));
                            }
                            // Benign interior gap: every evicted revision
                            // below a still-low floor was a per-subject
                            // overwrite, whose later revision the fold will
                            // see — safe for last-write-wins.
                        }
                        frontier = frontier.max(revision);
                        let update = nats_entry_to_kv_update(entry);
                        if tx.send(update).await.is_err() {
                            debug!("watch receiver closed");
                            break;
                        }
                    }
                    Err(e) => {
                        error!(error = %e, "NATS KV watch error");
                        return Err(KvError::WatchError(e.to_string()));
                    }
                }
            }
            _ = backstop.tick() => {
                // No-traffic backstop: nothing is being delivered, so the
                // in-band check has no evidence to act on; probe the floor
                // directly.
                let first = first_sequence().await?;
                if !crate::protocol::resume_window_ok(frontier, first) {
                    return Err(trip(frontier, first, bucket));
                }
            }
        }
    }
    Ok(())
}

/// Check if a NATS watch error indicates the requested start sequence is
/// too old (compacted), meaning callers should fall back to a full watch.
///
/// SECOND line of defense only: live nats-server (2.14) does not error on a
/// below-head start sequence at all — it silently clamps to the first
/// retained message (pinned by `tests/resync.rs`), so the PRIMARY expiry
/// detection is [`NatsKvWatcher::check_resume_window`]'s proactive
/// `first_sequence` comparison. This matcher remains for server versions or
/// paths that do error, where mapping to [`KvError::CursorExpired`] keeps
/// the fallback reachable instead of stranding the caller.
///
/// async-nats has no granular error kind for this: `WatchErrorKind` is only
/// `InvalidKey`/`TimedOut`/`ConsumerCreate`/`Other`, and "start sequence too old"
/// arrives as `ConsumerCreate`/`Other` with the real reason buried in the source
/// error's *message*. So we substring-match the full error string — which already
/// includes the source, since `Error`'s `Display` renders `"{kind}: {source}"`.
///
/// Two deliberate choices make this robust to wording drift:
/// - We lowercase first, so a capitalization change in NATS/async-nats can't slip
///   past.
/// - Detection is biased toward `true`. A false positive only costs an
///   unnecessary (but always-safe) full `watch_all()` replay; a false negative
///   propagates `WatchError` and strands a caller that would otherwise recover.
///
/// If these messages ever change, `cursor_expired_matches_known_nats_error_strings`
/// is the canary that fails loudly on the next dependency bump.
fn is_cursor_expired_error(err: &str) -> bool {
    use std::sync::OnceLock;
    // One Aho-Corasick automaton over all needles: a single pass over the error
    // string regardless of how many needles accumulate as NATS versions reword
    // their messages, vs. one `windows()` scan per needle. Case-insensitivity is
    // baked into the automaton, so no lowercased copy is allocated either.
    static MATCHER: OnceLock<aho_corasick::AhoCorasick> = OnceLock::new();
    MATCHER
        .get_or_init(|| {
            aho_corasick::AhoCorasick::builder()
                .ascii_case_insensitive(true)
                .build([
                    "start sequence",
                    "first sequence",
                    "sequence not found",
                    "too old",
                ])
                .expect("static needle set always compiles")
        })
        .is_match(err)
}

struct NatsKvWatcher {
    kv: Store,
    // `watch_prefixes_from` has no async-nats equivalent (there is no
    // `watch_many_from_revision`), so it hand-builds the multi-filter ordered
    // consumer itself — which needs the raw client (inbox allocation), the
    // JetStream context (stream lookup), and the bucket name (subject filters),
    // same as the reader's scan path.
    client: async_nats::Client,
    js: async_nats::jetstream::Context,
    bucket: String,
}

/// Decode a raw KV stream message (as delivered by a hand-built ordered push
/// consumer) into a [`KvUpdate`] — the same mapping `async-nats`'s `kv::Watch`
/// performs internally for the `watch_*` paths: key from the subject (stripping
/// the `$KV.{bucket}.` prefix), operation from the `KV-Operation` header
/// (absent = Put), revision from the stream sequence in the ACK reply subject.
///
/// Returns `None` for a subject outside the bucket's keyspace, which a
/// subject-filtered consumer should never deliver — skipped rather than
/// surfaced, matching `kv::Watch`'s behavior.
fn kv_message_to_update(msg: &async_nats::Message, kv_prefix: &str) -> Option<KvUpdate> {
    let key = msg.subject.strip_prefix(kv_prefix)?.to_string();
    let revision = msg
        .reply
        .as_deref()
        .and_then(stream_sequence_from_ack)
        .unwrap_or(0);
    let version = VersionToken::from_u64(revision);
    let operation = msg
        .headers
        .as_ref()
        .and_then(|h| h.get("KV-Operation"))
        .map(|v| v.as_str());
    Some(match operation {
        Some("DEL") => KvUpdate::Delete { key, version },
        Some("PURGE") => KvUpdate::Purge { key, version },
        // No header (or an explicit "PUT") is a put — the common case carries
        // no KV-Operation header at all.
        _ => KvUpdate::Put(KvEntry {
            key,
            value: msg.payload.to_vec(),
            version,
        }),
    })
}

impl NatsKvWatcher {
    /// Proactive cursor-expiry detection, REQUIRED before any `*_from` resume.
    ///
    /// NATS does **not** error when an ordered consumer's `ByStartSequence`
    /// falls below the stream's first retained sequence — it silently delivers
    /// from the first available message (pinned against a live nats-server by
    /// `tests/resync.rs::nats_silently_clamps_resume_below_first_seq`). A
    /// silent clamp skips the gap's evicted messages — delete markers
    /// included — without ever taking the `CursorExpired` → resync path, so
    /// expiry MUST be detected by comparing the stream's `first_sequence`
    /// against the resume point before trusting the consumer. The
    /// error-string matching at the consumer-create sites stays as a second
    /// line of defense for server versions that do error.
    ///
    /// Why `first_sequence` is the right boundary: interior (per-subject
    /// history) eviction inside the gap is safe for a last-write-wins fold —
    /// an overwrite-evicted revision implies a LATER revision of the same
    /// subject exists and will be delivered. Lost *deletes* come from head
    /// eviction (stream limits/age), which is exactly what advances
    /// `first_sequence`. (An admin interior purge of a subject can also
    /// destroy a delete marker without moving the head — that is a manual
    /// destructive operation, same trust class as deleting the stream.)
    ///
    /// Head eviction racing the window between this check and consumer
    /// creation is the same exposure any live consumer has against
    /// aggressive retention; the check bounds the silent gap to that
    /// milliseconds-scale window, where the prior behavior left it unbounded.
    async fn check_resume_window(&self, revision: u64) -> Result<(), KvError> {
        let stream = timed(self.js.get_stream(format!("KV_{}", self.bucket)))
            .await?
            .map_err(|e| {
                KvError::OperationFailed(format!("get KV stream for resume check: {e}"))
            })?;
        let first = stream.cached_info().state.first_sequence;
        // The shared protocol kernel — the same guard the model checker's
        // Resume transition executes (`crate::protocol::resume_window_ok`).
        if !crate::protocol::resume_window_ok(revision, first) {
            warn!(
                revision,
                first_sequence = first,
                "resume cursor is below the stream's first retained sequence; cursor expired"
            );
            return Err(KvError::CursorExpired);
        }
        Ok(())
    }
}

#[async_trait]
impl KvWatcher for NatsKvWatcher {
    async fn watch_all(&self, tx: Sender<KvUpdate>) -> Result<(), KvError> {
        // `watch_with_history` (DeliverPolicy::LastPerSubject), NOT `watch_all`
        // (DeliverPolicy::New): the trait contract is state-sync — current value
        // of every key first, then live updates. async-nats's `watch_all` only
        // delivers messages published AFTER the consumer exists, which would
        // leave a no-cursor consumer empty until keys happen to change.
        //
        // Bound the watch *setup* with `timed()` for the same reason every KV op
        // is bounded: a half-dead (CLOSE_WAIT) NATS connection parks this await
        // forever instead of failing. The streaming drain in `stream_watch` is
        // intentionally unbounded (a watch is long-lived), but establishing it
        // must not be able to hang a reconnecting caller.
        let watcher = timed(self.kv.watch_with_history(">"))
            .await?
            .map_err(|e| KvError::WatchError(e.to_string()))?;
        stream_watch(watcher, &tx).await
    }

    async fn watch_prefix(&self, prefix: &str, tx: Sender<KvUpdate>) -> Result<(), KvError> {
        // Use native NATS subject-based filtering. KV key "node.abc" maps to
        // subject "$KV.BUCKET.node.abc", and ">" is the multi-level wildcard.
        // `_with_history` for the same state-sync contract as `watch_all`.
        let nats_key = format!("{prefix}>");
        let watcher = timed(self.kv.watch_with_history(&nats_key))
            .await?
            .map_err(|e| KvError::WatchError(e.to_string()))?;
        stream_watch(watcher, &tx).await
    }

    async fn watch_prefixes(&self, prefixes: &[&str], tx: Sender<KvUpdate>) -> Result<(), KvError> {
        if prefixes.is_empty() {
            // Nothing to watch. Critically, do NOT fall through to `watch_many`
            // with an empty filter set — an unfiltered ordered consumer would
            // watch the WHOLE bucket, the opposite of a scoped watch.
            return Ok(());
        }
        // ONE multi-filter consumer for every prefix (NATS 2.10 `filter_subjects`)
        // rather than one consumer per prefix. `watch_many_with_history` builds a
        // single ordered push consumer with `filter_subjects = [{p}> ...]` and
        // yields the same `Entry` stream as `watch`, so `stream_watch` is reused
        // verbatim. This is the per-stream-consumer-count fix: a node scoped to N
        // prefixes costs 1 consumer, not N. `_with_history` for the same
        // state-sync contract as `watch_all`.
        let keys: Vec<String> = prefixes.iter().map(|p| format!("{p}>")).collect();
        let watcher = timed(self.kv.watch_many_with_history(keys))
            .await?
            .map_err(|e| KvError::WatchError(e.to_string()))?;
        stream_watch(watcher, &tx).await
    }

    async fn watch_all_from(
        &self,
        cursor: &WatchCursor,
        tx: Sender<KvUpdate>,
    ) -> Result<(), KvError> {
        let revision = match cursor.as_u64() {
            Some(rev) if rev > 0 => rev,
            _ => return self.watch_all(tx).await,
        };
        self.check_resume_window(revision).await?;

        let watcher = match timed(self.kv.watch_all_from_revision(revision + 1)).await? {
            Ok(w) => w,
            Err(e) => {
                let err_str = e.to_string();
                if is_cursor_expired_error(&err_str) {
                    warn!(revision, error = %err_str, "cursor expired, caller should fall back to full watch");
                    return Err(KvError::CursorExpired);
                }
                return Err(KvError::WatchError(err_str));
            }
        };
        // Re-check AFTER the consumer exists: head eviction in the window
        // between the pre-flight check and consumer creation would otherwise
        // clamp silently.
        self.check_resume_window(revision).await?;

        info!(revision, "resumed watch from cursor");
        // The LIVE floor guard takes over from here: in-band gapped-delivery
        // checks plus a no-traffic backstop, so retention overrunning this
        // watch mid-stream fail-stops into the restart→resync repair path
        // instead of silently skipping evicted deletes (model:
        // tests/model_live_watch.rs).
        stream_watch_floor_guarded(watcher, &tx, revision, &self.js, &self.bucket).await
    }

    async fn watch_prefix_from(
        &self,
        prefix: &str,
        cursor: &WatchCursor,
        tx: Sender<KvUpdate>,
    ) -> Result<(), KvError> {
        let revision = match cursor.as_u64() {
            Some(rev) if rev > 0 => rev,
            _ => return self.watch_prefix(prefix, tx).await,
        };
        self.check_resume_window(revision).await?;

        let nats_key = format!("{prefix}>");
        let watcher = match timed(self.kv.watch_from_revision(&nats_key, revision + 1)).await? {
            Ok(w) => w,
            Err(e) => {
                let err_str = e.to_string();
                if is_cursor_expired_error(&err_str) {
                    warn!(revision, prefix, error = %err_str, "cursor expired for prefix watch, caller should fall back");
                    return Err(KvError::CursorExpired);
                }
                return Err(KvError::WatchError(err_str));
            }
        };
        // Same post-create re-check as watch_all_from: close the
        // check→create eviction window.
        self.check_resume_window(revision).await?;

        info!(revision, prefix, "resumed prefix watch from cursor");
        stream_watch(watcher, &tx).await
    }

    async fn watch_prefixes_from(
        &self,
        prefixes: &[&str],
        cursor: &WatchCursor,
        tx: Sender<KvUpdate>,
    ) -> Result<(), KvError> {
        use async_nats::jetstream::consumer::{DeliverPolicy, ReplayPolicy, push};

        if prefixes.is_empty() {
            // Same guard as watch_prefixes: an empty filter set must not become
            // an unfiltered whole-bucket consumer.
            return Ok(());
        }
        let revision = match cursor.as_u64() {
            Some(rev) if rev > 0 => rev,
            _ => return self.watch_prefixes(prefixes, tx).await,
        };

        // async-nats has `watch_many` (multi-filter) and `watch_from_revision`
        // (seek) but no combination of the two, so build the multi-filter
        // ordered push consumer ourselves — the exact consumer
        // `watch_many_with_deliver_policy` would build, with
        // `ByStartSequence(cursor+1)` for the delta seek. The ordered-consumer
        // machinery (gap detection, auto-recreate from the last delivered
        // sequence) comes with `OrderedConfig` for free.
        let bucket = self.bucket.as_str();
        let kv_prefix = format!("$KV.{bucket}.");
        let filter_subjects: Vec<String> = prefixes
            .iter()
            .map(|p| format!("{kv_prefix}{p}>"))
            .collect();

        let stream = timed(self.js.get_stream(format!("KV_{bucket}")))
            .await?
            .map_err(|e| KvError::WatchError(format!("get KV stream: {e}")))?;

        // Same proactive expiry detection as `check_resume_window` (NATS
        // silently clamps a below-head ByStartSequence; see that method's
        // docs) — checked on the stream handle this path already fetched,
        // via the shared protocol kernel.
        let first = stream.cached_info().state.first_sequence;
        if !crate::protocol::resume_window_ok(revision, first) {
            warn!(
                revision,
                first_sequence = first,
                ?prefixes,
                "resume cursor is below the stream's first retained sequence; cursor expired"
            );
            return Err(KvError::CursorExpired);
        }

        let consumer = match timed(stream.create_consumer(push::OrderedConfig {
            deliver_subject: self.client.new_inbox(),
            description: Some("kv multi-prefix resume consumer".to_string()),
            filter_subjects,
            replay_policy: ReplayPolicy::Instant,
            deliver_policy: DeliverPolicy::ByStartSequence {
                start_sequence: revision + 1,
            },
            ..Default::default()
        }))
        .await?
        {
            Ok(c) => c,
            Err(e) => {
                // Same expiry classification as watch_all_from: a start sequence
                // the stream has compacted past surfaces as a consumer-create
                // error whose message names the sequence problem.
                let err_str = e.to_string();
                if is_cursor_expired_error(&err_str) {
                    warn!(revision, ?prefixes, error = %err_str, "cursor expired for multi-prefix watch, caller should fall back");
                    return Err(KvError::CursorExpired);
                }
                return Err(KvError::WatchError(err_str));
            }
        };

        // Re-check AFTER the consumer exists (fresh stream info, not the
        // handle's cached copy): closes the check→create eviction window,
        // same as the single-filter resume paths.
        self.check_resume_window(revision).await?;

        let mut messages = timed(consumer.messages())
            .await?
            .map_err(|e| KvError::WatchError(e.to_string()))?;

        info!(
            revision,
            ?prefixes,
            "resumed multi-prefix watch from cursor"
        );
        while let Some(msg) = messages.next().await {
            match msg {
                Ok(msg) => {
                    // A subject-filtered consumer only delivers in-keyspace
                    // subjects; `None` here would be a server bug, skipped to
                    // match kv::Watch's tolerance.
                    let Some(update) = kv_message_to_update(&msg, &kv_prefix) else {
                        continue;
                    };
                    if tx.send(update).await.is_err() {
                        debug!("watch receiver closed");
                        break;
                    }
                }
                Err(e) => {
                    error!(error = %e, "NATS KV multi-prefix watch error");
                    return Err(KvError::WatchError(e.to_string()));
                }
            }
        }
        Ok(())
    }
}

struct NatsKvWriterImpl {
    kv: Store,
}

#[async_trait]
impl KvWriter for NatsKvWriterImpl {
    async fn put(&self, key: &str, value: &[u8]) -> Result<VersionToken, KvError> {
        let rev = timed(self.kv.put(key, value.to_vec().into()))
            .await?
            .map_err(|e| KvError::OperationFailed(e.to_string()))?;
        Ok(VersionToken::from_u64(rev))
    }

    async fn delete(&self, key: &str) -> Result<bool, KvError> {
        // NATS delete doesn't tell us if key existed, so we always return true
        timed(self.kv.delete(key))
            .await?
            .map_err(|e| KvError::OperationFailed(e.to_string()))?;
        Ok(true)
    }

    async fn create(&self, key: &str, value: &[u8]) -> Result<VersionToken, KvError> {
        use async_nats::jetstream::kv::CreateErrorKind;
        timed(self.kv.create(key, value.to_vec().into()))
            .await?
            .map(VersionToken::from_u64)
            .map_err(|e| {
                if e.kind() == CreateErrorKind::AlreadyExists {
                    KvError::AlreadyExists
                } else {
                    KvError::OperationFailed(e.to_string())
                }
            })
    }

    async fn update(
        &self,
        key: &str,
        value: &[u8],
        expected: &VersionToken,
    ) -> Result<VersionToken, KvError> {
        use async_nats::jetstream::kv::UpdateErrorKind;
        let rev = expected.as_u64().ok_or_else(|| {
            KvError::OperationFailed("invalid version token for NATS update".into())
        })?;
        timed(self.kv.update(key, value.to_vec().into(), rev))
            .await?
            .map(VersionToken::from_u64)
            .map_err(|e| {
                if e.kind() == UpdateErrorKind::WrongLastRevision {
                    KvError::RevisionMismatch
                } else {
                    KvError::OperationFailed(e.to_string())
                }
            })
    }

    async fn delete_with_version(
        &self,
        key: &str,
        expected: &VersionToken,
    ) -> Result<bool, KvError> {
        use async_nats::jetstream::kv::UpdateErrorKind;
        let rev = expected.as_u64().ok_or_else(|| {
            KvError::OperationFailed("invalid version token for NATS delete".into())
        })?;
        // Write empty value with CAS — logically deletes while preserving conflict detection
        timed(self.kv.update(key, Vec::new().into(), rev))
            .await?
            .map(|_| true)
            .map_err(|e| {
                if e.kind() == UpdateErrorKind::WrongLastRevision {
                    KvError::RevisionMismatch
                } else {
                    KvError::OperationFailed(e.to_string())
                }
            })
    }
}

impl std::fmt::Debug for NatsConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NatsConnection")
            .field("url", &self.config.url)
            // `Acquire` to match every other read of `healthy` — a `Relaxed`
            // outlier here reads like a deliberate exception during an atomics
            // audit, and the fmt path is far too cold for the ordering to cost
            // anything.
            .field("healthy", &self.healthy.load(Ordering::Acquire))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_create_success_has_no_error() {
        // A successful STREAM.CREATE echoes back the stream config, no "error".
        let payload = br#"{"type":"io.nats.jetstream.api.v1.stream_create_response","config":{"name":"KV_certs"}}"#;
        assert_eq!(
            classify_raw_create_response(payload),
            RawCreateOutcome::Created
        );
    }

    #[test]
    fn raw_create_swallows_stream_already_exists() {
        // 10058 = stream name already in use → the bucket already exists, OK.
        let payload =
            br#"{"error":{"code":400,"err_code":10058,"description":"stream name already in use"}}"#;
        assert_eq!(
            classify_raw_create_response(payload),
            RawCreateOutcome::AlreadyExists
        );
    }

    #[test]
    fn raw_create_swallows_stream_limit() {
        // Synadia Cloud returns 400 + "maximum number of streams" at the limit,
        // but the bucket may already exist — treat as non-fatal.
        let payload =
            br#"{"error":{"code":400,"description":"maximum number of streams reached"}}"#;
        assert_eq!(
            classify_raw_create_response(payload),
            RawCreateOutcome::StreamLimit
        );
    }

    #[test]
    fn raw_create_propagates_unknown_error() {
        // Any other JetStream error is fatal and must surface code + description.
        let payload = br#"{"error":{"code":403,"description":"insufficient permissions"}}"#;
        match classify_raw_create_response(payload) {
            RawCreateOutcome::Failed { code, description } => {
                assert_eq!(code, 403);
                assert_eq!(description, "insufficient permissions");
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    #[test]
    fn raw_create_400_without_stream_limit_is_fatal() {
        // A bare 400 that isn't the stream-limit message must NOT be swallowed,
        // otherwise a genuine bad-config rejection would masquerade as success.
        let payload = br#"{"error":{"code":400,"description":"invalid stream config"}}"#;
        match classify_raw_create_response(payload) {
            RawCreateOutcome::Failed { code, description } => {
                assert_eq!(code, 400);
                assert!(description.contains("invalid stream config"));
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    #[test]
    fn raw_create_unparseable_payload_is_treated_as_success() {
        // The caller re-verifies with get_key_value, so a garbled body must not
        // be reported as a hard failure here.
        assert_eq!(
            classify_raw_create_response(b"not json at all"),
            RawCreateOutcome::Created
        );
    }

    #[test]
    fn ack_subject_legacy_format() {
        // $JS.ACK.<stream>.<consumer>.<delivered>.<stream_seq>.<consumer_seq>.<ts>.<pending>
        let reply = "$JS.ACK.KV_certs.cons.1.42.7.1700000000000000000.0";
        assert_eq!(stream_sequence_from_ack(reply), Some(42));
    }

    #[test]
    fn ack_subject_modern_format_with_domain_and_account() {
        // $JS.ACK.<domain>.<account>.<stream>.<consumer>.<delivered>.<stream_seq>.<consumer_seq>.<ts>.<pending>
        let reply = "$JS.ACK.hub.AABBCC.KV_certs.cons.1.42.7.1700000000000000000.0";
        assert_eq!(stream_sequence_from_ack(reply), Some(42));
    }

    #[test]
    fn ack_subject_modern_format_with_trailing_token() {
        // Some servers append a random trailing token (12 tokens total).
        let reply = "$JS.ACK.hub.AABBCC.KV_certs.cons.1.99.7.1700000000000000000.0.rng";
        assert_eq!(stream_sequence_from_ack(reply), Some(99));
    }

    #[test]
    fn ack_subject_last_token_is_not_the_sequence() {
        // Regression guard: the final token is num_pending, never the sequence.
        // The old code returned this (0), corrupting every scanned entry's version.
        let reply = "$JS.ACK.KV_certs.cons.1.42.7.1700000000000000000.0";
        assert_ne!(stream_sequence_from_ack(reply), Some(0));
    }

    #[test]
    fn ack_subject_rejects_garbage() {
        assert_eq!(stream_sequence_from_ack(""), None);
        assert_eq!(stream_sequence_from_ack("not.an.ack.subject"), None);
        assert_eq!(stream_sequence_from_ack("$JS.ACK.too.few.tokens"), None);
        // Right shape, non-numeric sequence field.
        assert_eq!(stream_sequence_from_ack("$JS.ACK.s.c.1.notnum.7.0.0"), None);
    }

    #[test]
    fn cursor_expired_matches_known_nats_error_strings() {
        // These substrings come from async-nats error messages. If the library
        // rewrites them, watch_all_from would return WatchError instead of
        // CursorExpired, breaking callers that fall back to watch_all() on expiry.
        assert!(is_cursor_expired_error(
            "consumer start sequence is too old"
        ));
        assert!(is_cursor_expired_error("first sequence is 42, requested 1"));
        assert!(is_cursor_expired_error("sequence not found in stream"));
        // "too old" on its own (no "sequence" wording) must still be caught.
        assert!(is_cursor_expired_error("requested revision is too old"));
        // Case-insensitive: a capitalization change upstream must not slip past.
        assert!(is_cursor_expired_error("Consumer Start Sequence Too Old"));
        assert!(!is_cursor_expired_error("connection refused"));
        assert!(!is_cursor_expired_error("permission denied"));
        assert!(!is_cursor_expired_error("stream not found"));
    }

    fn raw_kv_msg(
        subject: &str,
        reply: Option<&str>,
        payload: &[u8],
        op: Option<&str>,
    ) -> async_nats::Message {
        let headers = op.map(|op| {
            let mut h = async_nats::HeaderMap::new();
            h.insert("KV-Operation", op);
            h
        });
        async_nats::Message {
            subject: subject.to_string().into(),
            reply: reply.map(|r| r.to_string().into()),
            payload: payload.to_vec().into(),
            headers,
            status: None,
            description: None,
            length: 0,
        }
    }

    const ACK_42: &str = "$JS.ACK.KV_certs.cons.1.42.7.1700000000000000000.0";

    #[test]
    fn kv_message_decodes_put_without_operation_header() {
        // The common case: a put carries no KV-Operation header at all.
        let msg = raw_kv_msg("$KV.certs.node.a", Some(ACK_42), b"v1", None);
        match kv_message_to_update(&msg, "$KV.certs.").expect("in keyspace") {
            KvUpdate::Put(e) => {
                assert_eq!(e.key, "node.a");
                assert_eq!(e.value, b"v1");
                assert_eq!(e.version.as_u64(), Some(42));
            }
            other => panic!("expected Put, got {other:?}"),
        }
    }

    #[test]
    fn kv_message_decodes_delete_and_purge_markers() {
        let msg = raw_kv_msg("$KV.certs.node.a", Some(ACK_42), b"", Some("DEL"));
        assert!(matches!(
            kv_message_to_update(&msg, "$KV.certs.").expect("in keyspace"),
            KvUpdate::Delete { ref key, ref version } if key == "node.a" && version.as_u64() == Some(42)
        ));

        let msg = raw_kv_msg("$KV.certs.node.a", Some(ACK_42), b"", Some("PURGE"));
        assert!(matches!(
            kv_message_to_update(&msg, "$KV.certs.").expect("in keyspace"),
            KvUpdate::Purge { ref key, .. } if key == "node.a"
        ));
    }

    #[test]
    fn kv_message_outside_keyspace_is_skipped() {
        // A subject-filtered consumer should never deliver this; the decode
        // skips rather than mis-keys it.
        let msg = raw_kv_msg("$KV.other.node.a", Some(ACK_42), b"v", None);
        assert!(kv_message_to_update(&msg, "$KV.certs.").is_none());
    }

    #[test]
    fn kv_message_without_reply_gets_revision_zero() {
        // No ACK reply subject → revision unparseable → 0, the same "unknown
        // version" convention scan() uses.
        let msg = raw_kv_msg("$KV.certs.node.a", None, b"v", None);
        match kv_message_to_update(&msg, "$KV.certs.").expect("in keyspace") {
            KvUpdate::Put(e) => assert_eq!(e.version.as_u64(), Some(0)),
            other => panic!("expected Put, got {other:?}"),
        }
    }

    #[test]
    fn raw_create_already_exists_when_10058_in_code_field() {
        // Some Synadia Cloud deployments echo 10058 in `code` rather than
        // `err_code`. Both paths must return AlreadyExists, not Failed.
        let payload = br#"{"error":{"code":10058,"description":"stream name already in use"}}"#;
        assert_eq!(
            classify_raw_create_response(payload),
            RawCreateOutcome::AlreadyExists
        );
    }

    #[test]
    fn raw_create_error_without_code_defaults_to_zero() {
        // Defensive: a malformed error object still classifies as Failed rather
        // than silently passing, with code defaulting to 0.
        let payload = br#"{"error":{"description":"mystery"}}"#;
        match classify_raw_create_response(payload) {
            RawCreateOutcome::Failed { code, description } => {
                assert_eq!(code, 0);
                assert_eq!(description, "mystery");
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    }
}

/// Live-server conformance tests for the floor guard
/// ([`stream_watch_floor_guarded`]) — these drive the guarded loop DIRECTLY
/// with a deliberately clamped `Watch`, which reproduces exactly the state
/// retention leaves behind when it overruns a live consumer (the watcher
/// methods' resume-time checks can't be raced deterministically from
/// outside, but the guarded loop neither knows nor cares how its watch got
/// clamped). Spawns a throwaway `nats-server` (mise-installed, same pattern
/// as tests/common).
#[cfg(test)]
mod floor_guard_tests {
    use super::*;
    use std::process::{Child, Command, Stdio};

    struct TestServer {
        child: Child,
        url: String,
        _dir: tempfile::TempDir,
    }

    impl Drop for TestServer {
        fn drop(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }

    async fn start_server() -> TestServer {
        let bin = std::env::var("NATS_SERVER_BIN").unwrap_or_else(|_| "nats-server".into());
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let dir = tempfile::tempdir().unwrap();
        let child = Command::new(&bin)
            .args([
                "--jetstream",
                "--addr",
                "127.0.0.1",
                "--port",
                &port.to_string(),
                "--store_dir",
                dir.path().to_str().unwrap(),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap_or_else(|e| panic!("spawn {bin}: {e}; run `mise install`"));
        let server = TestServer {
            child,
            url: format!("nats://127.0.0.1:{port}"),
            _dir: dir,
        };
        for _ in 0..100 {
            if async_nats::connect(&server.url).await.is_ok() {
                return server;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("nats-server never became ready");
    }

    /// `(js, kv store)` with five revisions across five subjects (history 1).
    async fn seeded_bucket(
        url: &str,
    ) -> (
        async_nats::jetstream::Context,
        async_nats::jetstream::kv::Store,
    ) {
        let client = async_nats::connect(url).await.unwrap();
        let js = async_nats::jetstream::new(client);
        let kv = js
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: "guard".into(),
                history: 1,
                ..Default::default()
            })
            .await
            .unwrap();
        for i in 1..=5u8 {
            kv.put(format!("k{i}"), vec![i].into()).await.unwrap();
        }
        (js, kv)
    }

    /// TRUE POSITIVE: the watch was clamped past evicted revisions (purge
    /// advanced first_seq beyond the frontier) — the first gapped delivery
    /// must trip the guard BEFORE the entry is processed, never silently
    /// folding past the lost range. This is the live twin of the model's
    /// `GuardRepair`-only-progress gate.
    #[tokio::test(flavor = "multi_thread")]
    async fn gapped_delivery_with_advanced_floor_trips() {
        let server = start_server().await;
        let (js, kv) = seeded_bucket(&server.url).await;

        // Evict revisions 1-3 outright: first_sequence becomes 4.
        let mut stream = js.get_stream("KV_guard").await.unwrap();
        stream.purge().sequence(4).await.unwrap();
        assert_eq!(stream.info().await.unwrap().state.first_sequence, 4);

        // A consumer resuming from revision 1 gets CLAMPED to revision 4
        // (NATS's silent skip, pinned by tests/resync.rs). Hand that watch
        // to the guarded loop as a live consumer whose retention just
        // overran it.
        let watch = kv.watch_all_from_revision(2).await.unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::channel(64);
        let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });

        let err = stream_watch_floor_guarded(watch, &tx, 1, &js, "guard")
            .await
            .expect_err("a gapped delivery over an advanced floor must trip");
        assert!(
            err.to_string().contains("retention overran live watch"),
            "{err}"
        );
        drop(tx);
        let _ = drain.await;
    }

    /// NO FALSE POSITIVE: interior (per-subject) eviction also gaps the
    /// delivered revisions, but the floor stays at or below the frontier —
    /// benign for a last-write-wins fold, and the guard must let it
    /// through. (Every existing bootstrap e2e also rides this path on its
    /// resume; this pins the discrimination explicitly.)
    #[tokio::test(flavor = "multi_thread")]
    async fn benign_interior_gap_passes() {
        let server = start_server().await;
        let (js, kv) = seeded_bucket(&server.url).await;

        // Overwrite k2 and k3: revisions 2 and 3 are interior-evicted
        // (history 1), revisions 6 and 7 replace them. first_sequence stays
        // 1 (k1's revision is retained).
        kv.put("k2", vec![22].into()).await.unwrap();
        kv.put("k3", vec![33].into()).await.unwrap();
        let mut stream = js.get_stream("KV_guard").await.unwrap();
        assert_eq!(stream.info().await.unwrap().state.first_sequence, 1);

        // Resume from revision 1: deliveries jump 2 and 3 — gapped, benign.
        let watch = kv.watch_all_from_revision(2).await.unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::channel(64);
        let guard =
            tokio::spawn(
                async move { stream_watch_floor_guarded(watch, &tx, 1, &js, "guard").await },
            );

        let mut got = Vec::new();
        while got.len() < 4 {
            let update = tokio::time::timeout(Duration::from_secs(5), rx.recv())
                .await
                .expect("deliveries continue past benign gaps")
                .expect("watch alive");
            got.push(update.version().as_u64().unwrap());
        }
        assert_eq!(got, vec![4, 5, 6, 7], "interior gaps jumped, tail dense");
        guard.abort(); // endless live watch; the assertion above is the test
    }
}
