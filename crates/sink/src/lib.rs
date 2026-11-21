//! The sink: fake customer webhook endpoints for tests and load runs.
//!
//! The URL path chooses how the endpoint behaves; the last path segment is a
//! free-form name so every test or load endpoint gets its own counters:
//!
//! | Path                              | Behaviour                                           |
//! |-----------------------------------|-----------------------------------------------------|
//! | `/ok/{name}`                      | 200                                                 |
//! | `/fail/{name}`                    | 500                                                 |
//! | `/slow/{name}?ms=2000`            | waits `ms`, then 200                                |
//! | `/timeout/{name}`                 | waits 60 s (longer than Hooklane's 15 s timeout)    |
//! | `/ratelimit/{name}?retry_after=2` | 429 with `Retry-After`                              |
//! | `/gone/{name}`                    | 410                                                 |
//! | `/notfound/{name}`                | 404                                                 |
//! | `/flaky/{name}?pct=30`            | 500 for `pct`% of requests (random), else 200       |
//! | `/failfirst/{name}?n=2`           | 500 for the first `n` requests of each webhook-id   |
//! | `/redirect/{name}`                | 302 to `/ok/{name}` (Hooklane must not follow)      |
//! | `/switch/{name}`                  | status set by `POST /_admin/switch` (default 200)   |
//!
//! Every request's `webhook-id` is recorded per path, so a load run can count
//! lost and duplicate deliveries exactly. If a secret was registered for the
//! path (`POST /_admin/secrets`), the signature is verified with the official
//! `standardwebhooks` library and failures are counted.
//!
//! Admin routes: `POST /_admin/secrets`, `POST /_admin/switch`, `GET /_admin/stats?prefix=`,
//! `GET /_admin/export` (NDJSON), `GET /_admin/requests?path=`, `POST /_admin/reset`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use rand::RngExt;
use serde::{Deserialize, Serialize};

/// Raw requests kept per path, for tests that inspect headers.
const MAX_LOG_PER_PATH: usize = 2000;

/// Aggregate per (path, webhook-id).
#[derive(Debug, Clone, Default, Serialize)]
pub struct IdRecord {
    /// Requests received with this id.
    pub count: u32,
    /// Requests we answered with 2xx.
    pub ok: u32,
    /// Requests whose signature failed verification.
    pub bad_sig: u32,
    pub first_ms: u64,
    pub first_ok_ms: Option<u64>,
}

/// One logged request (for tests).
#[derive(Debug, Clone, Serialize)]
pub struct LoggedRequest {
    pub webhook_id: String,
    pub timestamp: String,
    pub signature: String,
    pub sequence: String,
    pub attempt: String,
    pub user_agent: String,
    pub content_type: String,
    pub body: String,
    pub status: u16,
    pub sig_ok: Option<bool>,
    pub received_ms: u64,
}

#[derive(Default)]
struct Inner {
    secrets: HashMap<String, String>,
    ids: HashMap<String, HashMap<String, IdRecord>>,
    log: HashMap<String, Vec<LoggedRequest>>,
    flaky_counts: HashMap<(String, String), u32>,
    switches: HashMap<String, u16>,
}

/// Shared sink state. Cloning is cheap (an `Arc`).
#[derive(Clone, Default)]
pub struct Sink {
    inner: Arc<Mutex<Inner>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Stats {
    pub paths: usize,
    pub unique_ids: u64,
    pub requests: u64,
    pub ok_requests: u64,
    /// Ids answered 2xx more than once (each extra 2xx counts one).
    pub duplicate_ok: u64,
    pub bad_signatures: u64,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

impl Sink {
    pub fn new() -> Sink {
        Sink::default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        match self.inner.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        }
    }

    pub fn register_secret(&self, path: &str, secret: &str) {
        self.lock()
            .secrets
            .insert(normalise(path), secret.to_string());
    }

    /// Set the status code a `/switch/...` path answers with.
    pub fn set_switch(&self, path: &str, status: u16) {
        self.lock().switches.insert(normalise(path), status);
    }

    fn switch_status(&self, path: &str) -> u16 {
        self.lock().switches.get(path).copied().unwrap_or(200)
    }

    pub fn stats(&self, prefix: &str) -> Stats {
        let inner = self.lock();
        let mut s = Stats::default();
        for (path, ids) in inner.ids.iter().filter(|(p, _)| p.starts_with(prefix)) {
            let _ = path;
            s.paths += 1;
            for r in ids.values() {
                s.unique_ids += 1;
                s.requests += u64::from(r.count);
                s.ok_requests += u64::from(r.ok);
                s.duplicate_ok += u64::from(r.ok.saturating_sub(1));
                s.bad_signatures += u64::from(r.bad_sig);
            }
        }
        s
    }

    /// Per-id records for one path.
    pub fn ids(&self, path: &str) -> HashMap<String, IdRecord> {
        self.lock()
            .ids
            .get(&normalise(path))
            .cloned()
            .unwrap_or_default()
    }

    /// Raw request log for one path, oldest first.
    pub fn requests(&self, path: &str) -> Vec<LoggedRequest> {
        self.lock()
            .log
            .get(&normalise(path))
            .cloned()
            .unwrap_or_default()
    }

    pub fn reset(&self) {
        let mut inner = self.lock();
        inner.ids.clear();
        inner.log.clear();
        inner.flaky_counts.clear();
    }

    fn record(&self, path: &str, headers: &HeaderMap, body: &Bytes, status: u16) {
        let h = |name: &str| {
            headers
                .get(name)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_string()
        };
        let webhook_id = h("webhook-id");
        let mut inner = self.lock();
        let sig_ok = inner.secrets.get(path).map(|secret| {
            standardwebhooks::Webhook::new(secret)
                .map(|wh| wh.verify(body, headers).is_ok())
                .unwrap_or(false)
        });
        let now = now_ms();
        let rec = inner
            .ids
            .entry(path.to_string())
            .or_default()
            .entry(webhook_id.clone())
            .or_default();
        if rec.count == 0 {
            rec.first_ms = now;
        }
        rec.count += 1;
        if (200..300).contains(&status) {
            rec.ok += 1;
            rec.first_ok_ms.get_or_insert(now);
        }
        if sig_ok == Some(false) {
            rec.bad_sig += 1;
        }
        let log = inner.log.entry(path.to_string()).or_default();
        if log.len() < MAX_LOG_PER_PATH {
            log.push(LoggedRequest {
                webhook_id,
                timestamp: h("webhook-timestamp"),
                signature: h("webhook-signature"),
                sequence: h("webhook-sequence"),
                attempt: h("webhook-attempt"),
                user_agent: h("user-agent"),
                content_type: h("content-type"),
                body: String::from_utf8_lossy(body).into_owned(),
                status,
                sig_ok,
                received_ms: now,
            });
        }
    }

    /// Count of requests seen so far for (path, id), including this one.
    fn bump_attempt(&self, path: &str, id: &str) -> u32 {
        let mut inner = self.lock();
        let c = inner
            .flaky_counts
            .entry((path.to_string(), id.to_string()))
            .or_insert(0);
        *c += 1;
        *c
    }
}

fn normalise(path: &str) -> String {
    let p = path.split('?').next().unwrap_or(path);
    if p.starts_with('/') {
        p.to_string()
    } else {
        format!("/{p}")
    }
}

#[derive(Debug, Deserialize, Default)]
struct BehaviourParams {
    ms: Option<u64>,
    retry_after: Option<String>,
    pct: Option<u32>,
    n: Option<u32>,
}

async fn hook(
    State(sink): State<Sink>,
    Path((behaviour, name)): Path<(String, String)>,
    Query(params): Query<BehaviourParams>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let path = format!("/{behaviour}/{name}");
    let webhook_id = headers
        .get("webhook-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();

    let (status, extra): (StatusCode, Option<(&'static str, String)>) = match behaviour.as_str() {
        "ok" => (StatusCode::OK, None),
        "fail" => (StatusCode::INTERNAL_SERVER_ERROR, None),
        "slow" => {
            tokio::time::sleep(Duration::from_millis(params.ms.unwrap_or(2000))).await;
            (StatusCode::OK, None)
        }
        "timeout" => {
            tokio::time::sleep(Duration::from_millis(params.ms.unwrap_or(60_000))).await;
            (StatusCode::OK, None)
        }
        "ratelimit" => (
            StatusCode::TOO_MANY_REQUESTS,
            Some((
                "retry-after",
                params
                    .retry_after
                    .clone()
                    .unwrap_or_else(|| "2".to_string()),
            )),
        ),
        "gone" => (StatusCode::GONE, None),
        "notfound" => (StatusCode::NOT_FOUND, None),
        "flaky" => {
            let pct = params.pct.unwrap_or(30).min(100);
            if rand::rng().random_range(0..100) < pct {
                (StatusCode::INTERNAL_SERVER_ERROR, None)
            } else {
                (StatusCode::OK, None)
            }
        }
        "failfirst" => {
            let n = params.n.unwrap_or(1);
            if sink.bump_attempt(&path, &webhook_id) <= n {
                (StatusCode::INTERNAL_SERVER_ERROR, None)
            } else {
                (StatusCode::OK, None)
            }
        }
        "switch" => (
            StatusCode::from_u16(sink.switch_status(&path)).unwrap_or(StatusCode::OK),
            None,
        ),
        "redirect" => (StatusCode::FOUND, Some(("location", format!("/ok/{name}")))),
        _ => (StatusCode::NOT_FOUND, None),
    };

    sink.record(&path, &headers, &body, status.as_u16());
    let mut resp = (status, "").into_response();
    if let Some((k, v)) = extra
        && let Ok(value) = v.parse()
    {
        resp.headers_mut().insert(k, value);
    }
    resp
}

#[derive(Debug, Deserialize)]
struct SecretBody {
    path: String,
    secret: String,
}

async fn register_secret(
    State(sink): State<Sink>,
    axum::Json(b): axum::Json<SecretBody>,
) -> StatusCode {
    sink.register_secret(&b.path, &b.secret);
    StatusCode::NO_CONTENT
}

#[derive(Debug, Deserialize)]
struct SwitchBody {
    path: String,
    status: u16,
}

async fn set_switch(State(sink): State<Sink>, axum::Json(b): axum::Json<SwitchBody>) -> StatusCode {
    sink.set_switch(&b.path, b.status);
    StatusCode::NO_CONTENT
}

#[derive(Debug, Deserialize)]
struct PrefixParams {
    prefix: Option<String>,
}

async fn stats(State(sink): State<Sink>, Query(p): Query<PrefixParams>) -> axum::Json<Stats> {
    axum::Json(sink.stats(p.prefix.as_deref().unwrap_or("/")))
}

/// One JSON object per line: `{"path","id","count","ok","bad_sig","first_ms","first_ok_ms"}`.
async fn export(State(sink): State<Sink>, Query(p): Query<PrefixParams>) -> Response {
    let prefix = p.prefix.unwrap_or_else(|| "/".to_string());
    let inner = sink.lock();
    let mut out = String::new();
    for (path, ids) in inner.ids.iter().filter(|(p, _)| p.starts_with(&prefix)) {
        for (id, r) in ids {
            let line = serde_json::json!({
                "path": path, "id": id, "count": r.count, "ok": r.ok, "bad_sig": r.bad_sig,
                "first_ms": r.first_ms, "first_ok_ms": r.first_ok_ms,
            });
            out.push_str(&line.to_string());
            out.push('\n');
        }
    }
    ([("content-type", "application/x-ndjson")], out).into_response()
}

#[derive(Debug, Deserialize)]
struct PathParams {
    path: String,
}

async fn requests(
    State(sink): State<Sink>,
    Query(p): Query<PathParams>,
) -> axum::Json<Vec<LoggedRequest>> {
    axum::Json(sink.requests(&p.path))
}

async fn reset(State(sink): State<Sink>) -> StatusCode {
    sink.reset();
    StatusCode::NO_CONTENT
}

pub fn router(sink: Sink) -> Router {
    Router::new()
        .route("/_admin/secrets", post(register_secret))
        .route("/_admin/switch", post(set_switch))
        .route("/_admin/stats", get(stats))
        .route("/_admin/export", get(export))
        .route("/_admin/requests", get(requests))
        .route("/_admin/reset", post(reset))
        .route("/healthz", get(|| async { "ok" }))
        .route("/{behaviour}/{name}", post(hook))
        .with_state(sink)
}

/// Start a sink on `addr` (use "127.0.0.1:0" for a random port). Returns the
/// shared state and the bound address. Used by integration tests.
pub async fn spawn(addr: &str) -> std::io::Result<(Sink, std::net::SocketAddr)> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let local = listener.local_addr()?;
    let sink = Sink::new();
    let app = router(sink.clone());
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Ok((sink, local))
}
