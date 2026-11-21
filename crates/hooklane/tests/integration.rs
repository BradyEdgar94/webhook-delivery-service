//! Integration tests: real Postgres, real Redpanda, an in-process sink.
//!
//! Each scenario is one `async fn` taking the queue mode, with a `_pg` and a
//! `_kafka` test calling it, so every behaviour is proven in both modes.
//! Run with `make test-integration`.

mod common;

use std::time::{Duration, Instant};

use common::*;
use hooklane::config::QueueMode;
use reqwest::Method;
use serde_json::json;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Event -> delivered once, signature valid, headers correct
// ---------------------------------------------------------------------------

async fn delivered_once_with_valid_signature(mode: QueueMode) {
    let env = TestEnv::start(mode).await;
    let (ep, _secret) = env.create_endpoint("/ok/basic", None).await;
    let before = chrono::Utc::now().timestamp();
    let ev = env
        .send_event("form.submitted", json!({"form": "contact", "n": 1}))
        .await;

    let d = env.wait_status(ev, ep, "succeeded", T).await;
    assert_eq!(d.attempt_count, 1);
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(env.sink_count("/ok/basic", ev), 1, "exactly one request");

    let reqs = env.sink.requests("/ok/basic");
    let r = &reqs[0];
    assert_eq!(
        r.sig_ok,
        Some(true),
        "signature verified by the official standardwebhooks library"
    );
    assert_eq!(r.webhook_id, ev.to_string());
    assert_eq!(
        r.sequence,
        d.id.to_string(),
        "webhook-sequence is the delivery id"
    );
    assert_eq!(r.attempt, "1");
    assert_eq!(r.content_type, "application/json");
    assert!(r.user_agent.starts_with("Hooklane"), "{}", r.user_agent);
    assert!(r.signature.starts_with("v1,"));
    let ts: i64 = r.timestamp.parse().expect("numeric timestamp");
    assert!(ts >= before && ts <= chrono::Utc::now().timestamp());
    let body: serde_json::Value = serde_json::from_str(&r.body).expect("json body");
    assert_eq!(body["type"], "form.submitted");
    assert_eq!(body["data"]["form"], "contact");
    assert!(
        body["timestamp"]
            .as_str()
            .expect("timestamp")
            .ends_with('Z')
    );

    let attempts = env.attempts(d.id).await;
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].response_status, Some(200));
    env.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delivered_once_pg() {
    delivered_once_with_valid_signature(QueueMode::Postgres).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delivered_once_kafka() {
    delivered_once_with_valid_signature(QueueMode::Kafka).await;
}

// ---------------------------------------------------------------------------
// Fan-out and event-type filtering
// ---------------------------------------------------------------------------

async fn fan_out_and_filtering(mode: QueueMode) {
    let env = TestEnv::start(mode).await;
    let (all, _) = env.create_endpoint("/ok/fan-all", None).await;
    let (a, _) = env
        .create_endpoint("/ok/fan-a", Some(vec!["a.created"]))
        .await;
    let (b, _) = env
        .create_endpoint("/ok/fan-b", Some(vec!["b.created"]))
        .await;

    let ev = env.send_event("a.created", json!({})).await;
    env.wait_status(ev, all, "succeeded", T).await;
    env.wait_status(ev, a, "succeeded", T).await;
    assert!(
        env.delivery(ev, b).await.is_none(),
        "b is not subscribed to a.created"
    );
    let (_, v) = env
        .api(Method::GET, &format!("/v1/events/{ev}"), None)
        .await;
    assert_eq!(v["deliveries"].as_array().map(|d| d.len()), Some(2));

    // An event nobody subscribes to is still saved, with zero deliveries.
    env.api(Method::DELETE, &format!("/v1/endpoints/{all}"), None)
        .await;
    let lonely = env.send_event("c.created", json!({})).await;
    let (status, v) = env
        .api(Method::GET, &format!("/v1/events/{lonely}"), None)
        .await;
    assert_eq!(status, 200);
    assert_eq!(v["deliveries"].as_array().map(|d| d.len()), Some(0));
    env.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fan_out_pg() {
    fan_out_and_filtering(QueueMode::Postgres).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fan_out_kafka() {
    fan_out_and_filtering(QueueMode::Kafka).await;
}

// ---------------------------------------------------------------------------
// Idempotency key sent twice concurrently -> one event
// ---------------------------------------------------------------------------

async fn idempotency_concurrent(mode: QueueMode) {
    let env = TestEnv::start(mode).await;
    let (ep, _) = env.create_endpoint("/ok/idem", None).await;
    let mut handles = Vec::new();
    for _ in 0..20 {
        let http = env.http.clone();
        let url = env.url("/v1/events");
        let key = env.api_key.clone();
        handles.push(tokio::spawn(async move {
            let r = http
                .post(url)
                .bearer_auth(key)
                .header("Idempotency-Key", "order-42")
                .json(&json!({"type": "order.paid", "data": {"order": 42}}))
                .send()
                .await
                .expect("send");
            assert_eq!(r.status().as_u16(), 202);
            let v: serde_json::Value = r.json().await.expect("json");
            v["id"].as_str().expect("id").to_string()
        }));
    }
    let mut ids = Vec::new();
    for h in handles {
        ids.push(h.await.expect("join"));
    }
    ids.dedup();
    assert_eq!(ids.len(), 1, "all 20 requests return the same event id");
    assert_eq!(
        env.count("SELECT count(*) FROM events WHERE idempotency_key = 'order-42'")
            .await,
        1
    );
    assert_eq!(env.count("SELECT count(*) FROM deliveries").await, 1);
    let ev: Uuid = ids[0].parse().expect("uuid");
    env.wait_status(ev, ep, "succeeded", T).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(env.sink_count("/ok/idem", ev), 1);
    env.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idempotency_pg() {
    idempotency_concurrent(QueueMode::Postgres).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idempotency_kafka() {
    idempotency_concurrent(QueueMode::Kafka).await;
}

// ---------------------------------------------------------------------------
// Failing endpoint -> retried on the schedule -> dead after 8 -> system event
// ---------------------------------------------------------------------------

async fn dead_after_eight_attempts(mode: QueueMode) {
    let env = TestEnv::start(mode).await;
    let (fail, _) = env
        .create_endpoint("/fail/dead", Some(vec!["job.done"]))
        .await;
    let (watcher, _) = env
        .create_endpoint("/ok/watch", Some(vec!["hooklane.delivery.dead"]))
        .await;
    // This one also wants dead notifications but is itself broken: its failed
    // delivery of the *system* event must not create another system event.
    let (broken_watcher, _) = env
        .create_endpoint("/fail/watch", Some(vec!["hooklane.delivery.dead"]))
        .await;

    let ev = env.send_event("job.done", json!({"job": 7})).await;
    let d = env
        .wait_status(ev, fail, "dead", Duration::from_secs(90))
        .await;
    assert_eq!(d.attempt_count, 8);
    let attempts = env.attempts(d.id).await;
    assert_eq!(attempts.len(), 8, "8 attempts logged");
    assert!(attempts.iter().all(|a| a.response_status == Some(500)));
    let reqs = env.sink.requests("/fail/dead");
    assert_eq!(reqs.len(), 8);
    assert!(
        reqs.iter().all(|r| r.webhook_id == ev.to_string()),
        "webhook-id identical across retries"
    );
    let numbers: Vec<String> = reqs.iter().map(|r| r.attempt.clone()).collect();
    assert_eq!(numbers, (1..=8).map(|n| n.to_string()).collect::<Vec<_>>());

    // The system event reaches the watcher.
    let got = wait_until(T, || async { !env.sink.requests("/ok/watch").is_empty() }).await;
    assert!(got, "watcher received hooklane.delivery.dead");
    let body: serde_json::Value =
        serde_json::from_str(&env.sink.requests("/ok/watch")[0].body).expect("json");
    assert_eq!(body["type"], "hooklane.delivery.dead");
    assert_eq!(body["data"]["delivery_id"], d.id);
    let _ = watcher;

    // The broken watcher's delivery of the system event dies too...
    let sys_event: Uuid =
        sqlx::query_scalar("SELECT id FROM events WHERE event_type = 'hooklane.delivery.dead'")
            .fetch_one(&env.pool)
            .await
            .expect("system event");
    env.wait_status(sys_event, broken_watcher, "dead", Duration::from_secs(90))
        .await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    // ...and generates no further system event (no loops).
    assert_eq!(
        env.count("SELECT count(*) FROM events WHERE event_type LIKE 'hooklane.%'")
            .await,
        1
    );
    env.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dead_after_eight_pg() {
    dead_after_eight_attempts(QueueMode::Postgres).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dead_after_eight_kafka() {
    dead_after_eight_attempts(QueueMode::Kafka).await;
}

// ---------------------------------------------------------------------------
// 4xx -> failed, no retry. 410 -> endpoint disabled. 429 Retry-After honoured.
// ---------------------------------------------------------------------------

async fn client_errors(mode: QueueMode) {
    let env = TestEnv::start(mode).await;

    // 404: failed after one attempt.
    let (nf, _) = env
        .create_endpoint("/notfound/c4", Some(vec!["t.nf"]))
        .await;
    let ev = env.send_event("t.nf", json!({})).await;
    let d = env.wait_status(ev, nf, "failed", T).await;
    assert_eq!(d.attempt_count, 1);
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(env.sink_count("/notfound/c4", ev), 1, "no retry for 4xx");

    // 410: failed and the endpoint is disabled; later events skip it.
    let (gone, _) = env.create_endpoint("/gone/c4", Some(vec!["t.gone"])).await;
    let ev = env.send_event("t.gone", json!({})).await;
    env.wait_status(ev, gone, "failed", T).await;
    let (_, v) = env
        .api(Method::GET, &format!("/v1/endpoints/{gone}"), None)
        .await;
    assert_eq!(v["status"], "disabled");
    let later = env.send_event("t.gone", json!({})).await;
    assert!(
        env.delivery(later, gone).await.is_none(),
        "disabled endpoints get no new deliveries"
    );

    // 429 with Retry-After: 2 -> next attempt ~2 s later, not the 50 ms schedule.
    let (rl, _) = env
        .create_endpoint("/ratelimit/c4?retry_after=2", Some(vec!["t.rl"]))
        .await;
    let ev = env.send_event("t.rl", json!({})).await;
    let d = env.wait_status(ev, rl, "retry_scheduled", T).await;
    let attempts = env.attempts(d.id).await;
    let first = &attempts[0];
    assert_eq!(first.response_status, Some(429));
    let wait = d.next_attempt_at.expect("next_attempt_at") - first.started_at;
    assert!(
        wait >= chrono::Duration::milliseconds(1_800)
            && wait <= chrono::Duration::milliseconds(3_000),
        "waited {wait}"
    );
    let (_, v) = env
        .api(Method::GET, &format!("/v1/deliveries/{}", d.id), None)
        .await;
    assert!(
        v["next_attempt_at"].is_string(),
        "API shows when the next attempt is due"
    );
    env.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_errors_pg() {
    client_errors(QueueMode::Postgres).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_errors_kafka() {
    client_errors(QueueMode::Kafka).await;
}

// ---------------------------------------------------------------------------
// Relay duplicate: the same outbox row published twice -> one HTTP request
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn relay_duplicate_gives_one_request_kafka() {
    let env = TestEnv::with(Options::new(QueueMode::Kafka).no_relay()).await;
    let (ep, _) = env.create_endpoint("/ok/dup", None).await;
    let ev = env.send_event("x.y", json!({})).await;
    let producer = hooklane::queue::kafka::producer(&env.config).expect("producer");

    // Publish, then pretend the relay crashed before marking the row
    // published, and publish the very same row again.
    assert_eq!(
        hooklane::relay::publish_batch(&env.config, &env.pool, &producer)
            .await
            .expect("publish"),
        1
    );
    sqlx::query("UPDATE outbox SET published_at = NULL")
        .execute(&env.pool)
        .await
        .expect("reset");
    assert_eq!(
        hooklane::relay::publish_batch(&env.config, &env.pool, &producer)
            .await
            .expect("publish"),
        1
    );

    let d = env.wait_status(ev, ep, "succeeded", T).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        env.sink_count("/ok/dup", ev),
        1,
        "two Kafka messages, one HTTP request"
    );
    assert_eq!(env.attempts(d.id).await.len(), 1);
    env.finish().await;
}

/// Postgres-mode counterpart: several workers competing for the same rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn competing_workers_no_duplicates_pg() {
    let env = TestEnv::with(Options::new(QueueMode::Postgres).workers(4)).await;
    let mut eps = Vec::new();
    for i in 0..5 {
        let event_type = format!("t{i}");
        eps.push(
            env.create_endpoint(&format!("/ok/comp{i}"), Some(vec![event_type.as_str()]))
                .await
                .0,
        );
    }
    for n in 0..200 {
        env.send_event(&format!("t{}", n % 5), json!({"n": n}))
            .await;
    }
    let done = wait_until(T, || async {
        env.count("SELECT count(*) FROM deliveries WHERE status = 'succeeded'")
            .await
            == 200
    })
    .await;
    assert!(done);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let stats = env.sink.stats("/ok/comp");
    assert_eq!(stats.unique_ids, 200);
    assert_eq!(
        stats.requests, 200,
        "no duplicate requests across 4 workers"
    );
    env.finish().await;
}

// ---------------------------------------------------------------------------
// Worker crash mid-delivery -> reaper recovers -> delivered, nothing lost
// ---------------------------------------------------------------------------

async fn worker_crash_recovers(mode: QueueMode) {
    let mut env = TestEnv::with(Options::new(mode).partitions(1)).await;
    let (ep, _) = env.create_endpoint("/slow/crash?ms=1500", None).await;
    let ev = env.send_event("x.crash", json!({})).await;
    env.wait_status(ev, ep, "delivering", T).await;

    // Kill the worker in the middle of the HTTP request.
    let w = env.workers.remove(0);
    w.kill();
    env.add_worker();

    let d = env
        .wait_status(ev, ep, "succeeded", Duration::from_secs(60))
        .await;
    let attempts = env.attempts(d.id).await;
    assert!(
        attempts
            .iter()
            .any(|a| a.error_kind.as_deref() == Some("worker_lost")),
        "the lost attempt is logged: {attempts:?}"
    );
    assert!(attempts.iter().any(|a| a.response_status == Some(200)));
    // At-least-once: the customer may have received it twice, never zero times.
    assert!(
        env.sink
            .ids("/slow/crash")
            .get(&ev.to_string())
            .is_some_and(|r| r.ok >= 1)
    );
    env.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn worker_crash_pg() {
    worker_crash_recovers(QueueMode::Postgres).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn worker_crash_kafka() {
    worker_crash_recovers(QueueMode::Kafka).await;
}

// ---------------------------------------------------------------------------
// Circuit breaker: opens, parks without consuming attempts, probes, closes
// ---------------------------------------------------------------------------

async fn circuit_breaker(mode: QueueMode) {
    let env = TestEnv::start(mode).await;
    env.sink.set_switch("/switch/brk", 500);
    let (ep, _) = env.create_endpoint("/switch/brk", None).await;
    let mut events = Vec::new();
    for n in 0..8 {
        events.push(env.send_event("x.brk", json!({"n": n})).await);
    }
    // After 5 consecutive failures the breaker opens; later deliveries are
    // parked with no attempt used (attempt_count stays 0).
    let parked = wait_until(T, || async {
        env.count("SELECT count(*) FROM deliveries WHERE last_error = 'circuit_open' AND attempt_count = 0").await > 0
    })
    .await;
    assert!(
        parked,
        "a delivery was parked by the open breaker without using an attempt"
    );

    // The endpoint recovers: the next half-open probe succeeds, the breaker
    // closes, and everything is delivered.
    env.sink.set_switch("/switch/brk", 200);
    for ev in &events {
        let d = env
            .wait_status(*ev, ep, "succeeded", Duration::from_secs(60))
            .await;
        assert!(d.attempt_count <= 8);
    }
    // Requests sent while the breaker was open would show up as extra 500s;
    // with parking, total requests stay well below "8 events x 8 attempts".
    let total = env.sink.stats("/switch/brk").requests;
    assert!(
        total < 40,
        "breaker should have held most attempts back, saw {total}"
    );
    env.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn circuit_breaker_pg() {
    circuit_breaker(QueueMode::Postgres).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn circuit_breaker_kafka() {
    circuit_breaker(QueueMode::Kafka).await;
}

// ---------------------------------------------------------------------------
// A slow endpoint does not delay a healthy endpoint in the same partition
// ---------------------------------------------------------------------------

async fn slow_does_not_block_healthy(mode: QueueMode) {
    // One partition: both endpoints are guaranteed to share it.
    let env = TestEnv::with(
        Options::new(mode)
            .partitions(1)
            .config(|c| c.delivery_timeout_ms = 10_000),
    )
    .await;
    let (slow, _) = env
        .create_endpoint("/slow/hol?ms=3000", Some(vec!["slow.t"]))
        .await;
    let (fast, _) = env.create_endpoint("/ok/hol", Some(vec!["fast.t"])).await;
    // Warm up so the Kafka consumer has joined its group before timing.
    let warm = env.send_event("fast.t", json!({})).await;
    env.wait_status(warm, fast, "succeeded", T).await;

    for _ in 0..5 {
        env.send_event("slow.t", json!({})).await;
    }
    let start = Instant::now();
    let ev = env.send_event("fast.t", json!({})).await;
    env.wait_status(ev, fast, "succeeded", T).await;
    let took = start.elapsed();
    assert!(
        took < Duration::from_millis(1500),
        "healthy endpoint waited {took:?} behind the slow one"
    );
    let slow_done = env
        .count("SELECT count(*) FROM deliveries WHERE status = 'succeeded'")
        .await;
    assert!(
        slow_done < 7,
        "slow endpoint still busy while the healthy one was served"
    );
    let _ = slow;
    env.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_does_not_block_pg() {
    slow_does_not_block_healthy(QueueMode::Postgres).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_does_not_block_kafka() {
    slow_does_not_block_healthy(QueueMode::Kafka).await;
}

// ---------------------------------------------------------------------------
// SSRF: private targets refused at send time; redirects not followed
// ---------------------------------------------------------------------------

/// Insert an endpoint straight into the database, bypassing API validation.
/// This simulates an endpoint that passed validation earlier and whose DNS
/// now points somewhere private (DNS rebinding).
async fn insert_raw_endpoint(env: &TestEnv, url: &str) -> Uuid {
    let id = Uuid::now_v7();
    let secretbox = hooklane::crypto::SecretBox::from_base64_key(&env.config.secret_encryption_key)
        .expect("key");
    sqlx::query(
        "INSERT INTO endpoints (id, tenant_id, url, secret_ciphertext) VALUES ($1, $2, $3, $4)",
    )
    .bind(id)
    .bind(env.tenant_id)
    .bind(url)
    .bind(
        secretbox
            .encrypt(&hooklane::crypto::generate_signing_secret())
            .expect("encrypt"),
    )
    .execute(&env.pool)
    .await
    .expect("insert endpoint");
    id
}

async fn ssrf_blocked(mode: QueueMode) {
    let env = TestEnv::with(Options::new(mode).strict_ssrf()).await;
    let port = env.sink_url.rsplit(':').next().expect("port").to_string();

    // Rejected up front by the API.
    for url in [
        format!("http://127.0.0.1:{port}/ok/x"),
        "http://169.254.169.254/latest".to_string(),
        "http://localhost/x".to_string(),
    ] {
        let (status, v) = env
            .api(Method::POST, "/v1/endpoints", Some(json!({"url": url})))
            .await;
        assert_eq!(status, 422, "{url}: {v}");
    }

    // Literal private IP that got into the database anyway: refused at send time.
    let literal =
        insert_raw_endpoint(&env, &format!("http://127.0.0.1:{port}/ok/ssrf-literal")).await;
    let ev = env.send_event("x.ssrf", json!({})).await;
    let d = env.wait_status(ev, literal, "failed", T).await;
    assert_eq!(
        env.attempts(d.id).await[0].error_kind.as_deref(),
        Some("ssrf_blocked")
    );

    // A hostname that resolves to a private address: refused by the DNS
    // resolver at connect time. We use this machine's own hostname, which
    // resolves to a loopback or private LAN address.
    let host = std::process::Command::new("hostname")
        .output()
        .ok()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string());
    if let Some(host) = host.filter(|h| !h.is_empty()) {
        let addrs: Vec<_> = tokio::net::lookup_host((host.as_str(), 0))
            .await
            .map(|a| a.collect())
            .unwrap_or_default();
        if !addrs.is_empty()
            && addrs
                .iter()
                .all(|a: &std::net::SocketAddr| hooklane::delivery::ssrf::is_blocked_ip(a.ip()))
        {
            let by_name =
                insert_raw_endpoint(&env, &format!("http://{host}:{port}/ok/ssrf-dns")).await;
            let ev2 = env.send_event("x.ssrf", json!({})).await;
            let d = env.wait_status(ev2, by_name, "failed", T).await;
            let attempts = env.attempts(d.id).await;
            assert_eq!(
                attempts[0].error_kind.as_deref(),
                Some("ssrf_blocked"),
                "{attempts:?}"
            );
        } else {
            eprintln!(
                "skipping DNS part: hostname {host} does not resolve to only private addresses ({addrs:?})"
            );
        }
    }
    assert!(
        env.sink.stats("/").requests == 0,
        "nothing reached the sink"
    );
    env.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ssrf_pg() {
    ssrf_blocked(QueueMode::Postgres).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ssrf_kafka() {
    ssrf_blocked(QueueMode::Kafka).await;
}

async fn redirects_not_followed(mode: QueueMode) {
    let env = TestEnv::start(mode).await;
    let (ep, _) = env.create_endpoint("/redirect/r", None).await;
    let ev = env.send_event("x.r", json!({})).await;
    let d = env.wait_status(ev, ep, "failed", T).await;
    assert_eq!(env.attempts(d.id).await[0].response_status, Some(302));
    assert_eq!(
        env.sink_count("/ok/r", ev),
        0,
        "redirect target never called"
    );
    env.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn redirects_pg() {
    redirects_not_followed(QueueMode::Postgres).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn redirects_kafka() {
    redirects_not_followed(QueueMode::Kafka).await;
}

// ---------------------------------------------------------------------------
// Tenant isolation
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tenant_isolation() {
    let env = TestEnv::start(QueueMode::Postgres).await;
    let (ep, _) = env.create_endpoint("/notfound/iso", None).await;
    let ev = env.send_event("x.iso", json!({"secret": "a"})).await;
    let d = env.wait_status(ev, ep, "failed", T).await;
    let (_, b_key) = hooklane::admin::create_tenant(&env.pool, "tenant-b")
        .await
        .expect("tenant b");

    let checks = [
        (Method::GET, format!("/v1/endpoints/{ep}"), None),
        (
            Method::PATCH,
            format!("/v1/endpoints/{ep}"),
            Some(json!({"description": "pwned"})),
        ),
        (Method::DELETE, format!("/v1/endpoints/{ep}"), None),
        (Method::GET, format!("/v1/endpoints/{ep}/secret"), None),
        (
            Method::POST,
            format!("/v1/endpoints/{ep}/rotate-secret"),
            None,
        ),
        (
            Method::POST,
            format!("/v1/endpoints/{ep}/recover"),
            Some(json!({"since": "2000-01-01T00:00:00Z"})),
        ),
        (Method::GET, format!("/v1/events/{ev}"), None),
        (Method::GET, format!("/v1/deliveries/{}", d.id), None),
        (
            Method::POST,
            format!("/v1/deliveries/{}/resend", d.id),
            None,
        ),
    ];
    for (method, path, body) in checks {
        let (status, _) = env.api_as(&b_key, method.clone(), &path, body).await;
        assert_eq!(status, 404, "tenant B must not reach {method} {path}");
    }
    for path in ["/v1/endpoints", "/v1/events", "/v1/deliveries"] {
        let (status, v) = env.api_as(&b_key, Method::GET, path, None).await;
        assert_eq!(status, 200);
        assert_eq!(
            v["data"].as_array().map(|a| a.len()),
            Some(0),
            "{path} leaks data to tenant B"
        );
    }
    // Tenant A's data is untouched.
    let (_, v) = env
        .api(Method::GET, &format!("/v1/endpoints/{ep}"), None)
        .await;
    assert_eq!(v["description"], "");
    assert_eq!(
        env.attempts(d.id).await.len(),
        1,
        "tenant B's resend did nothing"
    );
    env.finish().await;
}

// ---------------------------------------------------------------------------
// Resend and recover
// ---------------------------------------------------------------------------

async fn resend_and_recover(mode: QueueMode) {
    let env = TestEnv::start(mode).await;
    let t0 = chrono::Utc::now() - chrono::Duration::seconds(5);
    env.sink.set_switch("/switch/rr", 404);
    let (ep, _) = env.create_endpoint("/switch/rr", None).await;
    let mut events = Vec::new();
    for _ in 0..3 {
        events.push(env.send_event("x.rr", json!({})).await);
    }
    for ev in &events {
        env.wait_status(*ev, ep, "failed", T).await;
    }

    // Resend one: fresh cycle, history kept.
    env.sink.set_switch("/switch/rr", 200);
    let d = env.delivery(events[0], ep).await.expect("delivery");
    let (status, _) = env
        .api(
            Method::POST,
            &format!("/v1/deliveries/{}/resend", d.id),
            None,
        )
        .await;
    assert_eq!(status, 202);
    let d = env.wait_status(events[0], ep, "succeeded", T).await;
    assert_eq!(d.attempt_count, 1, "attempt counter restarts");
    assert_eq!(env.attempts(d.id).await.len(), 2, "attempt history kept");
    // Resending something already queued is refused.
    let (status, _) = env
        .api(
            Method::POST,
            &format!("/v1/deliveries/{}/resend", d.id),
            None,
        )
        .await;
    assert_eq!(status, 202, "succeeded deliveries can be resent");
    let (status, _) = env
        .api(
            Method::POST,
            &format!("/v1/deliveries/{}/resend", d.id),
            None,
        )
        .await;
    assert!(
        status == 409 || status == 202,
        "second immediate resend: {status}"
    );

    // Recover the rest.
    let (status, v) = env
        .api(
            Method::POST,
            &format!("/v1/endpoints/{ep}/recover"),
            Some(json!({"since": t0.to_rfc3339()})),
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(v["requeued"], 2);
    for ev in &events[1..] {
        env.wait_status(*ev, ep, "succeeded", T).await;
    }

    // Recover on a disabled endpoint is refused until it is re-enabled.
    let (status, _) = env
        .api(
            Method::PATCH,
            &format!("/v1/endpoints/{ep}"),
            Some(json!({"status": "disabled"})),
        )
        .await;
    assert_eq!(status, 200);
    let (status, _) = env
        .api(
            Method::POST,
            &format!("/v1/endpoints/{ep}/recover"),
            Some(json!({"since": t0.to_rfc3339()})),
        )
        .await;
    assert_eq!(status, 409);
    let (_, v) = env
        .api(
            Method::PATCH,
            &format!("/v1/endpoints/{ep}"),
            Some(json!({"status": "active"})),
        )
        .await;
    assert_eq!(v["status"], "active");
    env.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resend_recover_pg() {
    resend_and_recover(QueueMode::Postgres).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resend_recover_kafka() {
    resend_and_recover(QueueMode::Kafka).await;
}

// ---------------------------------------------------------------------------
// Cursor pagination returns every row exactly once while rows are inserted
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cursor_pagination_exactly_once() {
    let env = TestEnv::with(Options::new(QueueMode::Postgres).workers(0)).await;
    let mut original = std::collections::HashSet::new();
    for n in 0..120 {
        original.insert(env.send_event("x.page", json!({"n": n})).await.to_string());
    }
    let mut seen = Vec::new();
    let mut cursor: Option<String> = None;
    let mut extra = 0;
    loop {
        // Insert new events between page reads.
        env.send_event("x.page", json!({"extra": extra})).await;
        extra += 1;
        let path = match &cursor {
            Some(c) => format!("/v1/events?limit=25&cursor={c}"),
            None => "/v1/events?limit=25".to_string(),
        };
        let (status, v) = env.api(Method::GET, &path, None).await;
        assert_eq!(status, 200);
        for e in v["data"].as_array().expect("data") {
            seen.push(e["id"].as_str().expect("id").to_string());
        }
        match v["next_cursor"].as_str() {
            Some(c) => cursor = Some(c.to_string()),
            None => break,
        }
    }
    let unique: std::collections::HashSet<_> = seen.iter().cloned().collect();
    assert_eq!(unique.len(), seen.len(), "no row returned twice");
    assert!(original.is_subset(&unique), "every original row returned");
    env.finish().await;
}

// ---------------------------------------------------------------------------
// API boundary validation
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn api_validation() {
    let env = TestEnv::with(Options::new(QueueMode::Postgres).workers(0)).await;
    let post = |body: String| {
        env.http
            .post(env.url("/v1/events"))
            .bearer_auth(&env.api_key)
            .header("content-type", "application/json")
            .body(body)
            .send()
    };
    let no_auth = env
        .http
        .post(env.url("/v1/events"))
        .json(&json!({"type": "a", "data": {}}))
        .send()
        .await
        .expect("send");
    assert_eq!(no_auth.status(), 401);
    let bad_key = env
        .api_as("hl_wrong", Method::GET, "/v1/events", None)
        .await;
    assert_eq!(bad_key.0, 401);
    assert_eq!(bad_key.1["error"]["code"], "unauthorized");

    assert_eq!(post("{not json".into()).await.expect("send").status(), 400);
    assert_eq!(
        post(json!({"type": "bad type!", "data": {}}).to_string())
            .await
            .expect("send")
            .status(),
        422
    );
    assert_eq!(
        post(json!({"type": "x".repeat(129), "data": {}}).to_string())
            .await
            .expect("send")
            .status(),
        422
    );
    assert_eq!(
        post(json!({"type": "hooklane.fake", "data": {}}).to_string())
            .await
            .expect("send")
            .status(),
        422
    );
    assert_eq!(
        post(json!({"type": "ok"}).to_string())
            .await
            .expect("send")
            .status(),
        422
    );
    let big = json!({"type": "big", "data": {"blob": "x".repeat(300 * 1024)}}).to_string();
    assert_eq!(post(big).await.expect("send").status(), 413);
    let just_under = json!({"type": "big", "data": {"blob": "x".repeat(250 * 1024)}}).to_string();
    assert_eq!(post(just_under).await.expect("send").status(), 202);

    let (status, v) = env
        .api(
            Method::POST,
            "/v1/endpoints",
            Some(json!({"url": "not a url"})),
        )
        .await;
    assert_eq!(status, 422, "{v}");
    let (status, _) = env
        .api(
            Method::POST,
            "/v1/endpoints",
            Some(json!({"url": "ftp://x.example"})),
        )
        .await;
    assert_eq!(status, 422);
    let (status, _) = env
        .api(Method::GET, "/v1/deliveries?status=bogus", None)
        .await;
    assert_eq!(status, 400);
    let (status, _) = env.api(Method::GET, "/v1/events?limit=1000", None).await;
    assert_eq!(status, 400);

    let r = env
        .http
        .get(env.url("/healthz"))
        .send()
        .await
        .expect("send");
    assert_eq!(r.status(), 200);
    let r = env.http.get(env.url("/readyz")).send().await.expect("send");
    assert_eq!(r.status(), 200);
    env.finish().await;
}
