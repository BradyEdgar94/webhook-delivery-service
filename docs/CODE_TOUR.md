# Code tour

Follow one event from `POST /v1/events` to the customer's `200`, file by file.
Then follow a delivery that keeps failing, through every retry, to `dead`.
Paths are relative to `crates/hooklane/src/`. Kafka mode unless stated.

---

## Part 1: the happy path

### 1. Process start: `main.rs`
`hooklane --role api` parses flags and environment into a `Config`
(`config.rs`), sets up logging and OpenTelemetry (`telemetry.rs`) *before*
starting the Tokio runtime, connects to Postgres with retry
(`db::connect_with_retry`), and calls `api::run`. Every role starts the same way;
only the last call differs.

### 2. The request arrives: `api/mod.rs`
The axum router maps `POST /v1/events` to `api::events::ingest`. Around every
request: a `TraceLayer` opens a span (the root of the trace), a middleware
records latency (`hooklane.api.request.duration`), and a body limit rejects
anything over ~260 KB.

### 3. Who is calling? `api/auth.rs`
`ingest` takes a `Tenant` argument. Axum builds it by running
`Tenant::from_request_parts`: read `Authorization: Bearer hl_...`, SHA-256 it,
look the hash up in a 60-second in-memory cache, and fall back to
`db::tenants::tenant_for_key_hash`. No valid key, no `Tenant`, so the handler
never runs (401). Every query after this filters by that tenant id.

### 4. Validation: `api/events.rs`
The body is parsed by hand so each problem gets the right status: unreadable
JSON → 400, missing fields → 422, type not matching `^[a-zA-Z0-9_.-]{1,128}$`
or using the reserved `hooklane.` prefix → 422, payload over 256 KB → 413.
The optional `Idempotency-Key` header is read. The current trace context is
captured as a W3C `traceparent` string.

### 5. The one transaction: `db/events.rs::ingest`
This is the **transactional outbox**:
1. `INSERT INTO events ... ON CONFLICT (tenant_id, idempotency_key) DO NOTHING RETURNING id`.
   If nothing comes back, this key was used before: roll back and return the
   original id (the unique index, not application code, decides the race).
2. `fan_out`: one SQL statement inserts a `deliveries` row for every active
   endpoint of this tenant subscribed to the type, and (writable CTE) an
   `outbox` row for each new delivery, carrying the `traceparent`.
3. `COMMIT`. From here the event cannot be lost.

The handler answers `202 {"id": ...}`.

### 6. Into Kafka: `relay.rs`
The relay loops: `db::outbox::lock_unpublished` selects up to 500 unpublished
rows `FOR UPDATE SKIP LOCKED` (a second relay would skip these rows). For each
row it opens a `relay.publish` span whose parent is the stored `traceparent`,
and produces `{delivery_id, endpoint_id, tenant_id, event_id}` to
`hooklane.deliveries` with **key = endpoint id** and a `traceparent` header.
The producer (`queue/kafka.rs`) uses `acks=all` and idempotence. Only after
*every* send is acknowledged does it mark the rows published and commit. If
anything fails it rolls back and the whole batch is retried; duplicates are
fine, losses are impossible.

### 7. Kafka hands it to a worker: `worker/kafka_loop.rs`
Each worker is a member of consumer group `hooklane-workers`; Kafka gave it
some of the 64 partitions (`post_rebalance` records which). For each message:
- `OffsetTracker::start(offset)` (`worker/offsets.rs`) marks it in flight;
- if the partition has 256 unfinished messages, fetching it is **paused**;
- if this endpoint's lane already holds 100 messages, the delivery is
  **parked** in Postgres instead (`park_overflow`);
- otherwise it is dispatched to the endpoint's lane.

### 8. The endpoint's lane: `worker/lanes.rs`
One Tokio task per endpoint processes that endpoint's jobs one at a time
(`PER_ENDPOINT_CONCURRENCY=1`), so a slow endpoint only slows itself. The lane
calls `process::process` and, if it returns an error such as "database down",
retries with backoff, without marking the offset done.

### 9. One delivery: `worker/process.rs`
1. `EndpointCache::get` (`worker/endpoints.rs`): the endpoint row with the
   decrypted signing secret (`crypto.rs`, AES-256-GCM), cached for 30 s.
2. `Guards::check`: is the endpoint paused after a 429? Is its circuit breaker
   (`delivery/breaker.rs`) open? If so, park and stop here.
3. `Guards::take_token`: the token bucket (`delivery/ratelimit.rs`); sleep if empty.
4. **The claim**, `db::deliveries::claim`:
   `UPDATE deliveries SET status='delivering', attempt_count = attempt_count+1 ... WHERE id=$1 AND status='pending'`.
   No row back means another process already handled it: skip. This is what
   turns duplicate Kafka messages into at most one HTTP request.
5. `delivery/http.rs::send`: re-checks the URL (`delivery/ssrf.rs::check_url`,
   catches literal IPs), builds the body
   `{"type", "timestamp", "data"}`, signs `id.timestamp.body`
   (`delivery/signing.rs`), and POSTs with headers `webhook-id`,
   `webhook-timestamp`, `webhook-signature`, `webhook-sequence`,
   `webhook-attempt`. The client follows no redirects, uses no proxy, resolves
   DNS through `SafeResolver` (drops private IPs), and times out after 15 s.
6. `delivery/retry.rs::classify` turns `200` into `Verdict::Succeeded`.
7. `db::deliveries::record_attempt`: in one transaction, the conditional
   `UPDATE ... SET status='succeeded' WHERE id=$1 AND status='delivering'` and
   the `attempts` row (status, duration, first 1 KB of the body).
8. Metrics: attempt duration, `first_attempt=true` success, and end-to-end
   latency (event `created_at` → now) into `hooklane.delivery.latency`.

### 10. Commit: back in `worker/kafka_loop.rs`
The lane's completion callback sends `Done{partition, offset}`.
`OffsetTracker::finish` removes it from the in-flight set, and every second
`commit` writes the highest offset below which everything is finished.

The customer has their `200`. In Jaeger this is one trace:
`api.ingest → relay.publish → worker.deliver → http.post`.

---

## Part 2: a delivery that keeps failing

Same path as above until step 9.6, but the endpoint answers `503`.

1. **Attempt 1 fails.** `classify` → `Verdict::Retry`. `next_delay(schedule, 1)`
   is 5 s; `jitter` makes it 4–6 s. `record_attempt` writes the attempt row and
   moves the delivery to `retry_scheduled` with `next_attempt_at = now + ~5s`.
   The breaker records a failure. `endpoints.failing_since` is set (only
   written because it changed).
2. **Waiting.** Nothing holds the delivery in memory. It is a row in Postgres.
3. **Due.** The scheduler (`scheduler.rs`, every second) runs
   `db::deliveries::requeue_due_retries`: `SELECT ... FOR UPDATE SKIP LOCKED`
   on due rows, set them `pending`, insert a fresh outbox row, commit. The relay
   publishes it; a worker picks it up again (step 7 of Part 1).
4. **Attempts 2–5** repeat with longer delays (5 m, 30 m, 2 h, 5 h).
   `webhook-id` is the same every time, so the receiver can dedupe.
5. **The breaker opens.** After 5 consecutive failures, `Guards::check`
   returns `Park`: other deliveries to this endpoint are set to
   `retry_scheduled` at the next probe time **without** incrementing
   `attempt_count`. After the cooldown, exactly one delivery goes through as a
   half-open probe; another failure doubles the cooldown (max 10 min).
6. **Attempt 8 fails.** `next_delay(schedule, 8)` is `None` → `AttemptResult::Dead`.
   `record_attempt`, in the same transaction, emits a `hooklane.delivery.dead`
   system event (`db::events::emit_system_event`) to endpoints that subscribed
   to it. If the failing delivery was *itself* a system event, nothing is
   emitted (no loops).
7. **Five days of failure.** If the endpoint has been failing continuously for
   5 days, the scheduler's auto-disable loop calls `db::endpoints::disable_in_tx`:
   the endpoint becomes `disabled`, its waiting deliveries become `failed`
   (`endpoint_disabled`), and `hooklane.endpoint.disabled` is emitted.
8. **Recovery.** The customer fixes their server, re-enables the endpoint
   (`PATCH {"status":"active"}`), and calls `POST /v1/endpoints/{id}/recover`
   with a `since` time: `db::deliveries::recover` puts every `failed`/`dead`
   delivery since then back to `pending` with a fresh attempt counter.

### What if a worker dies mid-request?
The delivery stays `delivering`. The scheduler's reaper
(`db::deliveries::reap_stuck`, every 15 s) finds rows stuck for over 2 minutes,
adds a `worker_lost` attempt row, and re-queues them. The Kafka message was not
committed either, so the new owner of the partition also receives it, but its
claim fails (`delivering`, not `pending`) and it skips. Exactly one new attempt
happens; the customer may have received the first one, which is the
at-least-once trade-off.
