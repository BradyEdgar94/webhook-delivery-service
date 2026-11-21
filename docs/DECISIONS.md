# Design decisions

Each decision: what was chosen, what was rejected, why, and one sentence you
can say out loud in an interview.

---

## 1. Language: Rust

**Choice.** Rust on the stable toolchain with the Tokio async runtime, one
Cargo workspace, one binary.

**Rejected.** Go, TypeScript (Node), Python.

**Why.** The compiler rejects whole classes of bugs before the code runs:
data races (a `Mutex` must be locked to reach the data inside), null
dereferences (`Option` must be handled), forgotten error paths (`Result`
must be handled), use-after-free. That is a strong safety net on code written
quickly, and especially on agent-written code. Rust is also proven for this
exact job: Svix, the best-known webhook service, is written in Rust. Go would
have been a reasonable second choice (simpler, also fast); TypeScript would be
the most familiar but gives weaker guarantees about concurrency and resource
use under load.

**How to explain it.** "The compiler catches the bugs that hurt a delivery
system most (races, unhandled errors, nulls) before anything runs, and Svix
proves the language fits the problem."

---

## 2. Queue: Postgres transactional outbox + Kafka

**Choice.** The API writes the event, its deliveries and one outbox row per
delivery in a single Postgres transaction. A relay copies outbox rows into a
Kafka topic (64 partitions, key = endpoint id). Workers consume the topic.
Postgres stays the source of truth.

**Rejected.** Postgres only (workers poll the table); Redis (lists/streams);
SQS; writing to Kafka directly from the API.

**Why.**
- Writing to Postgres *and* Kafka from the API is the **dual-write problem**:
  a crash between the two writes leaves an event saved but never delivered (or
  delivered but not saved). The outbox makes the "send" part of the same
  database transaction, so it is all-or-nothing.
- Kafka spreads the load across many workers and, because the key is the
  endpoint id, gives each endpoint to exactly one worker at a time. That makes
  in-memory rate limits and circuit breakers exact (decision 4) and keeps an
  endpoint's deliveries in order on the normal path.
- Postgres-only is kept as `QUEUE_MODE=postgres` and load-tested against Kafka
  mode (docs/RESULTS.md). It is simpler and fast at this scale; its weakness is
  that several workers can serve the same endpoint, so protections are only
  approximate, and every poll is a write on the hottest table.
- Redis and SQS would add a second source of truth (or lose ordering /
  per-key ownership) without solving the dual-write problem.

**How to explain it.** "The outbox makes 'save' and 'enqueue' one atomic
database write, and Kafka partitioned by endpoint gives me horizontal workers
with exactly one owner per endpoint."

---

## 3. Retries: 8 attempts over ~28 hours, schedule kept in Postgres

**Choice.** Attempt 1 immediately, then delays of 5 s, 5 m, 30 m, 2 h, 5 h,
10 h, 10 h (±20% jitter); after the 8th failure the delivery is `dead` (the
dead-letter state). The next attempt time is a `next_attempt_at` column; a
scheduler moves due rows back into the queue via the outbox.

**Rejected.** Kafka retry topics (`retry-5s`, `retry-5m`, ... consumed with a
delay).

**Why.** Kafka has no delayed delivery, so retry topics need a consumer that
sleeps per topic and can only approximate delays. A database column gives exact
delays, honours a receiver's `Retry-After` (any value from 1 s to 6 h), survives
crashes, and lets the API show "next attempt at 14:05". Jitter stops thousands
of deliveries that failed together from retrying in the same instant.

**How to explain it.** "Kafka can't delay a message, so the retry time is a
column in Postgres; that gives exact backoff, honours Retry-After and shows up
in the API."

---

## 4. Bad endpoints: timeout, token bucket, circuit breaker, auto-disable; state in worker memory

**Choice.** 15 s timeout; per-endpoint token bucket (default 50 req/s);
per-endpoint circuit breaker (opens after 5 consecutive failures or >50%
failures over ≥10 requests in a minute; half-open probe after 30 s doubling to
10 min); parking without using an attempt; auto-disable after ~5 days of
continuous failure. All of this state lives in the worker's memory.

**Rejected.** Shared state in Redis.

**Why.** One worker owns each partition, and so each endpoint, at a time.
That worker is the only process calling the endpoint, so its local view is the
whole truth: no network round-trip per delivery and no extra service. When a
partition moves (scaling, crash), the new owner starts with a fresh breaker:
the worst case is a few extra failed attempts, and nothing is lost because
delivery state is in Postgres.

**How to explain it.** "Because Kafka gives each endpoint to exactly one
worker, the rate limiter and breaker can be plain in-memory structs; there is
nothing to coordinate."

---

## 5. Ordering: best effort, with timestamp and sequence number

**Choice.** Normal path is in order (same partition, one lane per endpoint,
concurrency 1). A retried delivery arrives late and does not hold back later
events. Every request carries the event timestamp and a `webhook-sequence`
header (the delivery's id: increasing per endpoint, not gapless).

**Rejected.** Strict per-endpoint ordering; a gapless per-endpoint counter.

**Why.** Strict ordering means one failing event blocks every later event for
that endpoint for up to 28 hours. Stripe and Svix both choose best effort.
A gapless counter needs a row lock on the endpoint during every ingest, which
serialises all writes to a busy endpoint; the identity column is already
monotonic and free.

**How to explain it.** "Strict ordering would let one bad event block an
endpoint for a day, so like Stripe I deliver in order when things are healthy
and give receivers a timestamp and sequence number to reorder if they care."

---

## 6. Security: Standard Webhooks signing, in-process SSRF checks, hashed keys, encrypted secrets

**Choice.**
- Signing follows the Standard Webhooks spec exactly (HMAC-SHA256 over
  `id.timestamp.body`, `v1,<base64>`), verified in tests with the official
  `standardwebhooks` crate. Rotation keeps the old secret for 24 h and sends
  both signatures.
- SSRF: a custom DNS resolver inside the HTTP client drops private/reserved
  addresses *at connect time*; literal-IP URLs are checked explicitly; no
  redirects; no proxy; HTTPS only by default.
- API keys stored as SHA-256 hashes; endpoint secrets encrypted with
  AES-256-GCM using a key from the environment.

**Rejected.** A separate egress proxy (Smokescreen, as Convoy uses); a custom
signature scheme.

**Why.** Following the spec means customers can use off-the-shelf libraries
in any language. Checking the *resolved IP at connect time* is what defeats
DNS rebinding; checking the URL text at creation is not enough. The egress
proxy is a fine design but one more service to run; the in-process check gives
the same protection here. (Static outbound IPs for customer allowlists come
from a NAT gateway in the Terraform.)

**How to explain it.** "Signatures follow Standard Webhooks so customers can
verify with any library, and SSRF is blocked where it matters: on the actual IP
we are about to connect to."

---

## 7. Deployment: one binary, four roles; Compose and kind locally; EKS via Terraform only for load tests

**Choice.** `hooklane --role api|relay|scheduler|worker` from one image.
Docker Compose for everyday development (`make up`), kind + KEDA for local
Kubernetes autoscaling, Terraform describing EKS/RDS for a temporary cloud
load test (never applied by default).

**Rejected.** ECS Fargate; an always-on cluster; four separate images.

**Why.** One image keeps versions and builds in lockstep and makes the
"stateless roles, scale each independently" story obvious. Kubernetes is the
industry standard and KEDA can scale on Kafka lag directly. Creating the cloud
environment only for a load test and destroying it straight after keeps cost
near zero.

**How to explain it.** "Same image, different `--role`; each role scales on
its own signal: the API on CPU, workers on Kafka lag."

---

## 8. Observability: OpenTelemetry; Prometheus and Grafana locally; SLO 99% within 60 s

**Choice.** `tracing` + OpenTelemetry exported over OTLP to a Collector,
which feeds Prometheus (metrics), Jaeger (traces) and Grafana (dashboard).
SLO: 99% of events to healthy endpoints delivered within 60 seconds, measured
on first-attempt deliveries.

**Rejected.** Vendor-specific SDKs (Datadog, New Relic agents).

**Why.** One instrumentation, any backend: pointing the Collector at Datadog
is a config change, not a code change (docs/OBSERVABILITY.md). The SLO is
measured on first attempts because retries are slow *by design*; including
them would make the SLO measure customers' outages rather than ours.

**How to explain it.** "I instrument once with OpenTelemetry and route it
anywhere; the SLO measures our pipeline (first attempts), not our customers'
downtime."

---

## Choices made where the spec left a detail open

| # | Choice | Reason |
|---|---|---|
| 9 | **System events go only to endpoints that list the type explicitly.** An endpoint with no filter (all types) does not receive `hooklane.*` events. User events may not use the `hooklane.` prefix (422). | Nobody receives internal notifications by accident, and users cannot spoof them. |
| 10 | **Every disable path fails the endpoint's waiting deliveries** (`pending`/`retry_scheduled` → `failed`, `last_error = endpoint_disabled`): 410, auto-disable and `PATCH {"status":"disabled"}`. `POST /recover` brings them back. | Otherwise they would sit in the queue forever; one rule is easier to explain than three. |
| 11 | **Resend and recover reset `attempt_count` to 0** (a fresh 8-attempt cycle); attempt rows are kept, so attempt numbers can repeat in the log. The reaper decides whether a lost attempt was logged by `started_at >= claimed_at`, not by attempt number. | "Fresh retry cycle" per the spec, without losing history. |
| 12 | **Reaper on the last attempt → `dead`**, not another try. | Prevents a poison delivery that crashes workers from looping forever. |
| 13 | **429 with `Retry-After` pauses the endpoint** in the worker: later deliveries for it are parked until the pause ends, instead of blocking the lane for hours. The 429'd delivery itself is scheduled at `Retry-After` (clamped 1 s – 6 h). | Spec says "pause the lane"; parking keeps the partition flowing. |
| 14 | **4xx (other than 408/410/429) count as healthy for the breaker.** | The endpoint is up and answering; a 404 is a configuration problem, not an outage. |
| 15 | **DB errors during processing are retried in the lane** (backoff up to 30 s) and the Kafka offset is not marked done until processing succeeds. | Marking it done on error could strand a `pending` row with no message to deliver it. |
| 16 | **Pending-rescue safety net** (Kafka mode): a `pending` delivery untouched for 15 minutes with no unpublished outbox row is re-published. | Defence in depth against a lost Kafka message; duplicates are skipped by the claim. |
| 17 | **Postgres mode releases rows claimed long ago** (half the reaper threshold) instead of sending them, and caps rows queued per endpoint lane at 10. | In Postgres mode rows are claimed before they wait in a lane; this stops the reaper from re-queueing a row a worker still holds. |
| 18 | **Trace context is not carried in Postgres mode** (there is no outbox row to hold it). Traces there start at the worker. | Kept the seven-table schema exactly as specified. |
| 19 | **API keys: `hl_` + 32 random bytes (base64url); first 10 characters stored as the prefix.** | Recognisable in logs and secret scanners; prefix lets a user tell keys apart. |
| 20 | **Endpoint URL validation rejects** `localhost`, `*.localhost`, `*.internal`, credentials in the URL, and non-HTTP(S) schemes, in addition to blocked literal IPs. | Cheap early rejection; the connect-time resolver check remains the real protection. |
| 21 | **Endpoint changes reach workers within 30 s** (the endpoint cache TTL). | Avoids a DB read per delivery; 30 s staleness is acceptable for URL/rate edits. |
| 22 | **Postgres host port is 55432** in Compose. | Avoids colliding with a Postgres already installed on the developer's machine (this happened on the build machine). |
| 23 | **Non-API roles serve `GET /healthz` on `HEALTH_ADDR` (default `:9000`).** | Kubernetes liveness/readiness probes for every role. |
| 24 | **`synchronous_commit` stays on** even though load tests showed Postgres commit cost as the bottleneck. | A 202 must mean the event is durable (guarantee 1). |
