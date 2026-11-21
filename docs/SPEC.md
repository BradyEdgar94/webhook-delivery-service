# Hooklane: build specification

> This is the specification the project was built from. It is reproduced as written, apart from two sentences about the author that were reworded. Sections 21 (phases and gates) and 23 (the requirements checklist) are what the build was checked against; `docs/PROGRESS.md` records what each gate actually showed.

Hooklane is the working name for a self-hosted **outbound webhook delivery service** written in Rust. Rename it freely; nothing depends on the name.

This file is the complete specification. Build all of it, in the phase order in section 21, verifying each phase before starting the next.

---

## 1. Context: what this is for

An application (the "sender") posts events to Hooklane. Hooklane saves each event, then delivers it over HTTPS to the webhook endpoints the sender's customers have registered, with signing, retries, a delivery log, and protection against slow or failing endpoints. No event may ever be lost.

Three facts shape how you should build it:

1. **It is a portfolio piece that demonstrates large-scale infrastructure work.** The value is in the architecture being real and the results being measured: a transactional outbox, Kafka with partitioned consumers, horizontal workers that autoscale on lag, and load and chaos tests with honest numbers. A shortcut that makes a demo pass but breaks a guarantee defeats the purpose.
2. **Every design decision must be explainable.** Code must be readable by a Rust beginner, and the "why" behind each mechanism must be written down next to it (section 3 and section 20).
3. **It must run locally with one command.** The owner develops on macOS with Docker Desktop. `make up` must bring up the whole system using only Docker. The cloud is optional and you must never deploy to it.

The first real user will be Formsuite, a Next.js/TypeScript form builder that already sends form-submission webhooks. That integration happens in Formsuite's own repository, so here you only provide an integration guide and a TypeScript example (section 20).

---

## 2. How to work

- **Work through the phases in section 21 in order.** Each phase ends with a gate: a list of checks you must run and see pass. Do not start a phase until the previous gate passes. This is what makes a build this size succeed in one session.
- **Verify by running things, not by reading code.** Start the stack, send real requests, read real output. If something cannot be run in this environment (for example `kind` is not installed), say so in the progress file and the final report. Do not claim it works.
- **Keep `docs/PROGRESS.md` up to date.** After each phase, record what was built, what the gate checks showed, and anything deferred. Copy the checklist from section 23 into it and tick items as they are verified. If you run low on context, this file is how a fresh session resumes, so keep it accurate.
- **Commit to local git after every phase** with a clear message. Do not add a remote and do not push.
- **Never invent results.** Every number in `docs/results/` must come from a test you actually ran, with the machine and settings recorded beside it.
- **Never weaken a test or a guarantee to get a gate to pass.** If a requirement turns out to be wrong or impossible, stop, explain the problem in `docs/PROGRESS.md`, and choose the closest design that keeps the guarantees in section 4.
- **Do not stop to ask questions** unless you are truly blocked. Where this spec leaves a detail open, choose the simplest option consistent with the guarantees, and note the choice in `docs/DECISIONS.md`.
- **Use current stable versions of every dependency and check their documentation** when an API does not match what you expect. Rust's OpenTelemetry crates and `rdkafka` in particular change between versions. Do not pin versions from memory.
- **Never run `terraform apply`, never create cloud resources, never use real credentials.**

---

## 3. Code style: written for a Rust beginner who must explain it

- Prefer plain, explicit code over clever code. Avoid custom macros, deep generics, and trait gymnastics. A little repetition is better than an abstraction the owner cannot read.
- Small modules with one job each. File names should match the concepts in this spec (`outbox`, `relay`, `breaker`, `ssrf`, `signing`, `retry`).
- Every module starts with a doc comment that says **what it does and why it exists**, in plain English, naming the standard pattern where there is one (for example "This is the transactional outbox pattern. It exists to avoid the dual-write problem: ...").
- At each place where a guarantee is enforced (a transaction boundary, a `FOR UPDATE SKIP LOCKED`, an offset commit, a conditional `UPDATE`), add a short comment explaining what would go wrong without it.
- Errors: `thiserror` for typed errors in the library, `anyhow` at the edges. No `unwrap()` or `expect()` outside tests and startup configuration.
- `cargo fmt` and `cargo clippy --all-targets -- -D warnings` must pass.

---

## 4. Guarantees (the things that must always hold)

1. **No lost events.** Once the API returns `202`, the event is in Postgres and will be delivered or end in a visible terminal state (`failed` or `dead`).
2. **At-least-once delivery, with duplicates kept rare.** A crash anywhere may cause a repeat send, never a missed one. Receivers can dedupe on the stable `webhook-id`.
3. **No duplicate deliveries from internal duplication.** A duplicate Kafka message, a retried relay batch, or two workers seeing the same delivery must result in at most one HTTP request per attempt. This is enforced by a conditional claim in Postgres (section 10).
4. **One bad endpoint cannot stall the others** (section 13).
5. **Tenant isolation.** Every query is scoped by tenant. One tenant can never read or affect another's data.
6. **The service cannot be used to reach private networks** (section 15).

---

## 5. Technology

| Area | Choice | Notes |
| --- | --- | --- |
| Language | Rust, stable toolchain, Tokio async runtime | One Cargo workspace |
| HTTP server | `axum` | |
| Database | PostgreSQL 16+ via `sqlx` | Use `sqlx::query`/`query_as` (runtime-checked), or commit the `.sqlx` offline cache, so the Docker build never needs a live database |
| Broker | Kafka protocol via `rdkafka` | Redpanda (`redpandadata/redpanda`) is the broker locally and in Kubernetes. Enable `rdkafka`'s `cmake-build` feature and install `cmake` and a C/C++ toolchain in the Dockerfile |
| Outbound HTTP | `reqwest` with rustls | Redirects disabled, custom DNS resolver (section 15) |
| Crypto | `hmac`, `sha2`, `base64`, `aes-gcm`, `rand` | |
| CLI and config | `clap` (derive, env) | Every setting is an environment variable; commit `.env.example` |
| Telemetry | `tracing` + OpenTelemetry (OTLP) | Section 16 |
| Load testing | k6 (`grafana/k6` Docker image) | Section 19 |
| Local orchestration | Docker Compose; `kind` for local Kubernetes | |
| Autoscaling | KEDA (workers, on Kafka lag); HPA (API, on CPU) | |
| License | MIT | |

---

## 6. Architecture

```
Sender (e.g. Formsuite)
   │  POST /v1/events
   ▼
  API ──(one transaction: event + deliveries + outbox rows)──► Postgres  ◄── source of truth
                                                                 │   ▲
                                              unpublished outbox │   │ attempt log, retry schedule
                                                                 ▼   │
                                                               Relay │        Scheduler
                                                                 │   │   (due retries → outbox,
                                                     keyed by    ▼   │    stuck-delivery reaper,
                                                     endpoint  Kafka │    auto-disable, cleanup)
                                                  (64 partitions)│   │
                                                                 ▼   │
                                                               Workers ──signed POST──► Customer endpoints
```

- **Postgres is the source of truth.** Kafka is only the distribution layer between Postgres and the workers.
- **Transactional outbox.** The API writes the event, its deliveries, and one outbox row per delivery in a single transaction. This avoids the dual-write problem: a crash can never leave an event saved but not queued.
- **Only the relay writes to Kafka.** New deliveries and retries both enter Kafka through the outbox table. The scheduler never publishes to Kafka itself.
- **Kafka topic `hooklane.deliveries`, 64 partitions, message key = endpoint id.** All of one endpoint's deliveries land in one partition, which gives in-order consumption on the normal path and means one worker owns an endpoint at a time.
- **Partition count is the ceiling on worker parallelism** (one consumer per partition in a consumer group). 64 is chosen up front because adding partitions later reshuffles which endpoint maps to which partition.

### One binary, four roles

Build one binary, `hooklane`, that runs one role per process:

```
hooklane --role api | relay | scheduler | worker
hooklane migrate                 # run migrations; create the Kafka topic if missing
hooklane admin create-tenant --name <name>     # prints tenant id and an API key, shown once
```

| Role | Job | Scaling |
| --- | --- | --- |
| api | Authenticate, validate, save event + deliveries + outbox rows in one transaction, return `202`; serve endpoint management and the delivery log | Stateless; many replicas behind a load balancer; HPA on CPU |
| relay | Publish unpublished outbox rows to Kafka | 1–2 replicas; `SKIP LOCKED` makes several safe |
| scheduler | Re-queue due retries, reap stuck deliveries, auto-disable dead endpoints, housekeeping, backlog gauges | 1–2 replicas; `SKIP LOCKED` makes several safe |
| worker | Consume Kafka, protect endpoints, sign, POST, record the attempt | 1 to 64 replicas; KEDA on consumer lag |

All roles shut down gracefully on SIGTERM: stop taking new work, finish in-flight work, commit offsets, exit. Kubernetes autoscaling depends on this.

### Queue mode switch

Support two modes behind a small queue abstraction, selected by `QUEUE_MODE`:

- `kafka` (default): as described above.
- `postgres`: no outbox rows, no relay. Workers claim `pending` deliveries directly from the `deliveries` table in batches using `FOR UPDATE SKIP LOCKED`, and the scheduler flips due retries straight back to `pending`.

Postgres mode is Phase 1's implementation and stays in the codebase so the two can be load-tested against each other (section 19). Document its known limit: without partition ownership, two workers can serve the same endpoint, so in-memory rate limits and circuit breakers are only approximate. That limit is one of the reasons for Kafka.

---

## 7. Repository layout

```
Cargo.toml                      workspace
crates/hooklane/                library + the hooklane binary
  src/main.rs, config.rs, telemetry.rs, crypto.rs
  src/api/                      routes, auth, pagination
  src/db/                       queries, one file per table group
  src/queue/                    mod.rs (abstraction), kafka.rs, postgres.rs
  src/relay.rs
  src/scheduler.rs
  src/worker/                   consume loop, per-endpoint lanes, offset tracking
  src/delivery/                 signing.rs, ssrf.rs, http.rs, retry.rs, breaker.rs, ratelimit.rs
crates/sink/                    fake customer endpoints for tests and load runs
crates/loadcheck/               verifies a run: lost, duplicates, latency percentiles
migrations/                     sqlx migrations
deploy/compose/                 config for Postgres, Redpanda, OTel collector, Prometheus, Grafana
deploy/k8s/                     kustomize base + kind overlay
deploy/terraform/               optional AWS (never applied by you)
loadtest/                       k6 scripts, chaos scripts
examples/typescript/            send an event; verify a signature
docs/                           see section 20
.vscode/                        tasks.json, launch.json, extensions.json, settings.json
.github/workflows/ci.yml        fmt, clippy, unit and integration tests
Dockerfile, docker-compose.yml, Makefile, .env.example, README.md, LICENSE
```

---

## 8. Data model

Seven tables. The unique constraints are what make retries and duplicate sends safe, so do not drop them.

**tenants**: `id uuid pk`, `name`, `created_at`.

**api_keys**: `id uuid pk`, `tenant_id`, `key_hash bytea unique` (SHA-256 of the key; keys are high-entropy random strings so a fast hash is appropriate), `prefix text` (first characters, for display), `created_at`, `revoked_at`.

**endpoints**: `id uuid pk`, `tenant_id`, `url`, `description`, `event_types text[]` (null or empty = all types), `secret_ciphertext bytea`, `previous_secret_ciphertext bytea null`, `previous_secret_expires_at timestamptz null`, `status` (`active` | `disabled`), `disabled_reason text null`, `rate_limit_per_sec int default 50`, `failing_since timestamptz null`, `created_at`, `updated_at`. Index on `tenant_id`.

**events**: `id uuid pk` (UUIDv7 so ids sort by time), `tenant_id`, `event_type`, `payload jsonb`, `idempotency_key text null`, `created_at`. Unique on `(tenant_id, idempotency_key)` where the key is not null. Index on `(tenant_id, id)`.

**deliveries**: one row per event × endpoint. `id bigint identity pk`, `event_id`, `endpoint_id`, `tenant_id`, `status`, `attempt_count int default 0`, `next_attempt_at timestamptz null`, `claimed_at timestamptz null`, `last_error text null`, `completed_at timestamptz null`, `created_at`, `updated_at`.
- Unique on `(event_id, endpoint_id)`.
- Partial index on `next_attempt_at` where `status = 'retry_scheduled'`.
- Partial index on `claimed_at` where `status = 'delivering'`.
- Partial index on `id` where `status = 'pending'` (Postgres queue mode).
- Indexes on `(endpoint_id, id)` and `(tenant_id, id)` for the delivery log.

**attempts**: every try. `id bigint identity pk`, `delivery_id`, `attempt_number`, `started_at`, `duration_ms`, `response_status int null`, `response_body_snippet text null` (first 1 KB only), `error_kind text null`, `error text null`. Index on `delivery_id`.

**outbox**: `id bigint identity pk`, `delivery_id`, `endpoint_id`, `tenant_id`, `traceparent text null`, `created_at`, `published_at timestamptz null`. Partial index on `id` where `published_at is null`.

### Delivery status machine

```
pending ──claim──► delivering ──2xx──────────────► succeeded
   ▲                   │
   │                   ├─ retryable failure, attempts left ──► retry_scheduled ──(scheduler, when due)──► pending
   │                   ├─ retryable failure, attempts used up ─► dead
   │                   └─ non-retryable failure ──────────────► failed
   └── parked (breaker open, rate-limit overflow): pending ──► retry_scheduled, no attempt consumed
```

Every status change is a **conditional update** (`... WHERE id = $1 AND status = '<expected>'`), and code must check the affected row count. This is how duplicates and races are made harmless.

---

## 9. Role: API

All routes are under `/v1`, authenticated with `Authorization: Bearer <api key>`, JSON in and out, errors as `{"error": {"code": "...", "message": "..."}}`. Cache successful API-key lookups in memory for about 60 seconds so ingest does not hit the database for auth on every request.

**Ingest**

- `POST /v1/events` with `{"type": "form.submitted", "data": {...}}` and an optional `Idempotency-Key` header. Returns `202` with the event id.
- In **one transaction**: insert the event; select the tenant's `active` endpoints subscribed to this type; insert one delivery per endpoint; in Kafka mode insert one outbox row per delivery; commit.
- Same idempotency key again for the same tenant: return the original event's id and create nothing. Use the unique constraint (`ON CONFLICT`), not check-then-insert, because check-then-insert races under concurrency.
- Limits: payload up to 256 KB; event type matches `^[a-zA-Z0-9_.-]{1,128}$`. Reject with `400`/`413`/`422` at the boundary.
- An event with no matching endpoints is still saved, with zero deliveries.

**Endpoints**

- `POST /v1/endpoints` (url, description, event_types, rate_limit_per_sec) → creates the endpoint and returns it with its signing secret.
- `GET /v1/endpoints`, `GET /v1/endpoints/{id}`, `PATCH /v1/endpoints/{id}` (including re-enabling a disabled endpoint), `DELETE /v1/endpoints/{id}`.
- `GET /v1/endpoints/{id}/secret`, `POST /v1/endpoints/{id}/rotate-secret` (section 15).
- `POST /v1/endpoints/{id}/recover` with `{"since": "<timestamp>"}`: re-queues that endpoint's `failed` and `dead` deliveries created since the timestamp.
- URLs are validated on create and update (section 15).

**Delivery log**

- `GET /v1/events`, `GET /v1/events/{id}` (with its deliveries).
- `GET /v1/deliveries?endpoint_id=&event_id=&status=`, `GET /v1/deliveries/{id}` (with all attempts and, when scheduled, `next_attempt_at`).
- `POST /v1/deliveries/{id}/resend`: starts a fresh retry cycle on that delivery; attempt history is kept.
- All list routes use **cursor (keyset) pagination** (`?cursor=&limit=`, response includes `next_cursor`), never `OFFSET`, because OFFSET gets slower with depth and skips or repeats rows when data changes.

**Operational**: `GET /healthz` (process up), `GET /readyz` (database reachable). When Postgres is down the API returns `503`, it does not crash.

---

## 10. Role: worker

### Consuming Kafka

- Consumer group `hooklane-workers`, topic `hooklane.deliveries`, `enable.auto.commit=false`, cooperative-sticky assignment (less disruption when autoscaling changes the worker count).
- Each message carries `{delivery_id, endpoint_id, tenant_id, event_id}` plus a `traceparent` header.

### Per-endpoint lanes (avoiding head-of-line blocking)

Many endpoints share a partition. If the worker processed a partition strictly one message at a time, one slow endpoint would delay every endpoint behind it. So:

- For each assigned partition, dispatch each message to an in-memory **lane per endpoint**. Different endpoints are processed in parallel; each endpoint's own deliveries are processed in order (concurrency per endpoint is `PER_ENDPOINT_CONCURRENCY`, default `1`, which preserves order on the normal path; higher values trade ordering for throughput to one endpoint. Document that tradeoff).
- **Offset tracking:** messages complete out of order, so track in-flight offsets per partition and commit only the highest offset below which everything is finished. Committing past an unfinished message could lose it on a crash.
- **Backpressure:** cap in-flight messages per partition (default 256). When the cap is reached, pause fetching for that partition.
- **Lane overflow:** if one endpoint's lane grows past a limit (default 100), park the excess deliveries (set `retry_scheduled` a few seconds ahead, no attempt consumed) so a slow endpoint cannot fill the partition's in-flight cap and block its neighbours.
- **On partition revoke** (rebalance), stop that partition's lanes and discard its in-memory state. Unfinished work will be redelivered to the new owner and the claim step makes that safe.

### Processing one delivery

1. Load the endpoint (cache per worker, ~30 s TTL). If it is `disabled`, mark the delivery `failed` with reason `endpoint_disabled`.
2. **Circuit breaker** (section 13). If open, park the delivery until the breaker's next probe time.
3. **Rate limit** (section 13). Wait for a token.
4. **Claim**, immediately before sending: `UPDATE deliveries SET status='delivering', claimed_at=now(), attempt_count=attempt_count+1 WHERE id=$1 AND status='pending'`. If no row was updated, another process already handled it (a duplicate Kafka message, or a reaped delivery): skip it. **This conditional claim is what turns at-least-once Kafka delivery into at most one HTTP request per attempt.**
5. Build the request (section 15), POST with a **15-second total timeout**.
6. Classify the outcome (section 12). In **one transaction**, insert the attempt row and update the delivery to `succeeded`, `retry_scheduled` (with `next_attempt_at`), `failed`, or `dead`.
7. Maintain `endpoints.failing_since`: set it on a failure when it is null, clear it on a success. Only write when the value changes, so a busy endpoint's row is not updated on every delivery.
8. Mark the Kafka offset as done.

A worker crash between steps 4 and 6 leaves the delivery in `delivering`. The scheduler's reaper recovers it (section 11). This is the unavoidable at-least-once window: the request may have reached the customer, and it will be sent again.

---

## 11. Roles: relay and scheduler

### Relay (Kafka mode only)

Loop:
1. In a transaction, select a batch (default 500) of outbox rows `WHERE published_at IS NULL ORDER BY id FOR UPDATE SKIP LOCKED`.
2. Produce each to Kafka, key = endpoint id, with `acks=all` and `enable.idempotence=true`. Wait for every acknowledgement.
3. Mark the rows published and commit.
4. If the batch was empty, sleep briefly (50–100 ms).

If the relay crashes after Kafka accepted a message but before the row was marked, the row is published again. That duplicate is harmless because of the worker's claim step. Note in the docs that more than one relay can slightly reorder an endpoint's messages, which is acceptable under best-effort ordering (section 14).

### Scheduler

Independent loops, each claiming batches with `FOR UPDATE SKIP LOCKED` so multiple scheduler instances are safe:

- **Due retries** (every second): deliveries with `status='retry_scheduled' AND next_attempt_at <= now()` become `pending` and, in Kafka mode, get a new outbox row in the same transaction. Skip deliveries whose endpoint is disabled.
- **Reaper** (every 15 s): deliveries stuck in `delivering` for more than 2 minutes are set back to `pending` (plus an outbox row), with an attempt row of kind `worker_lost` if that attempt has none.
- **Auto-disable** (every minute): endpoints whose `failing_since` is older than 5 days become `disabled` with a reason; their undelivered deliveries become `failed` (`endpoint_disabled`); a system event is emitted (section 12). `POST /recover` brings those deliveries back after the customer fixes the endpoint.
- **Outbox cleanup**: delete published outbox rows older than 1 hour.
- **Gauges** (every 15 s): oldest unpublished outbox row age, retry backlog size and oldest overdue, dead-delivery count.

---

## 12. Retries

**Schedule: 8 attempts over about 28 hours.** Attempt 1 is immediate. Delays before attempts 2–8: `5s, 5m, 30m, 2h, 5h, 10h, 10h`, each with **±20% random jitter**. Jitter stops many deliveries that failed together from retrying at the same instant. Make the schedule configurable (tests use a schedule of milliseconds).

**Why retry times live in Postgres and not Kafka:** Kafka has no delayed delivery. A `next_attempt_at` column gives exact delays (including `Retry-After`), survives crashes, and lets the API show when the next attempt is due. The rejected alternative was Kafka retry topics.

**Classification**

| Outcome | Action |
| --- | --- |
| 2xx | `succeeded` |
| Timeout, connection error, TLS error | Retry |
| 5xx | Retry |
| 408 | Retry |
| 429 | Retry; honor `Retry-After` (seconds or HTTP date), clamped to 1 second–6 hours; pause that endpoint's lane until then |
| 410 Gone | `failed`, and **disable the endpoint** |
| Any other 4xx | `failed` (it will not fix itself) |
| 3xx | Not followed; `failed` |
| Blocked by SSRF rules | `failed` |

After the 8th failed attempt the delivery becomes **`dead`**. This is the dead-letter state.

**Notifying the sender.** When a delivery goes `dead` or an endpoint is disabled, emit a system event for that tenant (`hooklane.delivery.dead`, `hooklane.endpoint.disabled`) through the normal pipeline, delivered to endpoints subscribed to those types. A system event whose own delivery fails must never generate another system event.

---

## 13. Protecting against bad endpoints

Four layers, so one customer's broken server cannot delay everyone else (the "noisy neighbour" problem):

1. **Timeout:** 15 seconds per attempt.
2. **Per-endpoint rate limit:** a token bucket, default 50 requests/second, configurable per endpoint (`rate_limit_per_sec`). Implement the token bucket by hand in a small, well-commented module.
3. **Circuit breaker**, per endpoint, three states:
   - **Closed:** deliveries flow.
   - **Open:** entered after 5 consecutive failures, or more than 50% failures over the last minute with at least 10 requests. While open, the endpoint is not called; its deliveries are parked as `retry_scheduled` at the breaker's next probe time plus jitter, **without consuming an attempt**.
   - **Half-open:** after a cooldown (30 s, doubling on each failed probe up to 10 minutes), let exactly one delivery through as a probe. Success closes the breaker; failure reopens it.
   - Breaker failures are timeouts, connection errors and 5xx. A 429 is rate-limit feedback, not a breaker failure.
4. **Auto-disable** after about 5 days of continuous failure (section 11).

**State lives in the worker's memory.** In Kafka mode one worker owns each partition, and so each endpoint, at a time, so rate-limit and breaker state need no shared store. If the partition moves to another worker the state resets; the worst case is a few extra failed attempts, and nothing is lost because deliveries are in Postgres. Write this reasoning in the module's doc comment.

---

## 14. Ordering

**Best-effort ordering**, the same choice Stripe and Svix make.

- On the normal path an endpoint receives events in order (same partition, one lane, concurrency 1).
- A retried delivery arrives late; later events are not held back for it. Strict ordering was rejected because one failing event would block an endpoint for hours.
- Every request carries the event's creation timestamp and a **`webhook-sequence`** header so receivers can detect and handle late arrivals. The sequence is the delivery's `id`: monotonically increasing per endpoint, not gapless. A gapless per-endpoint counter was rejected because it needs a row lock on the endpoint during every ingest, which serializes writes to busy endpoints. Record this in `docs/DECISIONS.md`.

---

## 15. Security

### Signing: implement the Standard Webhooks specification exactly

- Each endpoint has its own secret: 32 random bytes, presented as `whsec_<base64>`.
- Headers on every request: `webhook-id` (the event id, **identical across retries** so receivers can dedupe), `webhook-timestamp` (Unix seconds at send time), `webhook-signature`.
- Signed content is `{webhook-id}.{webhook-timestamp}.{body}`. Signature is HMAC-SHA256 using the base64-decoded secret bytes as the key, sent as `v1,<base64 signature>`.
- Also send `webhook-sequence`, `webhook-attempt`, `content-type: application/json`, and a `user-agent` naming the service.
- Body: `{"type": "<event type>", "timestamp": "<event created, RFC 3339>", "data": <payload>}`. Sign the exact bytes that are sent.
- **Rotation:** `rotate-secret` creates a new secret and keeps the previous one valid for 24 hours. During that window send both signatures, space-separated, so customers can switch with no downtime.
- **Cross-check in a test:** verify Hooklane's signatures with the official `standardwebhooks` crate (a dev-dependency), and verify that a tampered body or a wrong secret fails.

### SSRF protection

Customers choose the URL, so an attacker could point an endpoint at an internal address (cloud metadata at `169.254.169.254`, `localhost`, a private service) and have a worker call it from inside the network.

- Do the check **in-process**, at connection time, on the **resolved IP address**, not on the URL text. A harmless-looking domain can resolve to a private IP (DNS rebinding), so checking only at endpoint creation is not enough. Implement this as a custom DNS resolver for the `reqwest` client that drops disallowed addresses.
- **URLs with a literal IP address do not go through the DNS resolver**, so check literal hosts explicitly before sending.
- Blocked: IPv4 `0.0.0.0/8`, `10.0.0.0/8`, `100.64.0.0/10`, `127.0.0.0/8`, `169.254.0.0/16`, `172.16.0.0/12`, `192.0.0.0/24`, `192.0.2.0/24`, `192.168.0.0/16`, `198.18.0.0/15`, `198.51.100.0/24`, `203.0.113.0/24`, `224.0.0.0/4`, `240.0.0.0/4`; IPv6 `::/128`, `::1/128`, `fc00::/7`, `fe80::/10`, `ff00::/8`, `2001:db8::/32`; and IPv4-mapped or NAT64 addresses whose embedded IPv4 address is blocked.
- **No redirects are followed** (a public URL could redirect to an internal one).
- **HTTPS only** unless `ALLOW_HTTP=true`.
- The same rules run when an endpoint is created or updated, to reject obviously bad URLs early.
- `ALLOW_PRIVATE_ENDPOINTS=true` disables the IP rules. It exists only because the local fake endpoints live on a private Docker network. It defaults to `false`, logs a loud warning at startup when on, and the SSRF tests run with it off.
- The alternative, a separate egress proxy (Smokescreen, as Convoy uses), was rejected as more to operate. Static outbound IPs for customer allowlists come from a NAT gateway and belong to the optional Terraform.

### Data

- API keys are stored only as hashes and shown once at creation.
- Endpoint secrets are encrypted at rest with AES-256-GCM using `SECRET_ENCRYPTION_KEY` from the environment.
- Store only the first 1 KB of response bodies in the attempt log.

---

## 16. Observability

Instrument with `tracing` and export with **OpenTelemetry over OTLP**, so one set of instrumentation can feed any backend.

**Metrics** (labelled by role; avoid per-endpoint labels, which would explode cardinality):

| Metric | Why |
| --- | --- |
| End-to-end delivery latency histogram (event created → 2xx), split first-attempt vs retried | The number customers care about; p50/p95/p99 |
| Deliveries by outcome (succeeded, retry_scheduled, failed, dead) and first-attempt success rate | Overall health |
| Kafka consumer lag (from `rdkafka` statistics) | Are workers keeping up; also the autoscaling signal |
| Oldest unpublished outbox row age | Relay stuck or slow |
| Retry backlog count and oldest overdue | Scheduler health |
| Dead-delivery count | Events that failed every attempt |
| Open circuit breakers | How many endpoints are down |
| Ingest rate and API request latency | Load |
| Outbound attempt duration and status class | Endpoint behaviour |

**Traces:** one trace follows an event end to end: API request → outbox → relay publish → worker consume → outbound HTTP call. Carry the W3C `traceparent` on the outbox row and as a Kafka message header.

**Logs:** structured JSON, always including `tenant_id`, `event_id`, `delivery_id`, `endpoint_id` where known. Never log payloads, secrets or API keys.

**SLO:** 99% of events to healthy endpoints delivered within 60 seconds. Measure it on first-attempt deliveries.

**Alert rules** (Prometheus rules file): consumer lag growing for more than 5 minutes; outbox age above 30 seconds; dead-delivery count rising quickly; outbound failure rate spiking across many endpoints at once (that means the problem is ours).

**Local stack**, all in Docker Compose and provisioned automatically: OpenTelemetry Collector → Prometheus → Grafana with a ready-made "Hooklane" dashboard showing everything above, plus Jaeger for traces. Document in `docs/OBSERVABILITY.md` how to point the same OTLP output at Datadog.

---

## 17. Running it locally

**The only requirement for `make up` is Docker.** Rust, `cmake`, `kind`, `kubectl` and Terraform are needed only for native development and the optional parts; list them in the README with install commands for macOS.

`docker-compose.yml` runs: Postgres, Redpanda (single node), **Redpanda Console** (so the owner can see the topic's 64 partitions and consumer lag in a browser), a one-shot `migrate` job, `api`, `relay`, `scheduler`, `worker` (scalable with `--scale worker=N`), `sink`, OpenTelemetry Collector, Prometheus, Grafana, Jaeger. Services wait for their dependencies with health checks, and every role retries its Postgres and Kafka connections with backoff instead of crashing.

Use a multi-stage Dockerfile with dependency caching (for example `cargo-chef` or BuildKit cache mounts): the first build compiles `librdkafka` and is slow, and rebuilds must not repeat that.

**Makefile targets** (the README documents each):

| Target | Does |
| --- | --- |
| `make up` / `make down` / `make logs` | Start, stop (and remove volumes), tail the whole stack |
| `make demo` | Start the stack, create a tenant and endpoints pointing at the sink (one healthy, one flaky, one always failing), send 100 events, print a delivery summary, print the URLs for Grafana, Jaeger and Redpanda Console |
| `make test` | Unit tests, then integration tests (starts test dependencies itself) |
| `make lint` | fmt check and clippy |
| `make load` / `make load-full` | Steady-load test at the default size / at 1M events |
| `make load-spike`, `make chaos-worker`, `make chaos-endpoints`, `make chaos-db`, `make load-compare` | The other tests in section 19 |
| `make k8s-up` / `make k8s-demo` / `make k8s-down` | Local Kubernetes with autoscaling (section 18) |

**VS Code / Cursor**

- `.vscode/tasks.json`: one task per Makefile target, so the stack, tests and load runs can be started from "Run Task".
- `.vscode/launch.json`: debug configurations (CodeLLDB) for each role running natively against the Compose dependencies, and for the test suite.
- `.vscode/extensions.json`: recommend rust-analyzer, CodeLLDB, Even Better TOML and Docker. With rust-analyzer, every test shows Run/Debug buttons inline and in the Testing panel.
- `.vscode/settings.json`: format on save, clippy as the check command.

---

## 18. Kubernetes (local with kind) and optional Terraform

`deploy/k8s/` holds kustomize manifests:

- One Deployment per role, all using the **same image with a different `--role`**.
- `api`: Service, HPA on CPU.
- `worker`: a KEDA `ScaledObject` with the Kafka trigger on consumer group `hooklane-workers`, topic `hooklane.deliveries`, scaling on **consumer lag** between a minimum of 1 and a maximum that defaults to 16 on kind (64 in the cloud overlay). Lag is the right signal because it directly measures falling behind; CPU does not.
- Postgres and single-node Redpanda as simple in-cluster workloads for kind; the `sink`; a migrate Job.
- Readiness and liveness probes, resource requests, and a termination grace period long enough for graceful shutdown.

`make k8s-up` creates a kind cluster, installs KEDA and metrics-server, builds and loads the image, and applies the manifests. `make k8s-demo` sends a burst of events and shows worker pods scaling up as lag grows and back down as it drains.

**Terraform (optional, last, never applied).** `deploy/terraform/` describes the cloud version: VPC with a NAT gateway and Elastic IP (static outbound address), EKS, RDS Postgres, and Redpanda in the cluster. Validate with `terraform fmt -check` and `terraform validate` only. Its README must state the rough hourly cost, that it is meant to be created for a load test and destroyed straight after, and the exact destroy command. If any earlier phase is incomplete, skip this.

---

## 19. Tests

### Unit tests

Retry schedule and jitter bounds; outcome classification table; `Retry-After` parsing and clamping; token bucket; every circuit-breaker transition; signing (including the `standardwebhooks` cross-check and rotation with two signatures); every blocked SSRF range, IPv4-mapped IPv6, and literal-IP URLs; offset tracker (out-of-order completion commits only the contiguous prefix); cursor encoding; secret encryption round trip.

### Integration tests (real Postgres and Redpanda, plus the sink)

Run each of these in **both queue modes** where it applies:

- Event → delivered once, signature valid, headers correct.
- Fan-out to several endpoints; event-type filtering.
- Idempotency key sent twice concurrently → one event.
- Failing endpoint → retried on the (test-shortened) schedule → `dead` after 8 attempts → system event emitted.
- 4xx → `failed`, no retry. 410 → endpoint disabled. 429 with `Retry-After` honored.
- **Relay duplicate:** publish the same outbox row twice → exactly one HTTP request.
- **Worker crash:** kill a worker mid-delivery → reaper recovers → delivered, nothing lost.
- Circuit breaker opens, parks deliveries without consuming attempts, probes, closes.
- A slow endpoint does not delay a healthy endpoint in the same partition.
- SSRF: endpoint resolving to a private IP is refused at send time; redirects are not followed.
- Tenant isolation: tenant A cannot read or resend tenant B's data.
- Resend and recover.
- Cursor pagination returns every row exactly once while rows are being inserted.

### The sink (fake customer endpoints)

`crates/sink` is a small HTTP server whose URL path selects behaviour: always 200, always 500, slow (configurable delay), timeout, 429 with `Retry-After`, 410, and flaky (fails a configurable percentage). It verifies every signature and **records every `webhook-id` it receives per path**, so lost and duplicate deliveries can be counted exactly.

### Load and chaos tests

`crates/loadcheck` verifies a finished run by comparing what Postgres says should have been delivered with what the sink actually received, and reports: events sent, **lost (must be 0)**, **duplicates**, delivery latency p50/p95/p99, peak and sustained events per second, and whether the SLO was met. It writes a Markdown report to `docs/results/` that includes the date, machine (CPU, memory), worker count, queue mode and test size.

The k6 scripts create one tenant and N endpoints (each subscribed to its own event type, so each event goes to one endpoint) and post events with a ramping arrival rate. Sizes are parameters; the default must finish in a few minutes on a laptop (about 100k events) and `make load-full` uses 1M.

| Test | How | Pass when |
| --- | --- | --- |
| Steady load | Rising rate to the target total | 99% delivered within 60 s to healthy endpoints; peak events/second recorded; zero lost |
| Spike | 10× the normal rate for a period (10 minutes at full size) | Consumer lag drains afterwards; zero lost |
| Worker crash | Kill worker containers with SIGKILL during a run | Zero lost; duplicates counted and explained |
| Failing endpoints | 30% of sink endpoints return 5xx, time out or send 429 | Healthy endpoints still meet the SLO; breakers open and close |
| Database restart | Restart Postgres during a run | Service recovers; zero lost |
| Postgres vs Kafka | Same steady load in both queue modes | Both throughputs and each bottleneck written up |

Run every test at the default size, save the real reports, and summarize them in `docs/RESULTS.md`. Label them clearly as measured on a local Docker setup. If you find and fix a bottleneck (connection pool size, batching attempt writes, relay batch size), record the before and after numbers: that story is valuable.

### CI

`.github/workflows/ci.yml`: fmt, clippy, unit tests, integration tests with Postgres and Redpanda as service containers.

---

## 20. Documentation to write

The system must be explainable without the code open. Write for an engineer who is strong in TypeScript and new to this stack.

- **`README.md`**: what it is, a Mermaid architecture diagram, a 3-command quickstart, the Makefile targets, the URLs of the local dashboards, a results summary, limits and non-goals.
- **`docs/DECISIONS.md`**: the eight decisions below, each with the choice, the alternatives rejected, the reason, and a one-sentence "how to explain it". Add any choices you made where this spec left a detail open.

  | # | Decision | Choice | Rejected | Why |
  | --- | --- | --- | --- | --- |
  | 1 | Language | Rust | Go, TypeScript, Python | Compiler rejects whole classes of bugs, which is a strong check on agent-written code; proven for this job by Svix |
  | 2 | Queue | Postgres outbox + Kafka | Postgres only, Redis, SQS | Outbox avoids the dual-write problem; Kafka spreads load and partitions by endpoint |
  | 3 | Retries | 8 attempts over ~28 h with jitter, then dead-letter; retry times in Postgres | Kafka retry topics | Exact delays that honor `Retry-After`; durable; the log can show the next attempt |
  | 4 | Bad endpoints | Timeout, token bucket, circuit breaker, auto-disable; state in worker memory | Shared state in Redis | One worker owns each partition, so no coordination is needed |
  | 5 | Ordering | Best effort, with timestamp and sequence number | Strict ordering | One failure would block an endpoint for hours; Stripe and Svix choose the same |
  | 6 | Security | Standard Webhooks signing; in-process SSRF checks on the resolved IP; hashed keys; encrypted secrets | Separate egress proxy | Spec-compliant and simpler to run |
  | 7 | Deployment | One binary, four roles; Compose and kind locally; EKS via Terraform only for load tests | ECS Fargate; an always-on cluster | Kubernetes is the industry standard; tearing down keeps cost near zero |
  | 8 | Observability | OpenTelemetry; Prometheus and Grafana locally; SLO of 99% within 60 s | Vendor-specific SDKs | One instrumentation, any backend (including Datadog) |

- **`docs/CONCEPTS.md`**: a plain-English glossary, each entry two or three sentences plus where it lives in the code: transactional outbox, dual-write problem, at-least-once delivery, idempotency, competing consumers, partition, consumer group, consumer lag, offset commit, head-of-line blocking, exponential backoff with jitter, dead-letter queue, circuit breaker, token bucket, noisy neighbour, HMAC, replay attack, SSRF, DNS rebinding, stateless service, load balancer vs autoscaler, image vs container vs deployment, SLO.
- **`docs/CODE_TOUR.md`**: follow one event through the code, file by file, from `POST /v1/events` to the customer's `200`, then follow one failed delivery through retry to `dead`.
- **`docs/OBSERVABILITY.md`**, **`docs/RESULTS.md`** (with a "what broke and how I fixed it" section), **`docs/PROGRESS.md`**.
- **`docs/INTEGRATION.md`** and **`examples/typescript/`**: how an app such as Formsuite sends events (a small `fetch` example with an idempotency key) and how a receiver verifies signatures with the `standardwebhooks` npm package, including the 5-minute timestamp tolerance and deduping on `webhook-id`.

---

## 21. Phases and gates

**Phase 0: Scaffold.** Workspace, config, CLI skeleton, migrations for all seven tables, Dockerfile with caching, Compose with Postgres and the migrate job, Makefile, `.vscode`, CI, telemetry skeleton, `docs/PROGRESS.md`.
*Gate:* `make up` starts cleanly from nothing; `/healthz` and `/readyz` return 200; `make lint` and `make test` pass.

**Phase 1: Works end to end (Postgres queue mode).** Full API, auth, admin CLI, sink, worker claiming from Postgres, signing, classification, retry schedule, scheduler (due retries, reaper), delivery log, resend, idempotency, cursor pagination.
*Gate:* the Phase 1 integration tests pass; `make demo` shows the healthy endpoint at 100% delivered, the flaky one retrying, and the failing one heading to `dead`.

**Phase 2: Broker.** Queue abstraction, outbox writes in the ingest transaction, relay, topic with 64 partitions, Kafka worker with per-endpoint lanes and offset tracking, scheduler re-queueing through the outbox, Redpanda and Redpanda Console in Compose, both modes selectable.
*Gate:* every integration test passes in both modes, including relay-duplicate and worker-crash; a 10k-event smoke run in each mode reports zero lost.

**Phase 3: Protection and security.** Token bucket, circuit breaker, parking, lane overflow, auto-disable, `recover`, system events, SSRF resolver and literal-IP check, no redirects, HTTPS flag, secret rotation, secret encryption.
*Gate:* the protection, SSRF and rotation tests pass; a slow endpoint demonstrably does not delay a healthy one.

**Phase 4: Scale and observe.** Metrics, traces, JSON logs, Collector, Prometheus, alert rules, Grafana dashboard, Jaeger, graceful shutdown, kustomize manifests, KEDA, HPA, kind targets.
*Gate:* during `make demo` the dashboard shows live data and one trace in Jaeger spans API to outbound call; scaling workers up and down mid-run with `--scale` loses nothing; `make k8s-up` and `make k8s-demo` show workers scaling on lag (or the progress file states exactly what could not be run here and why).

**Phase 5: Prove it.** k6 scripts, chaos scripts, `loadcheck`, all six tests run at the default size, reports saved, `docs/RESULTS.md` written, bottlenecks investigated.
*Gate:* every test reports zero lost events; all reports contain real measured numbers.

**Phase 6: Finish.** All documentation in section 20, TypeScript examples, README, then the optional Terraform.
*Gate:* a fresh clone followed by `make up && make demo` works; `make lint` and `make test` pass; every item in section 23 is ticked or explicitly explained.

---

## 22. Out of scope

Do not build these. Mention them as future work in the README: receiving inbound webhooks; a customer-facing portal or any UI; multiple regions; a strict-ordering option; priority lanes; payload transformations; billing or usage limits; long-term data retention and table partitioning.

---

## 23. Requirements checklist

Copy this into `docs/PROGRESS.md` and tick each item only after verifying it.

**Architecture**
- [ ] One binary, roles selected with `--role api|relay|scheduler|worker`; `migrate` and `admin create-tenant` commands
- [ ] Event, deliveries and outbox rows written in one transaction; API returns 202
- [ ] Only the relay writes to Kafka; retries re-enter through the outbox
- [ ] Topic with 64 partitions, keyed by endpoint id
- [ ] `QUEUE_MODE=kafka|postgres`, both working
- [ ] Graceful shutdown on SIGTERM in every role; connection retry on Postgres or Kafka outage

**Data and API**
- [ ] Seven tables with the unique constraints and partial indexes in section 8
- [ ] Conditional status updates everywhere, with row counts checked
- [ ] Idempotency key enforced by a unique constraint
- [ ] Endpoint CRUD, secret fetch and rotation, recover, delivery log, resend
- [ ] Cursor pagination on every list route
- [ ] Tenant scoping on every query; API keys hashed; endpoint secrets encrypted

**Delivery**
- [ ] Conditional claim before every send
- [ ] Per-endpoint lanes, contiguous offset commits, backpressure, lane overflow parking
- [ ] 15 s timeout; 8-attempt schedule with ±20% jitter; `dead` after the last attempt
- [ ] Classification table implemented, including `Retry-After`, 410 disable, no redirects
- [ ] Reaper for stuck `delivering` rows
- [ ] System events on `dead` and on endpoint disable, with no loops

**Protection**
- [ ] Token bucket per endpoint, default 50/s, configurable
- [ ] Circuit breaker with closed, open and half-open states; parking consumes no attempt
- [ ] Auto-disable after about 5 days failing; recover API
- [ ] State kept in worker memory, with the reasoning documented

**Ordering and security**
- [ ] Best-effort ordering; `webhook-sequence` and event timestamp on every request
- [ ] Standard Webhooks signing verified against the official library; rotation with two signatures
- [ ] SSRF: resolved-IP check at connect time, literal-IP check, all listed ranges, HTTPS only by default
- [ ] `ALLOW_PRIVATE_ENDPOINTS` off by default and warned about when on

**Observability**
- [ ] All metrics in section 16; end-to-end trace through outbox and Kafka; structured logs
- [ ] SLO defined; alert rules file; Grafana dashboard provisioned; Jaeger; Datadog instructions

**Running and deploying**
- [ ] `make up` works with only Docker installed; `make demo` works on a fresh clone
- [ ] Redpanda Console available locally
- [ ] VS Code tasks, launch configurations and recommended extensions
- [ ] Kustomize manifests; KEDA scaling on consumer lag; HPA for the API; kind targets
- [ ] Terraform validated but never applied (or skipped, with the reason stated)

**Proof**
- [ ] Unit and integration tests in section 19 pass, in both queue modes where applicable
- [ ] Sink records every received id; `loadcheck` reports lost, duplicates and latency percentiles
- [ ] Six load and chaos tests run, with real reports in `docs/results/`
- [ ] CI workflow

**Documentation**
- [ ] README, DECISIONS, CONCEPTS, CODE_TOUR, OBSERVABILITY, RESULTS, INTEGRATION, TypeScript examples
- [ ] Module doc comments and "what would go wrong without this" comments at every guarantee

---

## 24. Final report

When finished, reply with:

1. What was built, phase by phase, in a few lines each.
2. The measured results: events per second, latency percentiles, lost and duplicate counts for each test, and the machine they were measured on.
3. Anything in section 23 that is not ticked, and why.
4. Anything you could not run or verify in this environment.
5. Choices you made where the spec was open.
6. The three commands a newcomer should run first.
