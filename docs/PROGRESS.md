# Progress

This file is the resumption point for any future session. It records what was
built per phase, what each gate check actually showed, and what is deferred.

## Environment

- Machine: Apple M1 Pro (10 cores), 32 GB RAM, macOS (Darwin 25.6).
- Docker: Colima VM, **6 CPUs / 12 GB** (raised from the 2 CPU / 4 GB default so
  Postgres + Redpanda + observability + Rust builds fit; `colima start --cpu 6 --memory 12`).
- Rust 1.98.1 (Homebrew) natively; the Docker build uses `rust:1.100-bookworm`.
- Installed during the build: `cmake` (Homebrew), needed for native `rdkafka` builds.

## Phase log

### Phase 0: Scaffold — gate passed (2026-11-20)
Built: Cargo workspace (`hooklane`, `sink`, `loadcheck`), clap config (every
setting is an env var, `.env.example`), CLI (`--role`, `migrate`, `admin
create-tenant`, `admin gen-key`), migration for all seven tables, cargo-chef
Dockerfile, Compose (Postgres, Redpanda, Console, migrate job, 4 roles, sink,
OTel Collector, Prometheus, Grafana, Jaeger), Makefile, `.vscode`, CI workflow,
telemetry module.

Gate evidence:
- `make up` from nothing: all 16 containers healthy; migrate applied the schema
  and created `hooklane.deliveries` with 64 partitions (`rpk topic describe`).
- `curl localhost:8080/healthz` -> 200 `ok`; `/readyz` -> 200 `ready`.
- `make lint` clean (fmt + clippy `-D warnings`); `make test` -> 60 unit tests pass.

Issues found and fixed:
- Compose built the same image tag from five services in parallel and the
  exports collided. Now only `migrate` builds; the others use `pull_policy: never`.
- This machine already runs a native Postgres on 127.0.0.1:5432, which shadows
  Docker's port forward. Hooklane's Postgres is published on **55432** instead.

### Phase 1: Works end to end (Postgres queue mode) — gate passed (2026-11-20)
Built: full API (ingest, endpoints CRUD, secret, rotate, recover, delivery log,
resend), auth with 60 s key cache, admin CLI, sink, Postgres-mode worker
(`FOR UPDATE SKIP LOCKED` batch claim), signing, classification, retry
schedule, scheduler (due retries, reaper), idempotency, cursor pagination.
(Phase 2 and 3 code was written in the same pass; their gates are run below.)

Gate evidence:
- Integration suite: 27/27 pass (`make test-integration`, 75 s), which covers
  every Phase 1 scenario in Postgres mode (plus Kafka mode).
- `QUEUE_MODE=postgres make demo`: healthy 100/100 delivered (sink: 100
  unique ids, 0 duplicates, 0 bad signatures), flaky retrying (46 succeeded,
  54 retry_scheduled), failing retrying towards `dead` (100 retry_scheduled).
  Note it shows the documented Postgres-mode limit: with 2 workers the failing
  endpoint got 10 first attempts before the breakers opened (5 per worker).
- `make demo` (Kafka mode): healthy 100/100, failing got exactly 5 attempts
  before its breaker opened (single partition owner).

Fixed during the gate: SSRF integration test inserted its DNS-rebinding
endpoint after sending the event (test bug); now sends a fresh event.

### Disk incident (2026-11-20)
The host Mac ran out of disk (683 MB free). Colima keeps its VM disk as a host
file, so the VM's ext4 journal aborted mid-load-test and Docker went
read-only. After the owner freed space: `e2fsck -fy` on the VM data disk (clean
repair, no data lost), `fstrim` to return freed blocks, and `loadtest/run.sh`
now refuses to start below 4 GB free and resets Hooklane's volumes per test.

### Phase 2: Broker — gate passed (2026-11-20)
Built: queue abstraction (`queue/`), outbox rows in the ingest transaction,
relay (`FOR UPDATE SKIP LOCKED`, acks=all, idempotent producer, marks published
only after every ack), 64-partition topic keyed by endpoint id, Kafka worker
with per-endpoint lanes, contiguous offset commits, per-partition pause/resume
backpressure, commit-on-revoke, scheduler re-queueing through the outbox,
pending-rescue safety net, Redpanda + Console in Compose.

Gate evidence:
- All integration tests pass in both modes, including
  `relay_duplicate_gives_one_request_kafka` (same outbox row published twice ->
  1 HTTP request) and `worker_crash_{pg,kafka}` (worker runtime killed
  mid-request -> reaper -> delivered, `worker_lost` attempt logged).
- 10k smoke runs on fresh volumes (`docs/results/2026-11-20-steady-*-10000.md`):
  postgres 9748 accepted, **0 lost, 0 duplicates**, p99 75 ms;
  kafka 9746 accepted, **0 lost, 0 duplicates**, p99 46 ms.

### Phase 3: Protection and security — gate passed (2026-11-20)
Built: token bucket, circuit breaker, parking (no attempt used), 429 lane
pause, lane overflow parking, auto-disable, recover, system events (no loops),
SSRF resolver + literal-IP check, no redirects, no proxy, HTTPS flag, secret
rotation (two signatures for 24 h), AES-256-GCM secret encryption.

Gate evidence: integration suite 34/34 (twice in a row), adding
`secret_rotation_*`, `auto_disable_*`, `lane_overflow_parks_excess_kafka`,
`rate_limit_*` to the existing breaker / SSRF / redirect tests.
`slow_does_not_block_{pg,kafka}`: with both endpoints on one partition and 5
deliveries queued on a 3-second endpoint, the healthy endpoint is served in
well under 1.5 s.

## Requirements checklist (spec section 23)

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
