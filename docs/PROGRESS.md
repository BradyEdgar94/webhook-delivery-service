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

### Phase 4: Scale and observe — gate passed (2026-11-20)
Built: OTel metrics for every item in spec section 16 (+ `hooklane.attempts`
for first-attempt success rate), traces API -> outbox -> relay -> Kafka header
-> worker -> outbound HTTP, JSON logs with span fields, Collector ->
Prometheus + Jaeger, alert rules (`deploy/compose/alerts.yml`, 7 rules,
`promtool check rules` passes), generated Grafana dashboard (18 panels),
graceful shutdown, `/healthz` on non-API roles for probes, kustomize base +
kind and cloud overlays, KEDA ScaledObject on consumer lag, HPA on CPU for the
API, `make k8s-up` / `k8s-demo` / `k8s-down`.

Gate evidence:
- Dashboard: every panel query returns data during a run (checked by running
  each panel's PromQL against Prometheus); only "p50 retried" was empty, since
  that run had no retries.
- Jaeger: a single trace contains `hooklane-api:api.ingest`,
  `hooklane-relay:relay.publish` (x3, one per fanned-out endpoint),
  `hooklane-worker:worker.deliver` and `hooklane-worker:http.post`.
- `--scale` mid-run (`make load-scale`, 40k events, workers 4 -> 8 -> 2):
  **0 lost, 0 duplicates**, SLO met (`docs/results/2026-11-20-scale-kafka-40000.md`).
- kind (`kind` v0.33 and `kubectl` 1.37 installed with Homebrew): `make k8s-up`
  brings everything up; `make k8s-demo` at 3000/s: workers scaled
  1 -> 3 -> 7 -> 11 as lag reached 21k, back to 1 after it drained; all 77,634
  deliveries succeeded (`docs/results/2026-11-20-k8s-autoscaling.md`).

Issues found and fixed:
- Worker replicas exported identical metric labels, so the Collector merged
  their series. Added `service.instance.id` (= HOSTNAME) to the resource.
- Histogram bucket labels arrive as `le="60.0"`, not `"60"`; SLO queries fixed.
- KEDA could not resolve Redpanda's advertised short name `redpanda:9092`
  from its own namespace; the broker now advertises the FQDN.
- `kind load docker-image` fails for multi-arch images pulled for one platform
  (`--all-platforms`); kind-up.sh imports via `ctr` directly.
- KEDA 2.21's metrics deployment is `keda-metrics-apiserver`.
- In the k8s setup, telemetry export is not wired (no Collector in-cluster);
  the Compose stack is where dashboards/traces are demonstrated.

### Phase 5: Prove it — gate passed, with one test at reduced size (2026-11-20)
Built: k6 script (`loadtest/load.js`: steady and spike profiles, failing
endpoint mix), `loadtest/run.sh` (stack per test on fresh volumes, chaos
actions, disk guard, loadcheck), `crates/loadcheck` (lost, duplicates,
latency percentiles, SLO, throughput; Markdown report).

Gate evidence (all reports in `docs/results/`, summarised in `docs/RESULTS.md`):
| Test | Size | Lost | Dup |
|---|---|---:|---:|
| steady | 96,377 | 0 | 0 |
| spike | 99,017 | 0 | 0 |
| chaos-worker (3 × SIGKILL) | 101,132 | 0 | 17 (explained) |
| chaos-endpoints (30% failing) | 100,555 | 0 | 0 |
| chaos-db (Postgres restart) | 98,481 | 0 | 0 |
| compare pg / kafka | 9,748 / 9,746 | 0 / 0 | 0 / 0 |

Deferred: **compare at the default 100k size was not run**: the host disk
was at 3.5 GB free and `run.sh` refuses below 4 GB. The 10k comparison (same
settings, fresh volumes) is real and reported. **1M (`make load-full`) not run**
for the same reason.

Bottleneck work: API pool 20 -> 64 and `nofile` 65,536 took the steady run
from 72,064 accepted / 4,221 rejected / 1,332 ev/s sustained to 94,499 /
0 / 1,741 ev/s (then 1,785 ev/s in the saved run). Remaining limit: whole VM
CPU-bound, Postgres ~260% CPU; `synchronous_commit` deliberately left on.

### Phase 6: Finish — gate passed (2026-11-20)
Built: README (Mermaid diagram, quickstart, targets, URLs, results, limits),
DECISIONS (8 decisions + 16 choices where the spec was open), CONCEPTS,
CODE_TOUR, OBSERVABILITY (incl. Datadog), RESULTS, INTEGRATION,
`examples/typescript` (send with idempotency key, verify with
`standardwebhooks`, selftest), optional Terraform (VPC + NAT/EIP, EKS, ECR, RDS).

Gate evidence:
- Fresh `git clone` (under the home directory) -> `make up && make demo`:
  healthy 100/100 (0 duplicates, 0 bad signatures), flaky 97 + 3 retrying,
  failing 5 attempts then breaker open. (A clone under `/private/tmp` fails
  only because Colima shares just the home directory with its VM.)
- `make lint` clean; `make test`: 60 unit + 34 integration tests pass.
- TypeScript: `npm run selftest` 6/6 against the official npm library, and a
  real end-to-end run: `send-event.ts` -> Hooklane -> `verify.ts` verified the
  signature; repeated Idempotency-Key returned `duplicate: true`; a forged
  request got 401.
- Terraform: `terraform init -backend=false && terraform validate` -> valid;
  `terraform fmt -check` clean. **Never applied.** (Provider download deleted
  afterwards to save disk.)

### Follow-up after publishing (2026-11-21)
- **Deferred tests now run** (disk space freed): `make load-full` accepted
  943,815 events with 0 lost, 0 duplicates, sustained 1,753 events/s; the
  Postgres-vs-Kafka pair at 100k: 100,744 / 100,602 accepted, 0 lost in both.
  Run on battery with Low Power Mode on. Two disturbed comparison attempts
  (dashboard screenshots during the run; laptop asleep) were discarded.
- **CI fixed.** The first runs on GitHub failed compiling `rdkafka-sys`:
  `curl/curl.h: No such file or directory`. macOS and the rust Docker image
  ship the libcurl headers; GitHub's Ubuntu runners do not. CI now installs
  `libcurl4-openssl-dev`. To find this without access to the logs, CI prints
  the first error of a failed run as an annotation (`.github/first-error.sh`).
  Lint and all 34 integration tests now pass on GitHub.
- **Prebuilt image** published to GHCR for amd64 and arm64 by
  `.github/workflows/image.yml`; `make demo PREBUILT=1` uses it.
- Added `docs/SPEC.md`, an autoscaling chart and a dashboard screenshot.

## Requirements checklist (spec section 23)

**Architecture**
- [x] One binary, roles selected with `--role api|relay|scheduler|worker`; `migrate` and `admin create-tenant` commands
- [x] Event, deliveries and outbox rows written in one transaction; API returns 202
- [x] Only the relay writes to Kafka; retries re-enter through the outbox
- [x] Topic with 64 partitions, keyed by endpoint id
- [x] `QUEUE_MODE=kafka|postgres`, both working
- [x] Graceful shutdown on SIGTERM in every role; connection retry on Postgres or Kafka outage

**Data and API**
- [x] Seven tables with the unique constraints and partial indexes in section 8
- [x] Conditional status updates everywhere, with row counts checked
- [x] Idempotency key enforced by a unique constraint
- [x] Endpoint CRUD, secret fetch and rotation, recover, delivery log, resend
- [x] Cursor pagination on every list route
- [x] Tenant scoping on every query; API keys hashed; endpoint secrets encrypted

**Delivery**
- [x] Conditional claim before every send
- [x] Per-endpoint lanes, contiguous offset commits, backpressure, lane overflow parking
- [x] 15 s timeout; 8-attempt schedule with ±20% jitter; `dead` after the last attempt
- [x] Classification table implemented, including `Retry-After`, 410 disable, no redirects
- [x] Reaper for stuck `delivering` rows
- [x] System events on `dead` and on endpoint disable, with no loops

**Protection**
- [x] Token bucket per endpoint, default 50/s, configurable
- [x] Circuit breaker with closed, open and half-open states; parking consumes no attempt
- [x] Auto-disable after about 5 days failing; recover API
- [x] State kept in worker memory, with the reasoning documented

**Ordering and security**
- [x] Best-effort ordering; `webhook-sequence` and event timestamp on every request
- [x] Standard Webhooks signing verified against the official library; rotation with two signatures
- [x] SSRF: resolved-IP check at connect time, literal-IP check, all listed ranges, HTTPS only by default
- [x] `ALLOW_PRIVATE_ENDPOINTS` off by default and warned about when on

**Observability**
- [x] All metrics in section 16; end-to-end trace through outbox and Kafka; structured logs (Postgres mode has no outbox, so its traces start at the worker; k8s setup does not export telemetry)
- [x] SLO defined; alert rules file; Grafana dashboard provisioned; Jaeger; Datadog instructions

**Running and deploying**
- [x] `make up` works with only Docker installed; `make demo` works on a fresh clone (verified with a fresh clone; the build and demo use only Docker, curl and standard shell tools)
- [x] Redpanda Console available locally
- [x] VS Code tasks, launch configurations and recommended extensions
- [x] Kustomize manifests; KEDA scaling on consumer lag; HPA for the API; kind targets
- [x] Terraform validated but never applied (or skipped, with the reason stated)

**Proof**
- [x] Unit and integration tests in section 19 pass, in both queue modes where applicable
- [x] Sink records every received id; `loadcheck` reports lost, duplicates and latency percentiles
- [x] Six load and chaos tests run, with real reports in `docs/results/` — all six at the default ~100k size (the compare pair was rerun at 100k on 2026-11-21), plus a 1M run
- [x] CI workflow

**Documentation**
- [x] README, DECISIONS, CONCEPTS, CODE_TOUR, OBSERVABILITY, RESULTS, INTEGRATION, TypeScript examples
- [x] Module doc comments and "what would go wrong without this" comments at every guarantee
