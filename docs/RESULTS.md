# Results

> **Measured on a local Docker setup, not production hardware.** Apple M1 Pro
> (10 cores, 32 GB) running Docker in a Colima VM limited to **6 CPUs / 12 GB**.
> Inside that one VM: Postgres 18, single-node Redpanda, the API, the relay,
> the scheduler, 4 workers, the sink (fake customer endpoints), the
> OpenTelemetry Collector, Prometheus, Grafana, Jaeger **and the k6 load
> generator**. Absolute numbers are a floor; they are useful for comparing
> designs and runs on the same machine.

Every number below comes from a report in [`results/`](results/), written by
`crates/loadcheck` after the run. `loadcheck` compares what Postgres says
should have been delivered with what the sink actually received (every
`webhook-id`, per endpoint), so **lost** and **duplicates** are exact counts,
not estimates. Latency is event `created_at` → delivery recorded as succeeded,
for first-attempt deliveries to healthy endpoints.

## Summary (default size: ~100k events, 100 endpoints, Kafka mode, 4 workers)

| Test | Events accepted | **Lost** | **Duplicates** | p50 / p95 / p99 | SLO (99% < 60 s) | Throughput (peak / sustained events/s) | Report |
|---|---:|---:|---:|---|---|---|---|
| Steady (target 2000/s) | 96,377 | **0** | **0** | 0.149 / 0.383 / 0.559 s | met (100%) | 2,392 / 1,785 | [steady](results/2026-11-20-steady-kafka-100000.md) |
| Spike (200/s base, 2000/s for 40 s) | 99,017 | **0** | **0** | 0.134 / 0.401 / 0.683 s | met (100%) | 2,489 / 656 | [spike](results/2026-11-20-1945-spike-kafka-100000.md) |
| Worker crash (3 × SIGKILL) | 101,132 | **0** | **17** | 0.026 / 8.834 / 10.541 s | met (100%) | 1,582 / 1,405 | [chaos-worker](results/2026-11-20-1947-chaos-worker-kafka-100000.md) |
| Failing endpoints (30%: 5xx / timeout / 429) | 100,555 | **0** | **0** | 0.047 / 0.339 / 0.632 s (healthy) | met (100%) | 1,604 / 1,396 | [chaos-endpoints](results/2026-11-20-1951-chaos-endpoints-kafka-100000.md) |
| Database restart | 98,481 (+59 rejected with 503) | **0** | **0** | 0.037 / 0.277 / 0.412 s | met (100%) | 2,169 / 1,368 | [chaos-db](results/2026-11-20-1953-chaos-db-kafka-100000.md) |
| Postgres vs Kafka | see below (10k size) | **0 / 0** | **0 / 0** | see below | met | see below | [pg](results/2026-11-20-steady-postgres-10000.md), [kafka](results/2026-11-20-steady-kafka-10000.md) |

Additional runs:

| Run | Result | Report |
|---|---|---|
| Scale workers mid-run (`docker compose --scale`, 4 → 8 → 2), 40k events | 40,297 accepted, **0 lost, 0 duplicates**, p99 1.49 s (rebalances), SLO met | [scale](results/2026-11-20-scale-kafka-40000.md) |
| Kubernetes (kind) autoscaling, 3000/s burst | KEDA scaled workers **1 → 3 → 7 → 11** as lag reached 21k, back to **1** after it drained; 77,634 / 77,634 delivered | [k8s](results/2026-11-20-k8s-autoscaling.md) |
| 10k smoke, each queue mode | 0 lost, 0 duplicates in both | (the compare row) |

**The sustained ceiling on this machine is ~1,750–1,800 events/s.** Each
event costs one ingest transaction plus a claim and a result transaction in
the worker, and the whole VM is CPU-bound (details below).

## Test by test

### Steady
Ramp to 2,000/s, hold. The system accepted 96,377 events with **zero
rejections** and delivered all of them; p99 0.56 s. Ingest p95 was 438 ms
because 2,000/s is just above the ~1,785/s the box sustains: requests queue
in the API's connection pool rather than fail.

### Spike
200/s for ~50 s, then **10× (2,000/s) for 40 s**, then 200/s again. Peak
consumer lag (sampled every 2 s) was **1,884 messages** and it drained to 0
during the second base period, before the load ended. Zero lost, SLO met.

### Worker crash
A random worker container was SIGKILLed every 25 s (3 kills); Docker's restart
policy brought each back, like a Kubernetes pod restart. **Zero lost. 17
duplicates**, which are expected and explainable: a delivery whose HTTP request
was in flight when its worker died stays `delivering`; the reaper re-queues it
after 2 minutes and it is sent again. The customer received it twice, with the
same `webhook-id`, so a receiver that dedupes sees it once. That is the
documented at-least-once window, about 6 requests per kill.

The latency tail (p95 8.8 s, p99 10.5 s) is the other cost of a crash: the
killed worker's partitions are not reassigned until Kafka's session timeout
(10 s) expires, so their messages wait. The drain took 107 s because of the
2-minute reaper threshold.

### Failing endpoints
30% of endpoints misbehave: a third return 500, a third time out (the sink
waits 20 s; Hooklane gives up at 15 s), a third return 429 with
`Retry-After: 5`. The **70% healthy endpoints received every event (70,388 /
70,388)** with p99 0.63 s, i.e. bad neighbours did not slow them down. The
failing endpoints' 30,167 deliveries were left in `retry_scheduled`/`pending`:
their circuit breakers opened after 5 failures and kept parking them (no
attempts consumed), exactly as designed. Those endpoints never recover during
the run, so their breakers do not close here; breaker closing on recovery is
covered by the `circuit_breaker_{pg,kafka}` integration tests.

### Database restart
`docker compose restart postgres` 30 s into a 1,500/s run (about 4 s
unavailable). During the outage the API returned **503** for 59 requests (k6
counted them as rejected, so they were never accepted and are not "lost"),
the relay, scheduler and workers retried their connections with backoff, and
everything resumed. **Zero lost** among the 98,481 accepted events.

### Postgres queue mode vs Kafka mode
Same settings: 10k events at 1,000/s, 50 endpoints, 4 workers, fresh volumes.

| Mode | Accepted | Lost | Dup | Delivery p50 / p95 / p99 | Sustained events/s | Ingest p95 |
|---|---:|---:|---:|---|---:|---:|
| Postgres (`QUEUE_MODE=postgres`) | 9,748 | 0 | 0 | 0.002 / 0.047 / 0.075 s | 813 | 3.0 ms |
| Kafka (`QUEUE_MODE=kafka`) | 9,746 | 0 | 0 | 0.014 / 0.023 / 0.046 s | 813 | 4.7 ms |

At this rate neither mode is saturated (both deliver exactly what is
offered), so the difference is the *shape* of latency:

- **Postgres mode** has the lowest median (2 ms): a worker that is polling
  claims a new row almost immediately. Its tail is wider (p99 75 ms) because
  workers poll in batches and sleep 100 ms when the table looks empty. Its
  bottleneck at scale is the database itself: every poll is an
  `UPDATE ... FOR UPDATE SKIP LOCKED` on the hottest table, so more workers
  add more write contention, and per-endpoint protections are only per-worker
  (shown in the demo: the failing endpoint got 5 attempts *per worker* before
  the breakers opened).
- **Kafka mode** adds a hop (outbox → relay → Kafka → worker), so the median
  is higher (14 ms), but it is tighter (p99 46 ms) and the database does less
  work per delivery for the queue itself. Its bottleneck is the outbox: the
  relay's writes and the ingest transaction's extra insert, and ultimately the
  64-partition ceiling on worker parallelism.

**Not run: the comparison at the default 100k size.** The host disk had
3.5 GB free by then, and `loadtest/run.sh` refuses to start a test below 4 GB
(see "what broke" below). Run `make load-compare` with more free space to get it.

## What broke and how I fixed it

1. **The API's database pool was the first bottleneck.** The first 100k
   steady run at 2,000/s (with `DATABASE_MAX_CONNECTIONS=20`): only **72,064**
   of 76,485 requests accepted, **4,221 rejected** (503 "database
   unavailable": pool acquire timed out), 24,514 iterations dropped by k6,
   sustained 1,332 events/s, ingest p95 1,258 ms. The API logs also showed
   `Too many open files (os error 24)`: the container's default limit of 1,024
   file descriptors, hit once k6 opened ~2,400 connections.
   **Fix:** API pool 20 → 64 (`API_DATABASE_MAX_CONNECTIONS`) and `nofile`
   65,536 for every role. **After:** 94,499 accepted, **0 rejected**, sustained
   1,741 events/s, peak 2,520/s; a further run (the saved report) sustained
   1,785/s with ingest p95 438 ms. *(The "before" report file was overwritten
   by the next run, which led to timestamped report names; the before numbers
   are from that run's console output.)*

2. **The second bottleneck is Postgres CPU, and I chose not to "fix" it by
   weakening durability.** Sampling `docker stats` during a steady run:
   Postgres ~260% CPU, API ~60%, 4 workers ~25% each, Redpanda ~25%, sink
   ~15%, plus k6, on a 6-CPU VM. Each event costs ~4 commits (ingest; relay
   marks published; worker claim; worker result). Turning off
   `synchronous_commit` would make it faster but would break guarantee 1 (a
   `202` must mean durable). The next steps, not done here, would be batching
   attempt writes per lane and batching claims, which cut the commits per event.

3. **Metric series from different worker replicas were merged.** All worker
   processes exported identical resource labels, so the Collector folded
   their counters and gauges together. **Fix:** a `service.instance.id`
   resource attribute (the container/pod hostname).

4. **SLO queries returned nothing.** OTLP histogram bucket bounds reach
   Prometheus as `le="60.0"`, not `le="60"`. Fixed in the dashboard and alerts.

5. **KEDA could not read consumer lag in Kubernetes.** Redpanda advertised
   `redpanda:9092`, which only resolves inside the `hooklane` namespace; KEDA
   runs in `keda`. The first k8s demo stayed at 1 worker (and still delivered
   60,448 / 60,448, just slower). **Fix:** advertise the FQDN
   `redpanda.hooklane.svc.cluster.local`. Second run scaled 1 → 11.

6. **The host disk filled up mid-test and corrupted the Docker VM's
   filesystem.** The Mac had 683 MB free; Colima keeps its VM disk as a file on
   the host, so the VM's writes failed, ext4 aborted its journal and Docker
   went read-only (`input/output error`). **Fix:** freed space, `e2fsck` on the
   VM data disk (clean repair), and `loadtest/run.sh` now refuses to start
   below 4 GB free and starts every test from fresh volumes.

7. **Smaller fixes found by running things:** Compose built the same image
   tag from five services at once (only `migrate` builds now); a native
   Postgres on the host shadowed port 5432 (Compose uses 55432); `kind load
   docker-image` fails for single-platform pulls of multi-arch images
   (`kind-up.sh` imports with `ctr`); KEDA 2.21 renamed its metrics deployment.

## Reproducing

```bash
make up
make load            # steady, ~100k events
make load-spike
make chaos-worker
make chaos-endpoints
make chaos-db
make load-compare
make load-scale
make k8s-up && make k8s-demo && make k8s-down
```

Each writes `docs/results/<date-time>-<test>-<mode>-<size>.md`. Sizes and rates
are parameters: `make load LOAD_EVENTS=1000000 LOAD_ENDPOINTS=1000` (or
`make load-full`) for 1M events. **The 1M run was not performed on this
machine** (disk space); expect roughly 10 minutes at the measured ceiling.
