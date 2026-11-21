# Load test report: chaos-endpoints

> Measured on a local Docker setup (Colima VM on a laptop). Numbers are for comparison
> between runs and designs, not a statement of production capacity.

| | |
|---|---|
| Date | 2026-11-21 03:51 UTC |
| Machine | Apple M1 Pro, 32 GB RAM host; Docker VM: 6 CPUs, 11.7 GB |
| Queue mode | kafka |
| Workers | 4 |
| Test size | 100555 events accepted (HTTP 202) |
| Settings | k6 ramping-arrival-rate, target 1500/s (steady), 100 endpoints, ingest latency p50/p95/p99 7.9ms/82.4ms/149.1ms, rejected 0 |
| Drained | yes (waited 6 s after the load ended) |

## Result

| Metric | Value |
|---|---|
| **Lost** | **0** (0 at ingest, 0 healthy deliveries never received) |
| **Duplicates** (extra requests to healthy endpoints) | **0** |
| Events in Postgres | 100555 |
| Healthy-endpoint deliveries | 70388 (0 not `succeeded` in Postgres) |
| Latency p50 / p95 / p99 (first attempt, healthy) | 0.047 s / 0.339 s / 0.632 s |
| Latency max | 1.051 s |
| SLO (99% within 60 s) | **met** (100.00% within 60 s) |
| Ingest events/s: peak (1 s) / sustained | 1604 / 1396 |
| Deliveries/s: peak (1 s) / sustained | 1359 / 965 |

## Deliveries by status (all endpoints)

| Status | Count |
|---|---|
| pending | 1000 |
| retry_scheduled | 29167 |
| succeeded | 70388 |

## Notes

30% of endpoints misbehave: a third return 500, a third time out (sink waits 20 s, Hooklane gives up at 15 s), a third return 429 with Retry-After: 5. Only the 70% healthy (/ok/) endpoints count towards lost/latency/SLO.

