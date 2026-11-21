# Load test report: steady

> Measured on a local Docker setup (Colima VM on a laptop). Numbers are for comparison
> between runs and designs, not a statement of production capacity.

| | |
|---|---|
| Date | 2026-11-21 02:31 UTC |
| Machine | Apple M1 Pro, 32 GB RAM host; Docker VM: 6 CPUs, 11.7 GB |
| Queue mode | postgres |
| Workers | 4 |
| Test size | 9088 events accepted (HTTP 202) |
| Settings | k6 ramping-arrival-rate, target 1000/s (steady), 50 endpoints, ingest latency p50/p95/p99 249.0ms/659.0ms/704.4ms, rejected 0 |
| Drained | yes (waited 0 s after the load ended) |

## Result

| Metric | Value |
|---|---|
| **Lost** | **0** (0 at ingest, 0 healthy deliveries never received) |
| **Duplicates** (extra requests to healthy endpoints) | **0** |
| Events in Postgres | 9088 |
| Healthy-endpoint deliveries | 9088 (0 not `succeeded` in Postgres) |
| Latency p50 / p95 / p99 (first attempt, healthy) | 0.034 s / 0.075 s / 0.142 s |
| Latency max | 0.416 s |
| SLO (99% within 60 s) | **met** (100.00% within 60 s) |
| Ingest events/s: peak (1 s) / sustained | 994 / 715 |
| Deliveries/s: peak (1 s) / sustained | 981 / 715 |

## Deliveries by status (all endpoints)

| Status | Count |
|---|---|
| succeeded | 9088 |

