# Observability

Hooklane is instrumented once, with the Rust `tracing` crate and
OpenTelemetry, and exports everything over **OTLP/HTTP** to an OpenTelemetry
Collector. The Collector decides where data goes. Locally that is Prometheus
(metrics), Jaeger (traces) and Grafana (dashboards). Switching to Datadog is
a Collector config change (below); the application does not change.

```
hooklane (every role) --OTLP/HTTP :4318--> otel-collector --> Prometheus :9090 --> Grafana :3000
                                                         \--> Jaeger :16686
```

Set `OTEL_EXPORTER_OTLP_ENDPOINT` (e.g. `http://otel-collector:4318`) to
enable export; leave it empty to disable (tests do).

## Local URLs (`make up`)

| What | URL |
|---|---|
| Grafana, "Hooklane" dashboard (anonymous admin) | http://localhost:3000 |
| Prometheus (queries, alert rules at /alerts) | http://localhost:9090 |
| Jaeger (traces) | http://localhost:16686 |
| Redpanda Console (topic, 64 partitions, consumer lag) | http://localhost:8081 |

## Metrics

Names as they appear in Prometheus (OTel names with `.` → `_` and units/`_total` added).
Every series carries `hooklane_role` and `service_instance_id` (one per process).
**No metric is labelled by tenant or endpoint**: with thousands of endpoints
that would create thousands of series per metric (cardinality explosion).

| Metric | Type | Labels | Why |
|---|---|---|---|
| `hooklane_delivery_latency_seconds` | histogram | `first_attempt` | Event created → 2xx. p50/p95/p99 and the SLO. |
| `hooklane_deliveries_total` | counter | `outcome` (succeeded, retry_scheduled, failed, dead, parked, skipped) | Overall health |
| `hooklane_attempts_total` | counter | `first_attempt`, `result` | First-attempt success rate |
| `hooklane_kafka_consumer_lag` | gauge | per worker | Falling behind? Autoscaling signal |
| `hooklane_outbox_oldest_unpublished_age_seconds` | gauge | | Relay stuck or slow |
| `hooklane_retry_backlog`, `hooklane_retry_oldest_overdue_seconds` | gauge | | Scheduler health |
| `hooklane_deliveries_dead`, `hooklane_deliveries_pending` | gauge | | Dead-letter size; queue depth |
| `hooklane_breakers_open` | gauge | per worker | How many endpoints are down |
| `hooklane_events_ingested_total` | counter | | Ingest rate |
| `hooklane_api_request_duration_seconds` | histogram | `route`, `status_class` | API latency |
| `hooklane_attempt_duration_seconds` | histogram | `status_class` | Outbound behaviour |
| `hooklane_relay_published_total` | counter | | Relay throughput |

Histogram bucket labels arrive as floats: use `le="60.0"`, not `le="60"`.

Useful queries:

```promql
# p99 end-to-end latency, first attempts
histogram_quantile(0.99, sum by (le) (rate(hooklane_delivery_latency_seconds_bucket{first_attempt="true"}[5m])))

# SLO: share of first-attempt deliveries within 60 s
sum(rate(hooklane_delivery_latency_seconds_bucket{first_attempt="true",le="60.0"}[5m]))
  / sum(rate(hooklane_delivery_latency_seconds_count{first_attempt="true"}[5m]))

# first-attempt success rate
sum(rate(hooklane_attempts_total{first_attempt="true",result="success"}[5m]))
  / sum(rate(hooklane_attempts_total{first_attempt="true"}[5m]))
```

## SLO

**99% of events to healthy endpoints are delivered within 60 seconds,
measured on first-attempt deliveries.** Retries are excluded on purpose:
they are slow by design (5 s, 5 m, 30 m ...) and their latency measures the
customer's outage, not our pipeline. The recording rule
`hooklane:slo_first_attempt_within_60s:ratio_5m` tracks it, and every load
test report (`crates/loadcheck`) computes it from Postgres.

## Alerts (`deploy/compose/alerts.yml`)

| Alert | Fires when | Means |
|---|---|---|
| `HooklaneSLOBurn` | SLO ratio < 99% for 10 m | We are slow for healthy endpoints |
| `HooklaneConsumerLagGrowing` | lag > 1000 and still growing for 5 m | Workers falling behind (KEDA not keeping up or at 64 partitions) |
| `HooklaneOutboxStale` | oldest unpublished outbox row > 30 s | Relay down or cannot reach Kafka |
| `HooklaneDeadDeliveriesRising` | +100 dead in 15 m | Many deliveries exhausting retries |
| `HooklaneRetryBacklogOverdue` | due retries overdue > 2 m | Scheduler stuck |
| `HooklaneOutboundFailureSpike` | > 50% of all attempts failing and > 10 breakers open | Problem is likely ours (egress, DNS, TLS) |

Validate with `docker compose run --rm --no-deps --entrypoint promtool prometheus check rules /etc/prometheus/alerts.yml`.
Alertmanager routing (Slack, PagerDuty) is not part of the local stack.

## Traces

One trace follows an event end to end:

```
http.request (api) └ api.ingest
   └ relay.publish            parent = traceparent stored on the outbox row
      └ worker.deliver        parent = traceparent Kafka message header
         └ http.post          the outbound call
```

The W3C `traceparent` is captured in the API, stored on the `outbox` row,
re-injected by the relay as a Kafka header, and extracted by the worker
(`telemetry.rs`: `current_traceparent`, `set_parent_from_traceparent`).
Retries start a new trace at the relay (their outbox row has no traceparent).
In Postgres queue mode there is no outbox, so traces start at the worker.

In Jaeger, search service `hooklane-api`, operation `api.ingest`.

## Logs

Structured JSON on stdout (`LOG_FORMAT=json`, the default), one object per
line. Fields from the enclosing span are flattened in: `tenant_id`,
`event_id`, `delivery_id`, `endpoint_id`, `outcome`. Payloads, secrets and API
keys are never logged. `LOG_FORMAT=pretty` for a human terminal; `RUST_LOG`
controls levels (default `info`).

## Sending the same data to Datadog

The application keeps exporting OTLP; only the destination changes. Two options:

**Option A: Datadog exporter in the Collector** (keep the Collector).
In `deploy/compose/otel-collector.yaml` (the contrib image includes it):

```yaml
connectors:
  datadog/connector: {}

exporters:
  datadog:
    api:
      key: ${env:DD_API_KEY}
      site: datadoghq.com          # or datadoghq.eu, us5.datadoghq.com ...

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [batch]
      exporters: [datadog/connector, datadog]
    metrics:
      receivers: [otlp, datadog/connector]
      processors: [batch]
      exporters: [datadog]
```

and pass `DD_API_KEY` to the `otel-collector` service. The `datadog/connector`
computes APM trace metrics (request rates, error rates, latency) from spans.

**Option B: the Datadog Agent's OTLP receiver** (no Collector).
Run the Agent with
`DD_OTLP_CONFIG_RECEIVER_PROTOCOLS_HTTP_ENDPOINT=0.0.0.0:4318` and point
Hooklane at it: `OTEL_EXPORTER_OTLP_ENDPOINT=http://datadog-agent:4318`.

Either way, metric names stay the same (`hooklane.delivery.latency` etc.),
so the dashboard and alerts above translate one to one into Datadog monitors.
