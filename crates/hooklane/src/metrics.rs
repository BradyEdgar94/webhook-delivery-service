//! Metric instruments (OpenTelemetry), exported over OTLP and scraped into
//! Prometheus by the Collector.
//!
//! Labels are kept low-cardinality on purpose: role, outcome, status class.
//! **Never label by endpoint or tenant id**: with thousands of endpoints that
//! would create thousands of time series per metric ("cardinality
//! explosion"), which is slow and expensive in any metrics backend.
//!
//! When no OTLP endpoint is configured the global meter is a no-op, so these
//! calls cost almost nothing (tests, `cargo run` without the stack).

use std::sync::OnceLock;

use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::metrics::{Counter, Gauge, Histogram};

pub struct Metrics {
    /// Event created -> 2xx received, seconds. Label `first_attempt`=true/false.
    pub delivery_latency: Histogram<f64>,
    /// Final or intermediate outcome of each attempt: succeeded, retry_scheduled, failed, dead, parked.
    pub deliveries: Counter<u64>,
    /// HTTP attempts. Labels: `first_attempt` (true/false), `result` (success/failure).
    /// First-attempt success rate = success / all, where first_attempt="true".
    pub attempts: Counter<u64>,
    /// Events accepted by the API.
    pub events_ingested: Counter<u64>,
    /// API request duration, seconds. Labels: route, status class.
    pub api_latency: Histogram<f64>,
    /// Outbound HTTP attempt duration, seconds. Label: status class (2xx, 4xx, 5xx, error).
    pub attempt_duration: Histogram<f64>,
    /// Sum of consumer lag over this worker's partitions (from rdkafka statistics).
    pub consumer_lag: Gauge<i64>,
    /// Endpoints whose circuit breaker is not closed, in this worker.
    pub open_breakers: Gauge<i64>,
    /// Age of the oldest unpublished outbox row, seconds.
    pub outbox_oldest_age: Gauge<f64>,
    /// Deliveries in `retry_scheduled`.
    pub retry_backlog: Gauge<i64>,
    /// How overdue the most overdue retry is, seconds (scheduler health).
    pub retry_oldest_overdue: Gauge<f64>,
    /// Deliveries in `dead`.
    pub dead_deliveries: Gauge<i64>,
    /// Deliveries in `pending`.
    pub pending_deliveries: Gauge<i64>,
    /// Messages published by the relay.
    pub relay_published: Counter<u64>,
}

/// Latency buckets in seconds: fine-grained below a second, then up to the
/// retry horizon so retried deliveries still land in a bucket.
const LATENCY_BUCKETS: [f64; 18] = [
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0, 1800.0,
    7200.0, 36000.0, 108000.0,
];

pub fn get() -> &'static Metrics {
    static METRICS: OnceLock<Metrics> = OnceLock::new();
    METRICS.get_or_init(|| {
        let m = global::meter("hooklane");
        Metrics {
            delivery_latency: m
                .f64_histogram("hooklane.delivery.latency")
                .with_unit("s")
                .with_description("Event created to successful delivery")
                .with_boundaries(LATENCY_BUCKETS.to_vec())
                .build(),
            deliveries: m
                .u64_counter("hooklane.deliveries")
                .with_description("Delivery outcomes")
                .build(),
            attempts: m.u64_counter("hooklane.attempts").build(),
            events_ingested: m.u64_counter("hooklane.events.ingested").build(),
            api_latency: m
                .f64_histogram("hooklane.api.request.duration")
                .with_unit("s")
                .with_boundaries(LATENCY_BUCKETS[..12].to_vec())
                .build(),
            attempt_duration: m
                .f64_histogram("hooklane.attempt.duration")
                .with_unit("s")
                .with_boundaries(LATENCY_BUCKETS[..12].to_vec())
                .build(),
            consumer_lag: m.i64_gauge("hooklane.kafka.consumer_lag").build(),
            open_breakers: m.i64_gauge("hooklane.breakers.open").build(),
            outbox_oldest_age: m
                .f64_gauge("hooklane.outbox.oldest_unpublished_age")
                .with_unit("s")
                .build(),
            retry_backlog: m.i64_gauge("hooklane.retry.backlog").build(),
            retry_oldest_overdue: m
                .f64_gauge("hooklane.retry.oldest_overdue")
                .with_unit("s")
                .build(),
            dead_deliveries: m.i64_gauge("hooklane.deliveries.dead").build(),
            pending_deliveries: m.i64_gauge("hooklane.deliveries.pending").build(),
            relay_published: m.u64_counter("hooklane.relay.published").build(),
        }
    })
}

pub fn kv(key: &'static str, value: impl Into<opentelemetry::Value>) -> KeyValue {
    KeyValue::new(key, value)
}

pub fn status_class(status: u16) -> &'static str {
    match status {
        200..=299 => "2xx",
        300..=399 => "3xx",
        400..=499 => "4xx",
        500..=599 => "5xx",
        _ => "other",
    }
}
