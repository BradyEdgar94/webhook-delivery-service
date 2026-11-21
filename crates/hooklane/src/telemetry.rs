//! Logs, traces and metrics.
//!
//! We instrument with the `tracing` crate and export through **OpenTelemetry
//! over OTLP** to a Collector. The Collector decides where data goes
//! (Prometheus + Jaeger locally, or Datadog, etc.), so the application never
//! needs a vendor-specific SDK: one instrumentation, any backend.
//!
//! * **Logs** are structured JSON on stdout. Span fields such as `tenant_id`,
//!   `event_id`, `delivery_id` and `endpoint_id` are attached automatically to
//!   every log line inside that span. Payloads, secrets and API keys are never
//!   logged.
//! * **Traces** follow one event end to end. The W3C `traceparent` string is
//!   stored on the outbox row and copied into a Kafka message header, so the
//!   API request, relay publish, worker consume and outbound HTTP call are all
//!   one trace in Jaeger.
//! * **Metrics** live in `metrics.rs`.

use std::collections::HashMap;
use std::time::Duration;

use opentelemetry::propagation::TextMapPropagator;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry::{KeyValue, global};
use opentelemetry_otlp::{WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use crate::config::Config;

/// Keeps the exporters alive; call `shutdown` before exiting to flush them.
pub struct Telemetry {
    tracer_provider: Option<SdkTracerProvider>,
    meter_provider: Option<SdkMeterProvider>,
}

impl Telemetry {
    pub fn shutdown(self) {
        if let Some(tp) = self.tracer_provider {
            let _ = tp.shutdown();
        }
        if let Some(mp) = self.meter_provider {
            let _ = mp.shutdown();
        }
    }
}

/// Set up logging, and OTLP export if `OTEL_EXPORTER_OTLP_ENDPOINT` is set.
/// Call once, at startup, *before* starting the Tokio runtime (the OTLP HTTP
/// exporter uses a blocking client on its own thread).
pub fn init(config: &Config, role: &str) -> anyhow::Result<Telemetry> {
    global::set_text_map_propagator(TraceContextPropagator::new());

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new(
            "info,sqlx=warn,rdkafka=warn,hyper=warn,reqwest=warn,h2=warn,opentelemetry=warn",
        )
    });

    let service_name = format!("hooklane-{role}");
    let resource = Resource::builder()
        .with_service_name(service_name.clone())
        .with_attribute(KeyValue::new("hooklane.role", role.to_string()))
        // Unique per process. Without it, two worker replicas export series
        // with identical labels and the Collector merges them into one,
        // so counters and gauges would be wrong. (In Docker/k8s HOSTNAME is
        // the container/pod name.)
        .with_attribute(KeyValue::new(
            "service.instance.id",
            std::env::var("HOSTNAME").unwrap_or_else(|_| uuid::Uuid::now_v7().to_string()),
        ))
        .build();

    let mut tracer_provider = None;
    let mut meter_provider = None;
    let otel_layer = if config.otlp_endpoint.is_empty() {
        None
    } else {
        let base = config.otlp_endpoint.trim_end_matches('/');
        let span_exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_endpoint(format!("{base}/v1/traces"))
            .with_timeout(Duration::from_secs(5))
            .with_headers(HashMap::new())
            .build()?;
        let tp = SdkTracerProvider::builder()
            .with_batch_exporter(span_exporter)
            .with_resource(resource.clone())
            .build();
        let tracer = tp.tracer("hooklane");
        global::set_tracer_provider(tp.clone());
        tracer_provider = Some(tp);

        let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
            .with_http()
            .with_endpoint(format!("{base}/v1/metrics"))
            .with_timeout(Duration::from_secs(5))
            .build()?;
        let reader = PeriodicReader::builder(metric_exporter)
            .with_interval(Duration::from_secs(5))
            .build();
        let mp = SdkMeterProvider::builder()
            .with_reader(reader)
            .with_resource(resource)
            .build();
        global::set_meter_provider(mp.clone());
        meter_provider = Some(mp);

        Some(tracing_opentelemetry::layer().with_tracer(tracer))
    };

    let registry = tracing_subscriber::registry().with(filter).with(otel_layer);
    if config.log_format == "pretty" {
        registry
            .with(tracing_subscriber::fmt::layer().compact())
            .try_init()?;
    } else {
        registry
            .with(
                tracing_subscriber::fmt::layer()
                    .json()
                    .with_current_span(true)
                    .with_span_list(false)
                    .flatten_event(true),
            )
            .try_init()?;
    }

    Ok(Telemetry {
        tracer_provider,
        meter_provider,
    })
}

/// The W3C `traceparent` of the current span, e.g.
/// `00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01`.
pub fn current_traceparent() -> Option<String> {
    let cx = tracing::Span::current().context();
    let mut carrier: HashMap<String, String> = HashMap::new();
    TraceContextPropagator::new().inject_context(&cx, &mut carrier);
    carrier.remove("traceparent")
}

/// Make `span` a child of the trace described by `traceparent` (if any).
pub fn set_parent_from_traceparent(span: &tracing::Span, traceparent: Option<&str>) {
    let Some(tp) = traceparent else { return };
    let mut carrier: HashMap<String, String> = HashMap::new();
    carrier.insert("traceparent".to_string(), tp.to_string());
    let cx = TraceContextPropagator::new().extract(&carrier);
    let _ = span.set_parent(cx);
}
