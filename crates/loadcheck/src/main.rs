//! loadcheck: verify a finished load run.
//!
//! Compares what Postgres says should have been delivered with what the sink
//! (the fake customer) actually received, and prints a Markdown report:
//!
//! * **lost** (must be 0): events the load generator got a 202 for that are
//!   not in Postgres, plus deliveries to healthy endpoints the sink never
//!   received with a 2xx;
//! * **duplicates**: extra requests the sink received for a webhook-id on a
//!   healthy endpoint (each healthy endpoint should see each id once);
//! * **latency**: event created -> delivery recorded as succeeded, p50/p95/p99,
//!   for first-attempt deliveries to healthy endpoints;
//! * **SLO**: 99% of those within 60 seconds;
//! * **throughput**: peak and sustained events/second (ingest) and
//!   deliveries/second.
//!
//! The report goes to stdout; `loadtest/run.sh` saves it under docs/results/.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use anyhow::Context;
use chrono::{DateTime, Utc};
use clap::Parser;
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,
    #[arg(long, env = "SINK_URL", default_value = "http://sink:8080")]
    sink_url: String,
    #[arg(long)]
    tenant_id: Uuid,
    /// Number of events the load generator got a 202 for.
    #[arg(long)]
    accepted: i64,
    /// Endpoint URLs containing this are "healthy" (must receive everything).
    #[arg(long, default_value = "/ok/")]
    healthy_marker: String,
    /// Wait at most this long for in-flight deliveries to finish.
    #[arg(long, default_value_t = 600)]
    drain_timeout_secs: u64,
    #[arg(long, default_value = "steady")]
    test: String,
    #[arg(long, default_value = "kafka")]
    queue_mode: String,
    #[arg(long, default_value_t = 1)]
    workers: u32,
    #[arg(long, default_value = "unknown")]
    machine: String,
    /// Free text added to the report (what was done during the run).
    #[arg(long, default_value = "")]
    notes: String,
    /// Extra settings to record (k6 target rate etc.).
    #[arg(long, default_value = "")]
    settings: String,
}

#[derive(Debug, Default, Clone, serde::Deserialize)]
struct SinkRecord {
    path: String,
    id: String,
    count: u32,
    ok: u32,
}

#[derive(Debug, sqlx::FromRow)]
struct HealthyDelivery {
    event_id: Uuid,
    url: String,
    status: String,
    attempt_count: i32,
    event_created_at: DateTime<Utc>,
    completed_at: Option<DateTime<Utc>>,
}

fn url_path(url: &str) -> String {
    let after_scheme = url.split("://").nth(1).unwrap_or(url);
    let path = after_scheme
        .find('/')
        .map(|i| &after_scheme[i..])
        .unwrap_or("/");
    path.split('?').next().unwrap_or(path).to_string()
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let rank = ((p / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[rank.min(sorted.len() - 1)]
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let pool = PgPool::connect(&args.database_url)
        .await
        .context("connect to postgres")?;
    let tenant = args.tenant_id;

    // 1. Wait for the system to drain: nothing pending/delivering, and nothing
    //    waiting to retry on a healthy endpoint.
    let drain_start = Instant::now();
    let mut drained = false;
    while drain_start.elapsed() < Duration::from_secs(args.drain_timeout_secs) {
        let in_flight: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM deliveries d JOIN endpoints e ON e.id = d.endpoint_id
              WHERE d.tenant_id = $1 AND (d.status IN ('pending','delivering')
                    OR (d.status = 'retry_scheduled' AND position($2 in e.url) > 0))",
        )
        .bind(tenant)
        .bind(&args.healthy_marker)
        .fetch_one(&pool)
        .await?;
        if in_flight == 0 {
            drained = true;
            break;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    let drain_secs = drain_start.elapsed().as_secs_f64();

    // 2. What Postgres knows.
    let events_in_db: i64 = sqlx::query_scalar("SELECT count(*) FROM events WHERE tenant_id = $1")
        .bind(tenant)
        .fetch_one(&pool)
        .await?;
    let status_counts: Vec<(String, i64)> =
        sqlx::query_as("SELECT status, count(*) FROM deliveries WHERE tenant_id = $1 GROUP BY status ORDER BY status")
            .bind(tenant)
            .fetch_all(&pool)
            .await?;
    let healthy: Vec<HealthyDelivery> = sqlx::query_as(
        "SELECT d.event_id, e.url, d.status, d.attempt_count, ev.created_at AS event_created_at, d.completed_at
           FROM deliveries d JOIN endpoints e ON e.id = d.endpoint_id JOIN events ev ON ev.id = d.event_id
          WHERE d.tenant_id = $1 AND position($2 in e.url) > 0",
    )
    .bind(tenant)
    .bind(&args.healthy_marker)
    .fetch_all(&pool)
    .await?;
    // Ingest throughput: events per second, by creation time.
    let ingest_per_sec: Vec<i64> = sqlx::query_scalar(
        "SELECT count(*) FROM events WHERE tenant_id = $1 GROUP BY date_trunc('second', created_at)",
    )
    .bind(tenant)
    .fetch_all(&pool)
    .await?;
    let (first_event, last_event): (Option<DateTime<Utc>>, Option<DateTime<Utc>>) =
        sqlx::query_as("SELECT min(created_at), max(created_at) FROM events WHERE tenant_id = $1")
            .bind(tenant)
            .fetch_one(&pool)
            .await?;
    let delivered_per_sec: Vec<i64> = sqlx::query_scalar(
        "SELECT count(*) FROM deliveries WHERE tenant_id = $1 AND status = 'succeeded'
          GROUP BY date_trunc('second', completed_at)",
    )
    .bind(tenant)
    .fetch_all(&pool)
    .await?;
    let (first_done, last_done): (Option<DateTime<Utc>>, Option<DateTime<Utc>>) = sqlx::query_as(
        "SELECT min(completed_at), max(completed_at) FROM deliveries WHERE tenant_id = $1 AND status = 'succeeded'",
    )
    .bind(tenant)
    .fetch_one(&pool)
    .await?;

    // 3. What the customer (sink) actually received.
    let export = reqwest::get(format!("{}/_admin/export", args.sink_url))
        .await
        .context("fetch sink export")?
        .text()
        .await?;
    let mut sink: HashMap<(String, String), SinkRecord> = HashMap::new();
    for line in export.lines().filter(|l| !l.is_empty()) {
        let r: SinkRecord = serde_json::from_str(line)?;
        sink.insert((r.path.clone(), r.id.clone()), r);
    }

    // 4. Lost and duplicates.
    let ingest_lost = (args.accepted - events_in_db).max(0);
    let mut delivery_lost = 0i64;
    let mut duplicates = 0i64;
    let mut not_succeeded_in_db = 0i64;
    let mut latencies: Vec<f64> = Vec::new();
    let mut within_slo = 0usize;
    for d in &healthy {
        let key = (url_path(&d.url), d.event_id.to_string());
        match sink.get(&key) {
            Some(r) if r.ok >= 1 => {
                duplicates += i64::from(r.count.saturating_sub(1));
            }
            _ => delivery_lost += 1,
        }
        if d.status != "succeeded" {
            not_succeeded_in_db += 1;
        }
        if d.status == "succeeded"
            && d.attempt_count == 1
            && let Some(done) = d.completed_at
        {
            let secs = (done - d.event_created_at).num_milliseconds() as f64 / 1000.0;
            if secs <= 60.0 {
                within_slo += 1;
            }
            latencies.push(secs);
        }
    }
    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let lost = ingest_lost + delivery_lost;
    let slo_pct = if latencies.is_empty() {
        0.0
    } else {
        100.0 * within_slo as f64 / latencies.len() as f64
    };
    let span_secs = |a: Option<DateTime<Utc>>, b: Option<DateTime<Utc>>| match (a, b) {
        (Some(a), Some(b)) => ((b - a).num_milliseconds() as f64 / 1000.0).max(1.0),
        _ => 1.0,
    };
    let sustained_ingest = events_in_db as f64 / span_secs(first_event, last_event);
    let total_succeeded: i64 = status_counts
        .iter()
        .filter(|(s, _)| s == "succeeded")
        .map(|(_, n)| *n)
        .sum();
    let sustained_delivery = total_succeeded as f64 / span_secs(first_done, last_done);
    let peak_ingest = ingest_per_sec.iter().max().copied().unwrap_or(0);
    let peak_delivery = delivered_per_sec.iter().max().copied().unwrap_or(0);

    // 5. Report.
    let now = Utc::now();
    let mut out = String::new();
    out.push_str(&format!("# Load test report: {}\n\n", args.test));
    out.push_str(
        "> Measured on a local Docker setup (Colima VM on a laptop). Numbers are for comparison\n",
    );
    out.push_str("> between runs and designs, not a statement of production capacity.\n\n");
    out.push_str("| | |\n|---|---|\n");
    out.push_str(&format!(
        "| Date | {} |\n",
        now.format("%Y-%m-%d %H:%M UTC")
    ));
    out.push_str(&format!("| Machine | {} |\n", args.machine));
    out.push_str(&format!("| Queue mode | {} |\n", args.queue_mode));
    out.push_str(&format!("| Workers | {} |\n", args.workers));
    out.push_str(&format!(
        "| Test size | {} events accepted (HTTP 202) |\n",
        args.accepted
    ));
    if !args.settings.is_empty() {
        out.push_str(&format!("| Settings | {} |\n", args.settings));
    }
    out.push_str(&format!(
        "| Drained | {} (waited {:.0} s after the load ended) |\n\n",
        if drained { "yes" } else { "NO (timeout)" },
        drain_secs
    ));

    out.push_str("## Result\n\n| Metric | Value |\n|---|---|\n");
    out.push_str(&format!("| **Lost** | **{lost}** ({ingest_lost} at ingest, {delivery_lost} healthy deliveries never received) |\n"));
    out.push_str(&format!(
        "| **Duplicates** (extra requests to healthy endpoints) | **{duplicates}** |\n"
    ));
    out.push_str(&format!("| Events in Postgres | {events_in_db} |\n"));
    out.push_str(&format!(
        "| Healthy-endpoint deliveries | {} ({} not `succeeded` in Postgres) |\n",
        healthy.len(),
        not_succeeded_in_db
    ));
    out.push_str(&format!(
        "| Latency p50 / p95 / p99 (first attempt, healthy) | {:.3} s / {:.3} s / {:.3} s |\n",
        percentile(&latencies, 50.0),
        percentile(&latencies, 95.0),
        percentile(&latencies, 99.0)
    ));
    out.push_str(&format!(
        "| Latency max | {:.3} s |\n",
        latencies.last().copied().unwrap_or(f64::NAN)
    ));
    out.push_str(&format!(
        "| SLO (99% within 60 s) | {} ({:.2}% within 60 s) |\n",
        if slo_pct >= 99.0 {
            "**met**"
        } else {
            "**missed**"
        },
        slo_pct
    ));
    out.push_str(&format!(
        "| Ingest events/s: peak (1 s) / sustained | {peak_ingest} / {sustained_ingest:.0} |\n"
    ));
    out.push_str(&format!(
        "| Deliveries/s: peak (1 s) / sustained | {peak_delivery} / {sustained_delivery:.0} |\n\n"
    ));

    out.push_str("## Deliveries by status (all endpoints)\n\n| Status | Count |\n|---|---|\n");
    for (s, n) in &status_counts {
        out.push_str(&format!("| {s} | {n} |\n"));
    }
    if !args.notes.is_empty() {
        out.push_str(&format!("\n## Notes\n\n{}\n", args.notes));
    }
    println!("{out}");
    // Machine-readable line for scripts (load-compare).
    eprintln!(
        "LOADCHECK_JSON {}",
        serde_json::json!({
            "test": args.test, "mode": args.queue_mode, "lost": lost, "duplicates": duplicates,
            "p50": percentile(&latencies, 50.0), "p95": percentile(&latencies, 95.0), "p99": percentile(&latencies, 99.0),
            "slo_pct": slo_pct, "peak_ingest": peak_ingest, "sustained_ingest": sustained_ingest,
            "peak_delivery": peak_delivery, "sustained_delivery": sustained_delivery, "drained": drained,
        })
    );
    if lost > 0 {
        std::process::exit(2);
    }
    Ok(())
}
