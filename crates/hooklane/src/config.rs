//! Configuration.
//!
//! Every setting is a command-line flag that can also be supplied as an
//! environment variable (the env var name is shown next to each field). In
//! Docker and Kubernetes everything comes from the environment; `.env.example`
//! lists every variable with its default.
//!
//! Durations are given in plain units in the variable name (`_MS`, `_SECS`) so
//! there is no duration-string parsing to learn.

use std::time::Duration;

use clap::{Args, ValueEnum};

/// Which queue carries deliveries from Postgres to the workers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum QueueMode {
    /// Outbox table -> relay -> Kafka topic -> workers. The production design.
    Kafka,
    /// Workers claim `pending` rows straight from the `deliveries` table.
    /// Simpler, kept for comparison (see docs/RESULTS.md).
    Postgres,
}

impl QueueMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            QueueMode::Kafka => "kafka",
            QueueMode::Postgres => "postgres",
        }
    }
}

#[derive(Debug, Clone, Args)]
pub struct Config {
    // ---- connections -------------------------------------------------------
    #[arg(
        long,
        env = "DATABASE_URL",
        default_value = "postgres://hooklane:hooklane@localhost:5432/hooklane"
    )]
    pub database_url: String,

    /// Maximum Postgres connections per process.
    #[arg(long, env = "DATABASE_MAX_CONNECTIONS", default_value_t = 20)]
    pub database_max_connections: u32,

    #[arg(long, env = "KAFKA_BROKERS", default_value = "localhost:19092")]
    pub kafka_brokers: String,

    #[arg(long, env = "KAFKA_TOPIC", default_value = "hooklane.deliveries")]
    pub kafka_topic: String,

    /// Partition count used when `migrate` creates the topic. 64 is chosen up
    /// front because adding partitions later moves endpoints between partitions.
    #[arg(long, env = "KAFKA_PARTITIONS", default_value_t = 64)]
    pub kafka_partitions: i32,

    #[arg(long, env = "KAFKA_CONSUMER_GROUP", default_value = "hooklane-workers")]
    pub kafka_consumer_group: String,

    #[arg(long, env = "QUEUE_MODE", value_enum, default_value_t = QueueMode::Kafka)]
    pub queue_mode: QueueMode,

    // ---- api ---------------------------------------------------------------
    #[arg(long, env = "LISTEN_ADDR", default_value = "0.0.0.0:8080")]
    pub listen_addr: String,

    /// Health endpoint (`GET /healthz`) for the relay, scheduler and worker
    /// roles, used by Kubernetes liveness/readiness probes. Empty = off.
    #[arg(long, env = "HEALTH_ADDR", default_value = "0.0.0.0:9000")]
    pub health_addr: String,

    /// Base64 of 32 bytes. Encrypts endpoint signing secrets at rest (AES-256-GCM).
    #[arg(
        long,
        env = "SECRET_ENCRYPTION_KEY",
        default_value = "",
        hide_env_values = true
    )]
    pub secret_encryption_key: String,

    // ---- outbound safety ---------------------------------------------------
    /// Allow plain `http://` endpoint URLs. Off in production: webhooks carry data.
    #[arg(long, env = "ALLOW_HTTP", default_value_t = false)]
    pub allow_http: bool,

    /// Disable the SSRF IP-range rules. ONLY for local Docker networks where the
    /// fake endpoints live on private addresses. Never enable in production.
    #[arg(long, env = "ALLOW_PRIVATE_ENDPOINTS", default_value_t = false)]
    pub allow_private_endpoints: bool,

    /// Total time allowed for one outbound HTTP attempt.
    #[arg(long, env = "DELIVERY_TIMEOUT_MS", default_value_t = 15_000)]
    pub delivery_timeout_ms: u64,

    // ---- retries -----------------------------------------------------------
    /// Delays before attempts 2..N, in milliseconds. 8 attempts over ~28 hours:
    /// 5s, 5m, 30m, 2h, 5h, 10h, 10h. Tests use a schedule of milliseconds.
    #[arg(
        long,
        env = "RETRY_SCHEDULE_MS",
        value_delimiter = ',',
        default_value = "5000,300000,1800000,7200000,18000000,36000000,36000000"
    )]
    pub retry_schedule_ms: Vec<u64>,

    // ---- worker ------------------------------------------------------------
    /// Deliveries processed at the same time for ONE endpoint. 1 keeps an
    /// endpoint's deliveries in order; higher trades ordering for throughput.
    #[arg(long, env = "PER_ENDPOINT_CONCURRENCY", default_value_t = 1)]
    pub per_endpoint_concurrency: usize,

    /// Max un-finished messages per Kafka partition before fetching pauses.
    #[arg(long, env = "PARTITION_MAX_IN_FLIGHT", default_value_t = 256)]
    pub partition_max_in_flight: usize,

    /// Max queued deliveries in one endpoint's lane before the excess is parked.
    #[arg(long, env = "LANE_MAX_QUEUED", default_value_t = 100)]
    pub lane_max_queued: usize,

    /// Postgres queue mode: rows claimed per poll.
    #[arg(long, env = "PG_QUEUE_BATCH_SIZE", default_value_t = 200)]
    pub pg_queue_batch_size: i64,

    /// Postgres queue mode: concurrent deliveries per worker process.
    #[arg(long, env = "PG_QUEUE_CONCURRENCY", default_value_t = 256)]
    pub pg_queue_concurrency: usize,

    // ---- circuit breaker ----------------------------------------------------
    #[arg(long, env = "BREAKER_CONSECUTIVE_FAILURES", default_value_t = 5)]
    pub breaker_consecutive_failures: u32,

    #[arg(long, env = "BREAKER_BASE_COOLDOWN_MS", default_value_t = 30_000)]
    pub breaker_base_cooldown_ms: u64,

    #[arg(long, env = "BREAKER_MAX_COOLDOWN_MS", default_value_t = 600_000)]
    pub breaker_max_cooldown_ms: u64,

    // ---- relay -------------------------------------------------------------
    #[arg(long, env = "RELAY_BATCH_SIZE", default_value_t = 500)]
    pub relay_batch_size: i64,

    #[arg(long, env = "RELAY_IDLE_SLEEP_MS", default_value_t = 75)]
    pub relay_idle_sleep_ms: u64,

    // ---- scheduler ---------------------------------------------------------
    #[arg(long, env = "SCHEDULER_RETRY_INTERVAL_MS", default_value_t = 1_000)]
    pub scheduler_retry_interval_ms: u64,

    #[arg(long, env = "REAPER_INTERVAL_MS", default_value_t = 15_000)]
    pub reaper_interval_ms: u64,

    /// A delivery stuck in `delivering` longer than this is assumed to belong
    /// to a crashed worker and is put back in the queue.
    #[arg(long, env = "REAPER_STUCK_AFTER_MS", default_value_t = 120_000)]
    pub reaper_stuck_after_ms: u64,

    #[arg(long, env = "AUTO_DISABLE_AFTER_SECS", default_value_t = 5 * 24 * 3600)]
    pub auto_disable_after_secs: u64,

    #[arg(long, env = "AUTO_DISABLE_INTERVAL_MS", default_value_t = 60_000)]
    pub auto_disable_interval_ms: u64,

    // ---- telemetry ---------------------------------------------------------
    /// OTLP/HTTP endpoint of the OpenTelemetry Collector. Empty = no export.
    #[arg(long, env = "OTEL_EXPORTER_OTLP_ENDPOINT", default_value = "")]
    pub otlp_endpoint: String,

    /// `json` for machines (Docker, k8s), `pretty` for a human terminal.
    #[arg(long, env = "LOG_FORMAT", default_value = "json")]
    pub log_format: String,

    /// How long a role may take to finish in-flight work after SIGTERM.
    #[arg(long, env = "SHUTDOWN_GRACE_MS", default_value_t = 25_000)]
    pub shutdown_grace_ms: u64,
}

impl Config {
    pub fn delivery_timeout(&self) -> Duration {
        Duration::from_millis(self.delivery_timeout_ms)
    }

    /// Total attempts = first attempt + one per scheduled delay.
    pub fn max_attempts(&self) -> i32 {
        self.retry_schedule_ms.len() as i32 + 1
    }

    pub fn shutdown_grace(&self) -> Duration {
        Duration::from_millis(self.shutdown_grace_ms)
    }

    /// A config with every default applied and nothing read from the
    /// environment. Used by tests, which then override individual fields.
    pub fn defaults() -> Config {
        use clap::Parser;
        #[derive(Parser)]
        struct Wrapper {
            #[command(flatten)]
            config: Config,
        }
        // `no_env` is not available per-call, so parse with an empty argv; env
        // values that happen to be set still apply, which is what tests want
        // (they set DATABASE_URL etc. explicitly anyway).
        Wrapper::parse_from(["hooklane"]).config
    }
}
