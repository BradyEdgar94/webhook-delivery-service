//! Integration-test harness.
//!
//! Each test gets a completely isolated system:
//! * its own Postgres **database** (created on the test server, migrated);
//! * in Kafka mode, its own **topic and consumer group**;
//! * an in-process **sink** on a random port;
//! * the API, relay and scheduler as tasks on the test runtime;
//! * each worker in **its own Tokio runtime**, so a test can kill a worker
//!   abruptly (dropping the runtime stops every task instantly, mid-request),
//!   which is a realistic crash.
//!
//! Needs `TEST_DATABASE_URL` (a database on the server, used to create the
//! per-test databases) and `TEST_KAFKA_BROKERS`. `make test-integration` sets
//! both and starts Postgres and Redpanda.

#![allow(dead_code)]

use std::future::Future;
use std::sync::{Arc, Once};
use std::time::{Duration, Instant};

use hooklane::config::{Config, QueueMode};
use hooklane::{admin, api, crypto, db, relay, scheduler, worker};
use serde_json::{Value, json};
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub use hooklane::db::deliveries::DeliveryRow;

pub const MODES: [QueueMode; 2] = [QueueMode::Postgres, QueueMode::Kafka];

fn init_logging() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let filter = std::env::var("RUST_LOG").unwrap_or_else(|_| "warn".to_string());
        let _ = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_test_writer()
            .try_init();
    });
}

fn admin_db_url() -> String {
    std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://hooklane:hooklane@localhost:55432/postgres".to_string())
}

fn kafka_brokers() -> String {
    std::env::var("TEST_KAFKA_BROKERS").unwrap_or_else(|_| "localhost:19092".to_string())
}

/// A function that adjusts the test config before startup.
pub type ConfigTweak = Box<dyn FnOnce(&mut Config)>;

pub struct Options {
    pub mode: QueueMode,
    pub workers: usize,
    pub partitions: i32,
    pub run_relay: bool,
    pub allow_private: bool,
    pub tweak: Option<ConfigTweak>,
}

impl Options {
    pub fn new(mode: QueueMode) -> Options {
        Options {
            mode,
            workers: 1,
            partitions: 4,
            run_relay: true,
            allow_private: true,
            tweak: None,
        }
    }
    pub fn workers(mut self, n: usize) -> Self {
        self.workers = n;
        self
    }
    pub fn partitions(mut self, n: i32) -> Self {
        self.partitions = n;
        self
    }
    pub fn no_relay(mut self) -> Self {
        self.run_relay = false;
        self
    }
    pub fn strict_ssrf(mut self) -> Self {
        self.allow_private = false;
        self
    }
    pub fn config(mut self, f: impl FnOnce(&mut Config) + 'static) -> Self {
        self.tweak = Some(Box::new(f));
        self
    }
}

/// A worker running on its own runtime.
pub struct WorkerHandle {
    runtime: Option<tokio::runtime::Runtime>,
    shutdown: CancellationToken,
}

impl WorkerHandle {
    /// Simulate a crash: every task of this worker stops immediately.
    pub fn kill(mut self) {
        if let Some(rt) = self.runtime.take() {
            rt.shutdown_background();
        }
    }
}

impl Drop for WorkerHandle {
    fn drop(&mut self) {
        self.shutdown.cancel();
        if let Some(rt) = self.runtime.take() {
            rt.shutdown_background();
        }
    }
}

pub struct TestEnv {
    pub mode: QueueMode,
    pub config: Arc<Config>,
    pub pool: PgPool,
    pub api_url: String,
    pub api_key: String,
    pub tenant_id: Uuid,
    pub sink: sink::Sink,
    pub sink_url: String,
    pub http: reqwest::Client,
    pub workers: Vec<WorkerHandle>,
    shutdown: CancellationToken,
    db_name: String,
}

impl TestEnv {
    pub async fn start(mode: QueueMode) -> TestEnv {
        TestEnv::with(Options::new(mode)).await
    }

    pub async fn with(opts: Options) -> TestEnv {
        init_logging();
        let id = Uuid::now_v7().simple().to_string();
        let db_name = format!("hl_test_{}", &id[id.len() - 16..]);

        // A fresh database for this test.
        let admin_url = admin_db_url();
        let admin_pool = PgPool::connect(&admin_url)
            .await
            .expect("connect to TEST_DATABASE_URL");
        sqlx::query(sqlx::AssertSqlSafe(format!("CREATE DATABASE {db_name}")))
            .execute(&admin_pool)
            .await
            .expect("create test database");
        admin_pool.close().await;
        let mut url = url::Url::parse(&admin_url).expect("valid TEST_DATABASE_URL");
        url.set_path(&db_name);

        let mut config = Config::defaults();
        config.database_url = url.to_string();
        config.database_max_connections = 10;
        config.queue_mode = opts.mode;
        config.kafka_brokers = kafka_brokers();
        config.kafka_topic = format!("hl-test-{id}");
        config.kafka_consumer_group = format!("hl-test-{id}");
        config.kafka_partitions = opts.partitions;
        config.secret_encryption_key = crypto::generate_encryption_key();
        config.allow_http = true;
        config.allow_private_endpoints = opts.allow_private;
        config.delivery_timeout_ms = 2_000;
        config.retry_schedule_ms = vec![50; 7];
        config.scheduler_retry_interval_ms = 50;
        config.reaper_interval_ms = 250;
        config.reaper_stuck_after_ms = 3_000;
        config.breaker_base_cooldown_ms = 500;
        config.breaker_max_cooldown_ms = 2_000;
        config.relay_idle_sleep_ms = 10;
        config.shutdown_grace_ms = 2_000;
        config.otlp_endpoint = String::new();
        if let Some(tweak) = opts.tweak {
            tweak(&mut config);
        }
        let config = Arc::new(config);

        let pool = db::connect_with_retry(&config.database_url, 10).await;
        admin::migrate(&config, &pool, 1)
            .await
            .expect("migrate (and create topic)");
        let (tenant_id, api_key) = admin::create_tenant(&pool, "test-tenant")
            .await
            .expect("tenant");

        let shutdown = CancellationToken::new();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind api");
        let api_url = format!("http://{}", listener.local_addr().expect("addr"));
        tokio::spawn(api::serve(
            config.clone(),
            pool.clone(),
            listener,
            shutdown.clone(),
        ));
        if opts.mode == QueueMode::Kafka && opts.run_relay {
            tokio::spawn(relay::run(config.clone(), pool.clone(), shutdown.clone()));
        }
        tokio::spawn(scheduler::run(
            config.clone(),
            pool.clone(),
            shutdown.clone(),
        ));

        let (sink, sink_addr) = sink::spawn("127.0.0.1:0").await.expect("sink");
        let mut env = TestEnv {
            mode: opts.mode,
            config,
            pool,
            api_url,
            api_key,
            tenant_id,
            sink,
            sink_url: format!("http://{sink_addr}"),
            http: reqwest::Client::new(),
            workers: Vec::new(),
            shutdown,
            db_name,
        };
        for _ in 0..opts.workers {
            env.add_worker();
        }
        env
    }

    /// Start one more worker on its own runtime (with its own DB pool and
    /// HTTP client, so nothing it owns outlives a kill).
    pub fn add_worker(&mut self) {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("worker runtime");
        let token = CancellationToken::new();
        let config = self.config.clone();
        let t = token.clone();
        runtime.spawn(async move {
            let pool = db::connect_with_retry(&config.database_url, 10).await;
            if let Err(e) = worker::run(config, pool, t).await {
                tracing::error!(error = %e, "test worker failed");
            }
        });
        self.workers.push(WorkerHandle {
            runtime: Some(runtime),
            shutdown: token,
        });
    }

    /// Stop everything and drop the test database.
    pub async fn finish(mut self) {
        self.shutdown.cancel();
        for w in self.workers.drain(..) {
            w.kill();
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        self.pool.close().await;
        if let Ok(admin) = PgPool::connect(&admin_db_url()).await {
            let _ = sqlx::query(sqlx::AssertSqlSafe(format!(
                "DROP DATABASE IF EXISTS {} WITH (FORCE)",
                self.db_name
            )))
            .execute(&admin)
            .await;
        }
    }

    // ---- API helpers ------------------------------------------------------

    pub fn url(&self, path: &str) -> String {
        format!("{}{}", self.api_url, path)
    }

    pub async fn api(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<Value>,
    ) -> (u16, Value) {
        self.api_as(&self.api_key, method, path, body).await
    }

    pub async fn api_as(
        &self,
        key: &str,
        method: reqwest::Method,
        path: &str,
        body: Option<Value>,
    ) -> (u16, Value) {
        let mut req = self.http.request(method, self.url(path)).bearer_auth(key);
        if let Some(b) = body {
            req = req.json(&b);
        }
        let resp = req.send().await.expect("api request");
        let status = resp.status().as_u16();
        let text = resp.text().await.unwrap_or_default();
        (
            status,
            serde_json::from_str(&text).unwrap_or(Value::String(text)),
        )
    }

    /// Create an endpoint pointing at a sink path; registers its secret with
    /// the sink so signatures are verified. Returns (endpoint id, secret).
    pub async fn create_endpoint(
        &self,
        sink_path: &str,
        event_types: Option<Vec<&str>>,
    ) -> (Uuid, String) {
        let (status, v) = self
            .api(
                reqwest::Method::POST,
                "/v1/endpoints",
                Some(json!({ "url": format!("{}{}", self.sink_url, sink_path), "event_types": event_types })),
            )
            .await;
        assert_eq!(status, 201, "create endpoint: {v}");
        let secret = v["secret"].as_str().expect("secret").to_string();
        self.sink.register_secret(sink_path, &secret);
        (v["id"].as_str().expect("id").parse().expect("uuid"), secret)
    }

    pub async fn send_event(&self, event_type: &str, data: Value) -> Uuid {
        let (status, v) = self
            .api(
                reqwest::Method::POST,
                "/v1/events",
                Some(json!({ "type": event_type, "data": data })),
            )
            .await;
        assert_eq!(status, 202, "send event: {v}");
        v["id"].as_str().expect("id").parse().expect("uuid")
    }

    // ---- database helpers -------------------------------------------------

    pub async fn delivery(&self, event_id: Uuid, endpoint_id: Uuid) -> Option<DeliveryRow> {
        sqlx::query_as::<_, DeliveryRow>(
            "SELECT id, event_id, endpoint_id, tenant_id, status, attempt_count, next_attempt_at, claimed_at,
                    last_error, completed_at, created_at, updated_at
               FROM deliveries WHERE event_id = $1 AND endpoint_id = $2",
        )
        .bind(event_id)
        .bind(endpoint_id)
        .fetch_optional(&self.pool)
        .await
        .expect("query delivery")
    }

    pub async fn wait_status(
        &self,
        event_id: Uuid,
        endpoint_id: Uuid,
        status: &str,
        timeout: Duration,
    ) -> DeliveryRow {
        let deadline = Instant::now() + timeout;
        loop {
            let last = self.delivery(event_id, endpoint_id).await;
            if let Some(d) = &last
                && d.status == status
            {
                return d.clone();
            }
            assert!(
                Instant::now() < deadline,
                "delivery never reached '{status}'; last state: {last:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    pub async fn attempts(&self, delivery_id: i64) -> Vec<hooklane::db::attempts::AttemptRow> {
        hooklane::db::attempts::for_delivery(&self.pool, delivery_id)
            .await
            .expect("attempts")
    }

    pub async fn count(&self, sql: &'static str) -> i64 {
        sqlx::query_scalar(sql)
            .fetch_one(&self.pool)
            .await
            .expect("count")
    }

    /// How many requests the sink got for this webhook-id on this path.
    pub fn sink_count(&self, path: &str, event_id: Uuid) -> u32 {
        self.sink
            .ids(path)
            .get(&event_id.to_string())
            .map_or(0, |r| r.count)
    }
}

/// Poll `check` every 50 ms until it returns true or `timeout` passes.
pub async fn wait_until<F, Fut>(timeout: Duration, mut check: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if check().await {
            return true;
        }
        if Instant::now() > deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

pub const T: Duration = Duration::from_secs(45);
