//! The worker role: take deliveries off the queue, protect endpoints, sign,
//! POST, record the attempt.
//!
//! * `kafka_loop.rs` / `postgres_loop.rs` – where work comes from;
//! * `lanes.rs` – one in-memory lane per endpoint (no head-of-line blocking);
//! * `offsets.rs` – which Kafka offsets are safe to commit;
//! * `process.rs` – what happens to one delivery;
//! * `endpoints.rs` – endpoint cache and per-endpoint protection state.

pub mod endpoints;
pub mod kafka_loop;
pub mod lanes;
pub mod offsets;
pub mod postgres_loop;
pub mod process;

use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use crate::config::{Config, QueueMode};
use crate::crypto::SecretBox;
use crate::delivery::breaker::BreakerSettings;
use crate::delivery::{http, ssrf};
use crate::metrics;

/// Everything a delivery needs, shared by all lanes of one worker.
pub struct WorkerCtx {
    pub pool: PgPool,
    pub config: Arc<Config>,
    pub http: reqwest::Client,
    pub endpoints: endpoints::EndpointCache,
    pub guards: endpoints::Guards,
    pub policy: ssrf::Policy,
}

impl WorkerCtx {
    pub fn new(config: Arc<Config>, pool: PgPool) -> anyhow::Result<WorkerCtx> {
        let secrets = SecretBox::from_base64_key(&config.secret_encryption_key)?;
        let http = http::build_client(config.delivery_timeout(), config.allow_private_endpoints)?;
        let breaker = BreakerSettings {
            consecutive_failures: config.breaker_consecutive_failures,
            base_cooldown: Duration::from_millis(config.breaker_base_cooldown_ms),
            max_cooldown: Duration::from_millis(config.breaker_max_cooldown_ms),
        };
        Ok(WorkerCtx {
            endpoints: endpoints::EndpointCache::new(pool.clone(), secrets),
            guards: endpoints::Guards::new(breaker),
            policy: ssrf::Policy {
                allow_http: config.allow_http,
                allow_private: config.allow_private_endpoints,
            },
            pool,
            http,
            config,
        })
    }
}

pub async fn run(
    config: Arc<Config>,
    pool: PgPool,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let ctx = Arc::new(WorkerCtx::new(config.clone(), pool)?);

    // Export the number of open breakers every few seconds.
    let gauge_ctx = ctx.clone();
    let gauge_stop = shutdown.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(5));
        loop {
            tokio::select! {
                _ = gauge_stop.cancelled() => break,
                _ = tick.tick() => metrics::get().open_breakers.record(gauge_ctx.guards.open_breakers(), &[]),
            }
        }
    });

    match config.queue_mode {
        QueueMode::Kafka => kafka_loop::run(ctx, shutdown).await,
        QueueMode::Postgres => postgres_loop::run(ctx, shutdown).await,
    }
}
