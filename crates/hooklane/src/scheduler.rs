//! The scheduler: independent background loops that keep the system moving.
//!
//! * **Due retries** (every second): `retry_scheduled` rows whose
//!   `next_attempt_at` has passed go back to `pending` (plus an outbox row in
//!   Kafka mode, in the same transaction).
//! * **Reaper** (every 15 s): rows stuck in `delivering` for over 2 minutes
//!   belonged to a crashed worker; they go back to the queue.
//! * **Pending rescue** (Kafka mode, every minute): a safety net for a
//!   `pending` row whose Kafka message was somehow lost; re-publishes it after
//!   15 minutes. Duplicates this may create are skipped by the claim.
//! * **Auto-disable** (every minute): endpoints failing continuously for 5
//!   days are disabled, their waiting deliveries failed, and a system event sent.
//! * **Outbox cleanup**: published outbox rows older than an hour are deleted.
//! * **Gauges** (every 15 s): outbox age, retry backlog, dead count.
//!
//! Each loop locks the rows it works on with `FOR UPDATE SKIP LOCKED`, so two
//! scheduler replicas split the work instead of doing it twice.
//! The scheduler never talks to Kafka: retries reach Kafka via the outbox.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::config::{Config, QueueMode};
use crate::db;
use crate::metrics;

const BATCH: i64 = 1000;

pub async fn run(
    config: Arc<Config>,
    pool: PgPool,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    tracing::info!(queue_mode = config.queue_mode.as_str(), "scheduler started");
    let mut tasks = JoinSet::new();

    // Due retries: loop immediately while there is a full batch of work.
    spawn_loop(
        &mut tasks,
        "due_retries",
        config.scheduler_retry_interval_ms,
        &shutdown,
        {
            let (pool, config) = (pool.clone(), config.clone());
            move || {
                let (pool, config) = (pool.clone(), config.clone());
                async move {
                    loop {
                        let n =
                            db::deliveries::requeue_due_retries(&pool, BATCH, config.queue_mode)
                                .await?;
                        if n > 0 {
                            tracing::debug!(requeued = n, "due retries re-queued");
                        }
                        if n < BATCH as u64 {
                            return Ok(());
                        }
                    }
                }
            }
        },
    );

    spawn_loop(
        &mut tasks,
        "reaper",
        config.reaper_interval_ms,
        &shutdown,
        {
            let (pool, config) = (pool.clone(), config.clone());
            move || {
                let (pool, config) = (pool.clone(), config.clone());
                async move {
                    let stuck_secs = config.reaper_stuck_after_ms as f64 / 1000.0;
                    let n = db::deliveries::reap_stuck(
                        &pool,
                        stuck_secs,
                        BATCH,
                        config.max_attempts(),
                        config.queue_mode,
                    )
                    .await?;
                    if n > 0 {
                        tracing::warn!(
                            reaped = n,
                            "re-queued deliveries stuck in 'delivering' (worker lost)"
                        );
                    }
                    Ok(())
                }
            }
        },
    );

    if config.queue_mode == QueueMode::Kafka {
        spawn_loop(&mut tasks, "pending_rescue", 60_000, &shutdown, {
            let pool = pool.clone();
            move || {
                let pool = pool.clone();
                async move {
                    let n = db::deliveries::rescue_stale_pending(&pool, 15.0 * 60.0, BATCH).await?;
                    if n > 0 {
                        tracing::warn!(
                            rescued = n,
                            "re-published pending deliveries that were not picked up for 15 minutes"
                        );
                    }
                    Ok(())
                }
            }
        });
    }

    spawn_loop(
        &mut tasks,
        "auto_disable",
        config.auto_disable_interval_ms,
        &shutdown,
        {
            let (pool, config) = (pool.clone(), config.clone());
            move || {
                let (pool, config) = (pool.clone(), config.clone());
                async move { auto_disable(&pool, &config).await }
            }
        },
    );

    spawn_loop(&mut tasks, "outbox_cleanup", 60_000, &shutdown, {
        let pool = pool.clone();
        move || {
            let pool = pool.clone();
            async move {
                db::outbox::delete_published_older_than(&pool, 3600.0).await?;
                Ok(())
            }
        }
    });

    spawn_loop(&mut tasks, "gauges", 15_000, &shutdown, {
        let pool = pool.clone();
        move || {
            let pool = pool.clone();
            async move {
                let m = metrics::get();
                m.outbox_oldest_age
                    .record(db::outbox::oldest_unpublished_age_secs(&pool).await?, &[]);
                let b = db::deliveries::backlog(&pool).await?;
                m.retry_backlog.record(b.retry_scheduled, &[]);
                m.retry_oldest_overdue.record(b.oldest_overdue_secs, &[]);
                m.dead_deliveries.record(b.dead, &[]);
                m.pending_deliveries.record(b.pending, &[]);
                Ok(())
            }
        }
    });

    while tasks.join_next().await.is_some() {}
    tracing::info!("scheduler stopped");
    Ok(())
}

/// Run `tick` every `interval_ms` until shutdown. An error is logged and the
/// loop carries on (e.g. while Postgres restarts); it never kills the process.
fn spawn_loop<F, Fut>(
    tasks: &mut JoinSet<()>,
    name: &'static str,
    interval_ms: u64,
    shutdown: &CancellationToken,
    tick: F,
) where
    F: Fn() -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), sqlx::Error>> + Send,
{
    let shutdown = shutdown.clone();
    tasks.spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(interval_ms.max(10)));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = interval.tick() => {
                    if let Err(e) = tick().await {
                        tracing::warn!(task = name, error = %e, "scheduler task failed; will retry next tick");
                    }
                }
            }
        }
    });
}

/// Disable endpoints that have failed continuously for longer than the limit.
async fn auto_disable(pool: &PgPool, config: &Config) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    let ids = db::endpoints::lock_long_failing(&mut tx, config.auto_disable_after_secs as f64, 100)
        .await?;
    for id in &ids {
        let days = config.auto_disable_after_secs as f64 / 86_400.0;
        let reason = format!("failing continuously for more than {days:.1} days");
        db::endpoints::disable_in_tx(&mut tx, *id, &reason, config.queue_mode, false).await?;
        tracing::warn!(endpoint_id = %id, "endpoint auto-disabled");
    }
    tx.commit().await?;
    Ok(())
}
