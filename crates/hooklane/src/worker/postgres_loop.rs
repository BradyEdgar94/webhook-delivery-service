//! The Postgres-mode worker loop: claim a batch of `pending` rows, hand them
//! to per-endpoint lanes, repeat. Concurrency per worker is capped by
//! `PG_QUEUE_CONCURRENCY`.
//!
//! Rows are claimed (status `delivering`) the moment they are fetched, so a
//! row waiting in a lane is already "owned" by this worker. To keep that wait
//! short, a lane that already holds `PG_LANE_MAX` rows gets no more: the excess
//! is parked a few seconds ahead with its attempt given back.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::WorkerCtx;
use super::kafka_loop::park_with_retry;
use super::lanes::{Job, Lanes};
use crate::db::status;
use crate::queue::postgres;

/// Max claimed rows waiting in one endpoint's lane in Postgres mode.
const PG_LANE_MAX: usize = 10;

pub async fn run(ctx: Arc<WorkerCtx>, shutdown: CancellationToken) -> anyhow::Result<()> {
    let config = ctx.config.clone();
    let stopping = CancellationToken::new();
    let mut lanes = Lanes::new(ctx.clone(), stopping.clone());
    let (done_tx, mut done_rx) = mpsc::unbounded_channel::<()>();
    let cap = config.pg_queue_concurrency.max(1);
    let mut in_flight: usize = 0;
    let mut error_backoff = Duration::from_millis(250);
    let mut housekeeping = tokio::time::interval(Duration::from_secs(15));
    tracing::info!(concurrency = cap, "worker polling postgres queue");

    loop {
        while done_rx.try_recv().is_ok() {
            in_flight -= 1;
        }
        if shutdown.is_cancelled() {
            break;
        }
        let room = cap - in_flight;
        if room == 0 {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                Some(()) = done_rx.recv() => in_flight -= 1,
            }
            continue;
        }

        let batch = (room as i64).min(config.pg_queue_batch_size);
        match postgres::claim(&ctx.pool, batch).await {
            Err(e) => {
                tracing::warn!(error = %e, "claim failed, retrying");
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    _ = tokio::time::sleep(error_backoff) => {}
                }
                error_backoff = (error_backoff * 2).min(Duration::from_secs(5));
            }
            Ok(rows) if rows.is_empty() => {
                error_backoff = Duration::from_millis(250);
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    Some(()) = done_rx.recv() => in_flight -= 1,
                    _ = tokio::time::sleep(Duration::from_millis(100)) => {}
                    _ = housekeeping.tick() => {
                        lanes.remove_idle();
                        ctx.guards.expire_idle();
                    }
                }
            }
            Ok(rows) => {
                error_backoff = Duration::from_millis(250);
                for (msg, pre) in rows {
                    in_flight += 1;
                    let tx = done_tx.clone();
                    if lanes.queued(msg.endpoint_id) >= PG_LANE_MAX {
                        let ctx = ctx.clone();
                        tokio::spawn(async move {
                            park_with_retry(&ctx, &msg, status::DELIVERING).await;
                            let _ = tx.send(());
                        });
                        continue;
                    }
                    let on_done = Box::new(move |_| {
                        let _ = tx.send(());
                    });
                    lanes.dispatch(
                        0,
                        Job {
                            msg,
                            pre_claimed: Some(pre),
                            on_done,
                        },
                    );
                }
            }
        }
    }

    tracing::info!(in_flight, "draining in-flight deliveries");
    let deadline = tokio::time::Instant::now() + config.shutdown_grace();
    while in_flight > 0 {
        tokio::select! {
            Some(()) = done_rx.recv() => in_flight -= 1,
            _ = tokio::time::sleep_until(deadline) => {
                tracing::warn!(in_flight, "grace period over; the reaper will re-queue unfinished deliveries");
                break;
            }
        }
    }
    stopping.cancel();
    tracing::info!("worker stopped");
    Ok(())
}
