//! Per-endpoint lanes: how one worker avoids head-of-line blocking.
//!
//! **The problem.** Many endpoints share a Kafka partition. If the worker took
//! a partition's messages strictly one at a time, one endpoint that takes 15
//! seconds to time out would delay every other endpoint behind it in the
//! partition: *head-of-line blocking*.
//!
//! **The fix.** Each endpoint gets its own little queue (a "lane") with its own
//! Tokio task. Messages are dispatched to their endpoint's lane, so different
//! endpoints run in parallel while each endpoint's own deliveries still run in
//! order. `PER_ENDPOINT_CONCURRENCY` (default 1) sets how many deliveries of
//! one endpoint may run at once: 1 preserves order; more trades ordering for
//! throughput to a single endpoint.
//!
//! A lane remembers which partition it belongs to so that when a partition is
//! revoked in a rebalance, its lanes can be stopped: queued jobs are dropped
//! (the new owner of the partition will receive them again from Kafka), and a
//! job already running is allowed to finish (the claim keeps that safe).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::{Semaphore, mpsc};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::WorkerCtx;
use super::process::{self, Outcome, PreClaimed};
use crate::queue::DeliveryMessage;

/// Called exactly once when a job is completely handled (not called for jobs
/// dropped because their partition was revoked or the worker is stopping).
pub type OnDone = Box<dyn FnOnce(Outcome) + Send>;

pub struct Job {
    pub msg: DeliveryMessage,
    pub pre_claimed: Option<PreClaimed>,
    pub on_done: OnDone,
}

struct Lane {
    tx: mpsc::UnboundedSender<Job>,
    /// Jobs queued or running in this lane.
    queued: Arc<AtomicUsize>,
    cancel: CancellationToken,
    partition: i32,
}

pub struct Lanes {
    ctx: Arc<WorkerCtx>,
    lanes: HashMap<Uuid, Lane>,
    /// Cancelled when the whole worker is stopping for good.
    stopping: CancellationToken,
}

impl Lanes {
    pub fn new(ctx: Arc<WorkerCtx>, stopping: CancellationToken) -> Lanes {
        Lanes {
            ctx,
            lanes: HashMap::new(),
            stopping,
        }
    }

    /// How many jobs the endpoint's lane holds (queued + running).
    pub fn queued(&self, endpoint_id: Uuid) -> usize {
        self.lanes
            .get(&endpoint_id)
            .map_or(0, |l| l.queued.load(Ordering::SeqCst))
    }

    pub fn dispatch(&mut self, partition: i32, job: Job) {
        let endpoint_id = job.msg.endpoint_id;
        let lane = self.lanes.entry(endpoint_id).or_insert_with(|| {
            spawn_lane(self.ctx.clone(), partition, self.stopping.child_token())
        });
        lane.queued.fetch_add(1, Ordering::SeqCst);
        if let Err(mpsc::error::SendError(job)) = lane.tx.send(job) {
            // The lane task is gone (cannot normally happen). Run the job on a
            // fresh lane rather than drop it.
            tracing::warn!(%endpoint_id, "lane task ended unexpectedly, recreating");
            self.lanes.remove(&endpoint_id);
            self.dispatch(partition, job);
        }
    }

    /// Stop the lanes of a revoked partition and forget them.
    pub fn revoke_partition(&mut self, partition: i32) {
        self.lanes.retain(|_, lane| {
            if lane.partition == partition {
                lane.cancel.cancel();
                false
            } else {
                true
            }
        });
    }

    /// Remove empty lanes so idle endpoints do not keep a task each. Safe
    /// because an empty lane has no queued or running job, and a new job for
    /// that endpoint simply creates a new lane.
    pub fn remove_idle(&mut self) {
        self.lanes
            .retain(|_, lane| lane.queued.load(Ordering::SeqCst) > 0);
    }

    pub fn total_queued(&self) -> usize {
        self.lanes
            .values()
            .map(|l| l.queued.load(Ordering::SeqCst))
            .sum()
    }
}

fn spawn_lane(ctx: Arc<WorkerCtx>, partition: i32, cancel: CancellationToken) -> Lane {
    let (tx, rx) = mpsc::unbounded_channel();
    let queued = Arc::new(AtomicUsize::new(0));
    tokio::spawn(lane_task(ctx, rx, queued.clone(), cancel.clone()));
    Lane {
        tx,
        queued,
        cancel,
        partition,
    }
}

async fn lane_task(
    ctx: Arc<WorkerCtx>,
    mut rx: mpsc::UnboundedReceiver<Job>,
    queued: Arc<AtomicUsize>,
    cancel: CancellationToken,
) {
    let concurrency = ctx.config.per_endpoint_concurrency.max(1);
    let permits = Arc::new(Semaphore::new(concurrency));
    while let Some(job) = rx.recv().await {
        if cancel.is_cancelled() {
            // Partition revoked or worker stopping: drop without completing.
            queued.fetch_sub(1, Ordering::SeqCst);
            continue;
        }
        let Ok(permit) = permits.clone().acquire_owned().await else {
            break;
        };
        let ctx = ctx.clone();
        let queued = queued.clone();
        let cancel = cancel.clone();
        let run = async move {
            run_job(&ctx, job, &cancel).await;
            queued.fetch_sub(1, Ordering::SeqCst);
            drop(permit);
        };
        if concurrency == 1 {
            // Run inline: the next job starts only after this one finishes,
            // which is what keeps an endpoint's deliveries in order.
            run.await;
        } else {
            tokio::spawn(run);
        }
    }
}

/// Process a job, retrying on infrastructure errors (database down). The
/// offset is only marked done once processing actually succeeded; giving up
/// silently would strand a `pending` delivery that nothing re-queues.
async fn run_job(ctx: &Arc<WorkerCtx>, job: Job, cancel: &CancellationToken) {
    let mut delay = Duration::from_millis(500);
    loop {
        let pre_claimed = job.pre_claimed.as_ref().map(|p| PreClaimed {
            delivery: p.delivery.clone(),
            claimed_at: p.claimed_at,
        });
        match process::process(ctx, &job.msg, pre_claimed).await {
            Ok(outcome) => {
                (job.on_done)(outcome);
                return;
            }
            Err(e) => {
                tracing::warn!(error = %e, delivery_id = job.msg.delivery_id, retry_in_ms = delay.as_millis() as u64,
                    "could not process delivery, will retry");
                tokio::select! {
                    _ = cancel.cancelled() => return,
                    _ = tokio::time::sleep(delay) => {}
                }
                delay = (delay * 2).min(Duration::from_secs(30));
            }
        }
    }
}
