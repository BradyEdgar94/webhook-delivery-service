//! The Kafka consume loop.
//!
//! One loop per worker process reads messages from all partitions assigned to
//! it, and for each message:
//! 1. records the offset as in flight (`offsets.rs`);
//! 2. dispatches it to its endpoint's lane (`lanes.rs`), or parks it if that
//!    lane is overflowing;
//! 3. when the lane finishes it, marks the offset done.
//!
//! Every second, the highest *contiguous* finished offset of each partition is
//! committed. Messages are never committed before they are handled.
//!
//! **Backpressure.** If a partition has too many unfinished messages (default
//! 256) we pause fetching it until it drains to half that, so a slow partition
//! cannot make the worker buffer unbounded memory.
//!
//! **Lane overflow.** If one endpoint's lane holds more than 100 messages,
//! extra ones are parked in Postgres (`retry_scheduled` a few seconds ahead,
//! no attempt used). Otherwise a single slow endpoint could fill its
//! partition's in-flight cap, pause the partition, and so block every healthy
//! endpoint that shares it.
//!
//! **Rebalances.** When Kafka takes a partition away from this worker (scale
//! up/down, crash of a peer), we commit what is finished for that partition,
//! drop its in-memory state and stop its lanes. Unfinished messages will be
//! delivered to the new owner from the last committed offset; the claim step
//! makes any overlap harmless.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::Utc;
use rdkafka::client::ClientContext;
use rdkafka::consumer::{
    BaseConsumer, CommitMode, Consumer, ConsumerContext, Rebalance, StreamConsumer,
};
use rdkafka::message::{BorrowedMessage, Headers, Message};
use rdkafka::statistics::Statistics;
use rdkafka::{Offset, TopicPartitionList};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::WorkerCtx;
use super::lanes::{Job, Lanes};
use super::offsets::OffsetTracker;
use crate::db::{self, status};
use crate::delivery::retry;
use crate::metrics;
use crate::queue::{DeliveryMessage, kafka};

struct PartitionState {
    tracker: OffsetTracker,
    /// Increases every time we (re)start tracking a partition, so completions
    /// from a previous ownership of the same partition are ignored.
    generation: u64,
}

#[derive(Default)]
struct SharedState {
    assigned: HashSet<i32>,
    partitions: HashMap<i32, PartitionState>,
    next_generation: u64,
}

/// Offset bookkeeping shared between the loop and librdkafka's rebalance
/// callbacks. A std Mutex, never held across an `.await`.
type Shared = Arc<Mutex<SharedState>>;

fn lock(shared: &Shared) -> std::sync::MutexGuard<'_, SharedState> {
    match shared.lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    }
}

/// Hooks librdkafka calls into: rebalances and statistics.
struct WorkerKafkaContext {
    topic: String,
    shared: Shared,
    revoked_tx: mpsc::UnboundedSender<i32>,
}

impl ClientContext for WorkerKafkaContext {
    /// Consumer lag summed over our partitions (-1 = unknown, skipped).
    fn stats(&self, stats: Statistics) {
        let lag: i64 = stats
            .topics
            .get(&self.topic)
            .map(|t| {
                t.partitions
                    .values()
                    .filter(|p| p.partition >= 0 && p.consumer_lag > 0)
                    .map(|p| p.consumer_lag)
                    .sum()
            })
            .unwrap_or(0);
        metrics::get().consumer_lag.record(lag, &[]);
    }
}

impl ConsumerContext for WorkerKafkaContext {
    fn pre_rebalance(&self, base_consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        let Rebalance::Revoke(tpl) = rebalance else {
            return;
        };
        let mut to_commit = TopicPartitionList::new();
        {
            let mut state = lock(&self.shared);
            for elem in tpl.elements_for_topic(&self.topic) {
                let p = elem.partition();
                state.assigned.remove(&p);
                if let Some(mut ps) = state.partitions.remove(&p)
                    && let Some(offset) = ps.tracker.take_commit()
                {
                    let _ = to_commit.add_partition_offset(&self.topic, p, Offset::Offset(offset));
                }
                let _ = self.revoked_tx.send(p);
            }
        }
        // Commit finished work before giving the partition away, so the new
        // owner does not redo it (it would be skipped by the claim anyway).
        if to_commit.count() > 0
            && let Err(e) = base_consumer.commit(&to_commit, CommitMode::Sync)
        {
            tracing::warn!(error = %e, "commit on revoke failed");
        }
        tracing::info!(partitions = tpl.count(), "partitions revoked");
    }

    fn post_rebalance(&self, _base_consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        if let Rebalance::Assign(tpl) = rebalance {
            let mut state = lock(&self.shared);
            for elem in tpl.elements_for_topic(&self.topic) {
                state.assigned.insert(elem.partition());
            }
            tracing::info!(
                added = tpl.count(),
                total = state.assigned.len(),
                "partitions assigned"
            );
        }
    }
}

struct Done {
    partition: i32,
    offset: i64,
    generation: u64,
}

pub async fn run(ctx: Arc<WorkerCtx>, shutdown: CancellationToken) -> anyhow::Result<()> {
    let config = ctx.config.clone();
    let topic = config.kafka_topic.clone();
    let shared: Shared = Arc::new(Mutex::new(SharedState::default()));
    let (revoked_tx, mut revoked_rx) = mpsc::unbounded_channel::<i32>();
    let context = WorkerKafkaContext {
        topic: topic.clone(),
        shared: shared.clone(),
        revoked_tx,
    };
    let consumer: StreamConsumer<WorkerKafkaContext> = kafka::consumer(&config, context)?;
    // librdkafka reconnects to the broker on its own, with backoff; subscribe
    // only registers interest, so this works even if the broker is still down.
    consumer.subscribe(&[topic.as_str()])?;
    tracing::info!(%topic, group = %config.kafka_consumer_group, "worker consuming");

    let (done_tx, mut done_rx) = mpsc::unbounded_channel::<Done>();
    let stopping = CancellationToken::new();
    let mut lanes = Lanes::new(ctx.clone(), stopping.clone());
    let mut paused: HashSet<i32> = HashSet::new();
    let cap = config.partition_max_in_flight.max(1);
    let mut commit_tick = tokio::time::interval(Duration::from_secs(1));
    let mut housekeeping_tick = tokio::time::interval(Duration::from_secs(15));

    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => break,
            Some(p) = revoked_rx.recv() => {
                lanes.revoke_partition(p);
                paused.remove(&p);
            }
            Some(done) = done_rx.recv() => {
                handle_done(&consumer, &topic, &shared, &mut paused, cap, done);
            }
            received = consumer.recv() => match received {
                Ok(message) => handle_message(&ctx, &consumer, &topic, &shared, &mut lanes, &mut paused, cap, &done_tx, &message),
                Err(e) => {
                    tracing::warn!(error = %e, "kafka receive error");
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            },
            _ = commit_tick.tick() => commit(&consumer, &topic, &shared, CommitMode::Async),
            _ = housekeeping_tick.tick() => {
                lanes.remove_idle();
                ctx.guards.expire_idle();
            }
        }
    }

    // Graceful shutdown: stop reading, let in-flight deliveries finish (up to
    // the grace period), commit, then leave the group.
    tracing::info!(
        in_flight = lanes.total_queued(),
        "draining in-flight deliveries"
    );
    let deadline = tokio::time::Instant::now() + config.shutdown_grace();
    while in_flight_total(&shared) > 0 {
        tokio::select! {
            Some(done) = done_rx.recv() => handle_done(&consumer, &topic, &shared, &mut paused, cap, done),
            _ = tokio::time::sleep_until(deadline) => {
                tracing::warn!(remaining = in_flight_total(&shared), "grace period over; unfinished messages will be redelivered");
                break;
            }
        }
    }
    commit(&consumer, &topic, &shared, CommitMode::Sync);
    stopping.cancel();
    consumer.unsubscribe();
    tracing::info!("worker stopped");
    Ok(())
}

fn in_flight_total(shared: &Shared) -> usize {
    lock(shared)
        .partitions
        .values()
        .map(|p| p.tracker.in_flight())
        .sum()
}

fn partition_list(topic: &str, partition: i32) -> TopicPartitionList {
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition(topic, partition);
    tpl
}

#[allow(clippy::too_many_arguments)]
fn handle_message(
    ctx: &Arc<WorkerCtx>,
    consumer: &StreamConsumer<WorkerKafkaContext>,
    topic: &str,
    shared: &Shared,
    lanes: &mut Lanes,
    paused: &mut HashSet<i32>,
    cap: usize,
    done_tx: &mpsc::UnboundedSender<Done>,
    message: &BorrowedMessage<'_>,
) {
    let partition = message.partition();
    let offset = message.offset();

    let parsed: Option<DeliveryMessage> = message
        .payload()
        .and_then(|p| serde_json::from_slice(p).ok())
        .map(|mut m: DeliveryMessage| {
            m.traceparent = message.headers().and_then(|headers| {
                headers
                    .iter()
                    .find(|h| h.key == "traceparent")
                    .and_then(|h| h.value)
                    .map(|v| String::from_utf8_lossy(v).into_owned())
            });
            m
        });

    // Register the offset as in flight.
    let (generation, in_flight) = {
        let mut state = lock(shared);
        if !state.assigned.contains(&partition) {
            // Leftover message from a partition we no longer own: ignore it.
            return;
        }
        if !state.partitions.contains_key(&partition) {
            // First message since this partition was (re)assigned to us.
            let generation = state.next_generation;
            state.next_generation += 1;
            state.partitions.insert(
                partition,
                PartitionState {
                    tracker: OffsetTracker::new(),
                    generation,
                },
            );
        }
        let Some(ps) = state.partitions.get_mut(&partition) else {
            return;
        };
        ps.tracker.start(offset);
        (ps.generation, ps.tracker.in_flight())
    };

    let Some(msg) = parsed else {
        tracing::error!(partition, offset, "unreadable message, skipping");
        let _ = done_tx.send(Done {
            partition,
            offset,
            generation,
        });
        return;
    };

    // Backpressure: too much unfinished work in this partition -> stop fetching it.
    if in_flight >= cap && !paused.contains(&partition) {
        if let Err(e) = consumer.pause(&partition_list(topic, partition)) {
            tracing::warn!(error = %e, partition, "pause failed");
        } else {
            paused.insert(partition);
            tracing::debug!(partition, "partition paused (in-flight cap reached)");
        }
    }

    let tx = done_tx.clone();
    let on_done = Box::new(move |_outcome| {
        let _ = tx.send(Done {
            partition,
            offset,
            generation,
        });
    });

    if lanes.queued(msg.endpoint_id) >= ctx.config.lane_max_queued {
        // Lane overflow: park in Postgres instead of queueing in memory.
        let ctx = ctx.clone();
        tokio::spawn(async move {
            park_overflow(&ctx, &msg).await;
            on_done(super::process::Outcome::Parked);
        });
        return;
    }
    lanes.dispatch(
        partition,
        Job {
            msg,
            pre_claimed: None,
            on_done,
        },
    );
}

/// Park an overflowing delivery a few seconds ahead (no attempt used).
/// Retries until it succeeds: the offset must not be marked done before the
/// delivery is safely re-scheduled in Postgres.
pub(super) async fn park_overflow(ctx: &Arc<WorkerCtx>, msg: &DeliveryMessage) {
    park_with_retry(ctx, msg, status::PENDING).await
}

pub(super) async fn park_with_retry(
    ctx: &Arc<WorkerCtx>,
    msg: &DeliveryMessage,
    from_status: &str,
) {
    let mut delay = Duration::from_millis(200);
    loop {
        let wait = retry::jitter(Duration::from_secs(5));
        let until = Utc::now() + chrono::Duration::from_std(wait).unwrap_or_default();
        match db::deliveries::park(
            &ctx.pool,
            msg.delivery_id,
            until,
            from_status,
            "lane_overflow",
        )
        .await
        {
            Ok(_) => {
                metrics::get()
                    .deliveries
                    .add(1, &[metrics::kv("outcome", "parked")]);
                return;
            }
            Err(e) => {
                tracing::warn!(error = %e, delivery_id = msg.delivery_id, "could not park delivery, retrying");
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(10));
            }
        }
    }
}

fn handle_done(
    consumer: &StreamConsumer<WorkerKafkaContext>,
    topic: &str,
    shared: &Shared,
    paused: &mut HashSet<i32>,
    cap: usize,
    done: Done,
) {
    let in_flight = {
        let mut state = lock(shared);
        match state.partitions.get_mut(&done.partition) {
            Some(ps) if ps.generation == done.generation => {
                ps.tracker.finish(done.offset);
                ps.tracker.in_flight()
            }
            // Partition was revoked since: this completion no longer matters.
            _ => return,
        }
    };
    if paused.contains(&done.partition) && in_flight <= cap / 2 {
        if let Err(e) = consumer.resume(&partition_list(topic, done.partition)) {
            tracing::warn!(error = %e, partition = done.partition, "resume failed");
        } else {
            paused.remove(&done.partition);
        }
    }
}

/// Commit every partition whose contiguous finished offset moved forward.
fn commit(
    consumer: &StreamConsumer<WorkerKafkaContext>,
    topic: &str,
    shared: &Shared,
    mode: CommitMode,
) {
    let mut tpl = TopicPartitionList::new();
    {
        let mut state = lock(shared);
        for (p, ps) in state.partitions.iter_mut() {
            if let Some(offset) = ps.tracker.take_commit() {
                let _ = tpl.add_partition_offset(topic, *p, Offset::Offset(offset));
            }
        }
    }
    if tpl.count() == 0 {
        return;
    }
    if let Err(e) = consumer.commit(&tpl, mode) {
        tracing::warn!(error = %e, "offset commit failed; will commit a later offset next time");
    }
}
