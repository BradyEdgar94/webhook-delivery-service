//! The relay: copies outbox rows to Kafka (Kafka mode only).
//!
//! This is the second half of the transactional outbox pattern (the first half
//! is the ingest transaction in `db/events.rs`). It is the **only** component
//! that writes to Kafka: new deliveries, retries, reaped deliveries and
//! resends all enter Kafka through the outbox.
//!
//! Loop:
//! 1. in a transaction, lock a batch of unpublished rows (`FOR UPDATE SKIP LOCKED`);
//! 2. produce each to Kafka (key = endpoint id, `acks=all`, idempotent producer)
//!    and wait for **every** acknowledgement;
//! 3. mark the rows published and commit;
//! 4. if the batch was empty, sleep briefly.
//!
//! **Crash safety.** If the relay dies after Kafka accepted a message but
//! before the row was marked published, the row is published again on
//! restart. That duplicate message is harmless: the worker's conditional claim
//! lets only one copy turn into an HTTP request. What can never happen is the
//! opposite (marked published but not in Kafka), because we mark only after
//! the acknowledgements arrive.
//!
//! **Ordering note.** Running two relays is safe (SKIP LOCKED gives each its
//! own batch) but can slightly reorder one endpoint's messages across batches.
//! That is acceptable under best-effort ordering (see docs/DECISIONS.md).

use std::sync::Arc;
use std::time::Duration;

use futures::future::join_all;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::config::Config;
use crate::db::outbox;
use crate::queue::{DeliveryMessage, kafka};
use crate::{metrics, telemetry};

pub async fn run(
    config: Arc<Config>,
    pool: PgPool,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let producer = kafka::producer(&config)?;
    tracing::info!(topic = %config.kafka_topic, batch = config.relay_batch_size, "relay started");
    let mut error_backoff = Duration::from_millis(250);

    while !shutdown.is_cancelled() {
        match publish_batch(&config, &pool, &producer).await {
            Ok(0) => {
                error_backoff = Duration::from_millis(250);
                tokio::select! {
                    _ = shutdown.cancelled() => {}
                    _ = tokio::time::sleep(Duration::from_millis(config.relay_idle_sleep_ms)) => {}
                }
            }
            Ok(_) => error_backoff = Duration::from_millis(250),
            Err(e) => {
                // Database or Kafka unavailable: the rows stay unpublished and
                // are retried. Nothing is lost, only delayed.
                tracing::warn!(error = %e, retry_in_ms = error_backoff.as_millis() as u64, "relay batch failed");
                tokio::select! {
                    _ = shutdown.cancelled() => {}
                    _ = tokio::time::sleep(error_backoff) => {}
                }
                error_backoff = (error_backoff * 2).min(Duration::from_secs(5));
            }
        }
    }
    // A batch in progress always finishes (the loop only checks between
    // batches), so shutdown never abandons a half-published batch.
    tracing::info!("relay stopped");
    Ok(())
}

/// Publish one batch. Returns how many rows were published.
pub async fn publish_batch(
    config: &Config,
    pool: &PgPool,
    producer: &FutureProducer,
) -> anyhow::Result<usize> {
    let mut tx = pool.begin().await?;
    let rows = outbox::lock_unpublished(&mut tx, config.relay_batch_size).await?;
    if rows.is_empty() {
        tx.rollback().await?;
        return Ok(0);
    }

    let sends = rows.iter().map(|row| {
        let span = tracing::info_span!("relay.publish", delivery_id = row.delivery_id, endpoint_id = %row.endpoint_id);
        telemetry::set_parent_from_traceparent(&span, row.traceparent.as_deref());
        async move {
            let msg = DeliveryMessage {
                delivery_id: row.delivery_id,
                endpoint_id: row.endpoint_id,
                tenant_id: row.tenant_id,
                event_id: row.event_id,
                traceparent: None,
            };
            let payload = serde_json::to_vec(&msg)?;
            let key = row.endpoint_id.to_string();
            // Carry this span's context forward so the worker joins the trace.
            let mut headers = OwnedHeaders::new();
            if let Some(tp) = telemetry::current_traceparent() {
                headers = headers.insert(Header { key: "traceparent", value: Some(tp.as_str()) });
            }
            // Key = endpoint id: Kafka hashes the key to pick the partition, so
            // every delivery for one endpoint goes to the same partition.
            let record = FutureRecord::to(&config.kafka_topic).key(&key).payload(&payload).headers(headers);
            producer
                .send(record, Duration::from_secs(30))
                .await
                .map(|_| ())
                .map_err(|(e, _)| anyhow::anyhow!("kafka produce failed: {e}"))
        }
        .instrument(span)
    });
    let results = join_all(sends).await;
    if let Some(err) = results.into_iter().find_map(Result::err) {
        // Some messages may have been accepted. Rolling back leaves every row
        // unpublished, so they are all sent again: duplicates, never loss.
        tx.rollback().await?;
        return Err(err);
    }

    let ids: Vec<i64> = rows.iter().map(|r| r.id).collect();
    outbox::mark_published(&mut tx, &ids).await?;
    tx.commit().await?;
    metrics::get().relay_published.add(ids.len() as u64, &[]);
    Ok(ids.len())
}
