//! The outbox table: "messages that must be published to Kafka".
//!
//! Rows are written in the same transaction as the state change they announce
//! (new delivery, due retry, reaped delivery, resend). The relay publishes
//! them and marks them `published_at`. See `db/events.rs` for why this exists
//! (the dual-write problem) and `relay.rs` for the publishing side.

use chrono::{DateTime, Utc};
use sqlx::{PgConnection, Postgres, Transaction};
use uuid::Uuid;

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct OutboxRow {
    pub id: i64,
    pub delivery_id: i64,
    pub endpoint_id: Uuid,
    pub tenant_id: Uuid,
    pub event_id: Uuid,
    pub traceparent: Option<String>,
    pub created_at: DateTime<Utc>,
}

/// One outbox row per delivery id, in id order.
pub async fn insert_for_deliveries(
    conn: &mut PgConnection,
    delivery_ids: &[i64],
) -> Result<(), sqlx::Error> {
    if delivery_ids.is_empty() {
        return Ok(());
    }
    sqlx::query(
        "INSERT INTO outbox (delivery_id, endpoint_id, tenant_id)
         SELECT id, endpoint_id, tenant_id FROM deliveries WHERE id = ANY($1) ORDER BY id",
    )
    .bind(delivery_ids)
    .execute(conn)
    .await?;
    Ok(())
}

/// Lock a batch of unpublished rows for this relay.
///
/// `FOR UPDATE SKIP LOCKED`: a second relay replica skips rows the first has
/// locked and takes the next batch instead, so two relays never publish the
/// same batch concurrently (they might still both publish a row once each
/// across a crash; the worker's claim makes that harmless).
pub async fn lock_unpublished(
    tx: &mut Transaction<'_, Postgres>,
    limit: i64,
) -> Result<Vec<OutboxRow>, sqlx::Error> {
    sqlx::query_as::<_, OutboxRow>(
        "SELECT o.id, o.delivery_id, o.endpoint_id, o.tenant_id, d.event_id, o.traceparent, o.created_at
           FROM outbox o JOIN deliveries d ON d.id = o.delivery_id
          WHERE o.published_at IS NULL
          ORDER BY o.id LIMIT $1
          FOR UPDATE OF o SKIP LOCKED",
    )
    .bind(limit)
    .fetch_all(&mut **tx)
    .await
}

/// Mark rows as published. Called only after Kafka acknowledged every message.
pub async fn mark_published(
    tx: &mut Transaction<'_, Postgres>,
    ids: &[i64],
) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE outbox SET published_at = now() WHERE id = ANY($1)")
        .bind(ids)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// Housekeeping: published rows are only kept for an hour (for debugging).
pub async fn delete_published_older_than(
    pool: &sqlx::PgPool,
    secs: f64,
) -> Result<u64, sqlx::Error> {
    let r = sqlx::query(
        "DELETE FROM outbox WHERE id IN (
            SELECT id FROM outbox WHERE published_at < now() - make_interval(secs => $1) LIMIT 10000)",
    )
    .bind(secs)
    .execute(pool)
    .await?;
    Ok(r.rows_affected())
}

/// Age in seconds of the oldest unpublished row (0 if none). If this grows,
/// the relay is stuck or too slow.
pub async fn oldest_unpublished_age_secs(pool: &sqlx::PgPool) -> Result<f64, sqlx::Error> {
    sqlx::query_scalar(
        // Oldest by id = oldest by time; walks the partial index, reads one row.
        "SELECT COALESCE((SELECT EXTRACT(EPOCH FROM now() - created_at)::float8
                            FROM outbox WHERE published_at IS NULL ORDER BY id LIMIT 1), 0)",
    )
    .fetch_one(pool)
    .await
}
