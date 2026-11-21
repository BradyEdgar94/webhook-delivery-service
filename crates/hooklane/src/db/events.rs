//! Events and the ingest transaction.
//!
//! **This is the transactional outbox pattern.** It exists to avoid the
//! *dual-write problem*: if the API saved the event to Postgres and then
//! published to Kafka as two separate steps, a crash between them would leave
//! an event that is saved but never delivered (or, in the other order,
//! delivered but not saved). Instead, the event, one delivery row per matching
//! endpoint, and one outbox row per delivery are written in **one Postgres
//! transaction**. Either all of it exists or none of it does. A separate
//! process (the relay) later copies outbox rows to Kafka, and if it crashes it
//! simply retries, because the outbox row is still there.

use chrono::{DateTime, Utc};
use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

use crate::config::QueueMode;

/// Event types starting with this prefix are reserved for system events.
pub const SYSTEM_PREFIX: &str = "hooklane.";
pub const SYSTEM_DELIVERY_DEAD: &str = "hooklane.delivery.dead";
pub const SYSTEM_ENDPOINT_DISABLED: &str = "hooklane.endpoint.disabled";

pub fn is_system_event_type(t: &str) -> bool {
    t.starts_with(SYSTEM_PREFIX)
}

#[derive(Debug, Clone, sqlx::FromRow, serde::Serialize)]
pub struct EventRow {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub event_type: String,
    pub payload: serde_json::Value,
    pub idempotency_key: Option<String>,
    pub created_at: DateTime<Utc>,
}

pub struct IngestResult {
    pub event_id: Uuid,
    /// False when an existing event was returned for a repeated idempotency key.
    pub created: bool,
    pub deliveries: u64,
}

/// Save an event and fan it out, all in one transaction (see module docs).
pub async fn ingest(
    pool: &PgPool,
    tenant_id: Uuid,
    event_type: &str,
    payload: &serde_json::Value,
    idempotency_key: Option<&str>,
    queue_mode: QueueMode,
    traceparent: Option<&str>,
) -> Result<IngestResult, sqlx::Error> {
    let mut tx = pool.begin().await?;

    // The idempotency guarantee. `ON CONFLICT DO NOTHING` against the unique
    // index (tenant_id, idempotency_key) means that when two requests with the
    // same key race, the database lets exactly one insert win; the loser's
    // INSERT returns no row. A "SELECT first, INSERT if missing" version would
    // let both requests see "missing" and both insert.
    let inserted: Option<Uuid> = sqlx::query_scalar(
        "INSERT INTO events (id, tenant_id, event_type, payload, idempotency_key)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (tenant_id, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
         RETURNING id",
    )
    .bind(Uuid::now_v7())
    .bind(tenant_id)
    .bind(event_type)
    .bind(payload)
    .bind(idempotency_key)
    .fetch_optional(&mut *tx)
    .await?;

    let Some(event_id) = inserted else {
        // Repeat of an earlier request: return the original event, create nothing.
        tx.rollback().await?;
        let existing: Uuid = sqlx::query_scalar(
            "SELECT id FROM events WHERE tenant_id = $1 AND idempotency_key = $2",
        )
        .bind(tenant_id)
        .bind(idempotency_key)
        .fetch_one(pool)
        .await?;
        return Ok(IngestResult {
            event_id: existing,
            created: false,
            deliveries: 0,
        });
    };

    let deliveries = fan_out(
        &mut tx,
        tenant_id,
        event_id,
        event_type,
        false,
        queue_mode,
        traceparent,
    )
    .await?;

    // Commit point. Only after this succeeds does the API answer 202: from
    // here on the event cannot be lost (guarantee 1).
    tx.commit().await?;
    Ok(IngestResult {
        event_id,
        created: true,
        deliveries,
    })
}

/// Insert one delivery per subscribed, active endpoint and, in Kafka mode, one
/// outbox row per delivery. Must run inside the caller's transaction.
///
/// Normal events go to endpoints with no filter (NULL/empty `event_types`) or
/// a filter containing the type. System events go only to endpoints that
/// subscribed to that system type *explicitly*, so nobody receives them by
/// accident.
async fn fan_out(
    conn: &mut PgConnection,
    tenant_id: Uuid,
    event_id: Uuid,
    event_type: &str,
    system: bool,
    queue_mode: QueueMode,
    traceparent: Option<&str>,
) -> Result<u64, sqlx::Error> {
    let match_clause = if system {
        "$3 = ANY(event_types)"
    } else {
        "(event_types IS NULL OR cardinality(event_types) = 0 OR $3 = ANY(event_types))"
    };
    let n = match queue_mode {
        QueueMode::Postgres => {
            let sql = format!(
                "INSERT INTO deliveries (event_id, endpoint_id, tenant_id, status)
                 SELECT $1, id, tenant_id, 'pending' FROM endpoints
                 WHERE tenant_id = $2 AND status = 'active' AND {match_clause}
                 ORDER BY id"
            );
            sqlx::query(sqlx::AssertSqlSafe(sql))
                .bind(event_id)
                .bind(tenant_id)
                .bind(event_type)
                .execute(&mut *conn)
                .await?
                .rows_affected()
        }
        QueueMode::Kafka => {
            // Deliveries and their outbox rows in one statement (a writable CTE),
            // still inside the same transaction as the event.
            let sql = format!(
                "WITH d AS (
                    INSERT INTO deliveries (event_id, endpoint_id, tenant_id, status)
                    SELECT $1, id, tenant_id, 'pending' FROM endpoints
                    WHERE tenant_id = $2 AND status = 'active' AND {match_clause}
                    ORDER BY id
                    RETURNING id, endpoint_id, tenant_id
                 )
                 INSERT INTO outbox (delivery_id, endpoint_id, tenant_id, traceparent)
                 SELECT id, endpoint_id, tenant_id, $4 FROM d ORDER BY id"
            );
            sqlx::query(sqlx::AssertSqlSafe(sql))
                .bind(event_id)
                .bind(tenant_id)
                .bind(event_type)
                .bind(traceparent)
                .execute(&mut *conn)
                .await?
                .rows_affected()
        }
    };
    Ok(n)
}

/// Emit a system event (`hooklane.delivery.dead`, `hooklane.endpoint.disabled`)
/// for a tenant through the normal pipeline, inside the caller's transaction,
/// so the notification exists if and only if the state change does.
pub async fn emit_system_event(
    conn: &mut PgConnection,
    tenant_id: Uuid,
    event_type: &str,
    data: serde_json::Value,
    queue_mode: QueueMode,
) -> Result<Uuid, sqlx::Error> {
    let event_id = Uuid::now_v7();
    sqlx::query("INSERT INTO events (id, tenant_id, event_type, payload) VALUES ($1, $2, $3, $4)")
        .bind(event_id)
        .bind(tenant_id)
        .bind(event_type)
        .bind(&data)
        .execute(&mut *conn)
        .await?;
    fan_out(
        conn, tenant_id, event_id, event_type, true, queue_mode, None,
    )
    .await?;
    Ok(event_id)
}

pub async fn get(
    pool: &PgPool,
    tenant_id: Uuid,
    id: Uuid,
) -> Result<Option<EventRow>, sqlx::Error> {
    sqlx::query_as::<_, EventRow>(
        "SELECT id, tenant_id, event_type, payload, idempotency_key, created_at
         FROM events WHERE tenant_id = $1 AND id = $2",
    )
    .bind(tenant_id)
    .bind(id)
    .fetch_optional(pool)
    .await
}

/// Keyset pagination, newest first. Event ids are UUIDv7, so ordering by id is
/// ordering by creation time.
pub async fn list(
    pool: &PgPool,
    tenant_id: Uuid,
    before: Option<Uuid>,
    limit: i64,
) -> Result<Vec<EventRow>, sqlx::Error> {
    sqlx::query_as::<_, EventRow>(
        "SELECT id, tenant_id, event_type, payload, idempotency_key, created_at
         FROM events WHERE tenant_id = $1 AND ($2::uuid IS NULL OR id < $2)
         ORDER BY id DESC LIMIT $3",
    )
    .bind(tenant_id)
    .bind(before)
    .bind(limit)
    .fetch_all(pool)
    .await
}
