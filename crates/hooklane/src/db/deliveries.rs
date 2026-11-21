//! Deliveries: one row per (event, endpoint), and the state machine that
//! drives it.
//!
//! ```text
//! pending ──claim──► delivering ──2xx──────────────────────► succeeded
//!    ▲                   ├─ retryable, attempts left ──────► retry_scheduled ──(scheduler)──► pending
//!    │                   ├─ retryable, attempts used up ───► dead
//!    │                   └─ non-retryable ─────────────────► failed
//!    └── parked (breaker open, lane overflow, 429 pause): pending ──► retry_scheduled, no attempt used
//! ```
//!
//! Every transition is a conditional `UPDATE ... WHERE status = '<expected>'`
//! whose row count is checked (see `db/mod.rs`).

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use super::{attempts::NewAttempt, endpoints, events, outbox, status};
use crate::config::QueueMode;

#[derive(Debug, Clone, sqlx::FromRow, serde::Serialize)]
pub struct DeliveryRow {
    pub id: i64,
    pub event_id: Uuid,
    pub endpoint_id: Uuid,
    pub tenant_id: Uuid,
    pub status: String,
    pub attempt_count: i32,
    pub next_attempt_at: Option<DateTime<Utc>>,
    pub claimed_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub completed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

const COLUMNS: &str = "id, event_id, endpoint_id, tenant_id, status, attempt_count, next_attempt_at, \
                       claimed_at, last_error, completed_at, created_at, updated_at";

/// A delivery that this process has successfully claimed, with what it needs
/// to build the request.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ClaimedDelivery {
    pub id: i64,
    pub event_id: Uuid,
    pub endpoint_id: Uuid,
    pub tenant_id: Uuid,
    /// Already incremented: this is the number of the attempt about to be made.
    pub attempt_count: i32,
    pub event_type: String,
    pub payload: serde_json::Value,
    pub event_created_at: DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// Worker side
// ---------------------------------------------------------------------------

/// THE CLAIM. Immediately before sending, move the delivery from `pending` to
/// `delivering` and count the attempt, but only if it is still `pending`.
///
/// Kafka delivers messages *at least once*: a relay retry or a rebalance can
/// hand the same delivery to a worker twice, or to two workers. Whoever runs
/// this UPDATE first gets the row back; everyone else gets `None` and must
/// skip. That turns at-least-once *messages* into at most one HTTP request per
/// attempt. Without the `AND d.status = 'pending'` condition, both copies
/// would send.
pub async fn claim(
    pool: &PgPool,
    delivery_id: i64,
) -> Result<Option<ClaimedDelivery>, sqlx::Error> {
    sqlx::query_as::<_, ClaimedDelivery>(
        "UPDATE deliveries d
            SET status = 'delivering', claimed_at = now(), attempt_count = d.attempt_count + 1, updated_at = now()
           FROM events e
          WHERE d.id = $1 AND d.status = 'pending' AND e.id = d.event_id
         RETURNING d.id, d.event_id, d.endpoint_id, d.tenant_id, d.attempt_count,
                   e.event_type, e.payload, e.created_at AS event_created_at",
    )
    .bind(delivery_id)
    .fetch_optional(pool)
    .await
}

/// Postgres queue mode: claim a batch of `pending` deliveries in one go.
///
/// `FOR UPDATE SKIP LOCKED` is what makes many workers safe here: each worker
/// locks the rows it picks and the others *skip* locked rows instead of
/// waiting for them or picking the same ones. Without SKIP LOCKED every worker
/// would grab the same oldest rows and queue up behind each other.
pub async fn claim_batch(pool: &PgPool, limit: i64) -> Result<Vec<ClaimedDelivery>, sqlx::Error> {
    sqlx::query_as::<_, ClaimedDelivery>(
        "WITH picked AS (
            SELECT id FROM deliveries WHERE status = 'pending'
            ORDER BY id LIMIT $1
            FOR UPDATE SKIP LOCKED
         )
         UPDATE deliveries d
            SET status = 'delivering', claimed_at = now(), attempt_count = d.attempt_count + 1, updated_at = now()
           FROM picked, events e
          WHERE d.id = picked.id AND e.id = d.event_id
         RETURNING d.id, d.event_id, d.endpoint_id, d.tenant_id, d.attempt_count,
                   e.event_type, e.payload, e.created_at AS event_created_at",
    )
    .bind(limit)
    .fetch_all(pool)
    .await
}

/// Park a delivery: put it back to `retry_scheduled` at `until` WITHOUT using
/// up an attempt (the endpoint never got a chance). Used when the breaker is
/// open, the lane is overflowing, or the endpoint asked us to back off.
///
/// `from_status` is `pending` when parking before the claim (Kafka mode) and
/// `delivering` when the row was already claimed (Postgres mode); in the
/// latter case the attempt counted by the claim is given back.
pub async fn park(
    pool: &PgPool,
    delivery_id: i64,
    until: DateTime<Utc>,
    from_status: &str,
    reason: &str,
) -> Result<bool, sqlx::Error> {
    let give_back = if from_status == status::DELIVERING {
        1
    } else {
        0
    };
    let r = sqlx::query(
        "UPDATE deliveries
            SET status = 'retry_scheduled', next_attempt_at = $2, attempt_count = attempt_count - $4,
                claimed_at = NULL, last_error = $5, updated_at = now()
          WHERE id = $1 AND status = $3",
    )
    .bind(delivery_id)
    .bind(until)
    .bind(from_status)
    .bind(give_back)
    .bind(reason)
    .execute(pool)
    .await?;
    Ok(r.rows_affected() == 1)
}

/// Mark a delivery failed because its endpoint is disabled.
pub async fn fail_endpoint_disabled(
    pool: &PgPool,
    delivery_id: i64,
    from_status: &str,
) -> Result<bool, sqlx::Error> {
    let r = sqlx::query(
        "UPDATE deliveries SET status = 'failed', last_error = 'endpoint_disabled',
                completed_at = now(), updated_at = now()
          WHERE id = $1 AND status = $2",
    )
    .bind(delivery_id)
    .bind(from_status)
    .execute(pool)
    .await?;
    Ok(r.rows_affected() == 1)
}

/// The new state after an attempt.
#[derive(Debug, Clone)]
pub enum AttemptResult {
    Succeeded,
    RetryAt(DateTime<Utc>),
    Failed { disable_endpoint: bool },
    Dead,
}

impl AttemptResult {
    pub fn status(&self) -> &'static str {
        match self {
            AttemptResult::Succeeded => status::SUCCEEDED,
            AttemptResult::RetryAt(_) => status::RETRY_SCHEDULED,
            AttemptResult::Failed { .. } => status::FAILED,
            AttemptResult::Dead => status::DEAD,
        }
    }
}

/// Write the attempt row and the delivery's new status together, plus any
/// consequences (system event on `dead`, endpoint disable on 410), in one
/// transaction: the attempt log can never disagree with the delivery state.
///
/// Returns false if the delivery was no longer `delivering` (the reaper took
/// it back because this worker looked dead); nothing is written in that case
/// and the delivery will be sent again, which is the at-least-once trade-off.
pub async fn record_attempt(
    pool: &PgPool,
    claimed: &ClaimedDelivery,
    attempt: &NewAttempt,
    result: &AttemptResult,
    last_error: Option<&str>,
    queue_mode: QueueMode,
) -> Result<bool, sqlx::Error> {
    let mut tx = pool.begin().await?;

    let next_attempt_at = match result {
        AttemptResult::RetryAt(t) => Some(*t),
        _ => None,
    };
    let terminal = !matches!(result, AttemptResult::RetryAt(_));
    // Conditional on 'delivering': if the reaper already re-queued this row,
    // we must not overwrite whatever happened since.
    let updated = sqlx::query(
        "UPDATE deliveries
            SET status = $2, next_attempt_at = $3, last_error = $4,
                completed_at = CASE WHEN $5 THEN now() ELSE NULL END,
                claimed_at = NULL, updated_at = now()
          WHERE id = $1 AND status = 'delivering'",
    )
    .bind(claimed.id)
    .bind(result.status())
    .bind(next_attempt_at)
    .bind(last_error)
    .bind(terminal)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    if updated == 0 {
        tx.rollback().await?;
        return Ok(false);
    }

    super::attempts::insert(&mut tx, claimed.id, attempt).await?;

    // A system event whose own delivery fails must never generate another
    // system event, or one broken endpoint could produce an endless chain.
    let is_system = events::is_system_event_type(&claimed.event_type);

    if let AttemptResult::Dead = result
        && !is_system
    {
        let data = serde_json::json!({
            "delivery_id": claimed.id,
            "event_id": claimed.event_id,
            "endpoint_id": claimed.endpoint_id,
            "event_type": claimed.event_type,
            "attempts": claimed.attempt_count,
            "last_error": last_error,
        });
        events::emit_system_event(
            &mut tx,
            claimed.tenant_id,
            events::SYSTEM_DELIVERY_DEAD,
            data,
            queue_mode,
        )
        .await?;
    }

    if let AttemptResult::Failed {
        disable_endpoint: true,
    } = result
    {
        endpoints::disable_in_tx(
            &mut tx,
            claimed.endpoint_id,
            "endpoint returned 410 Gone",
            queue_mode,
            is_system,
        )
        .await?;
    }

    tx.commit().await?;
    Ok(true)
}

// ---------------------------------------------------------------------------
// Scheduler side
// ---------------------------------------------------------------------------

/// Move due retries back into the queue. Returns how many were re-queued.
///
/// Rows are locked with `FOR UPDATE SKIP LOCKED` so several scheduler replicas
/// can run this at once without double-queueing: each takes a disjoint batch.
/// In Kafka mode the new outbox row is written in the same transaction as the
/// status change, so a retry can never be "pending but not queued".
pub async fn requeue_due_retries(
    pool: &PgPool,
    limit: i64,
    queue_mode: QueueMode,
) -> Result<u64, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let due: Vec<(i64, String)> = sqlx::query_as(
        "SELECT d.id, e.status FROM deliveries d JOIN endpoints e ON e.id = d.endpoint_id
          WHERE d.status = 'retry_scheduled' AND d.next_attempt_at <= now()
          ORDER BY d.next_attempt_at LIMIT $1
          FOR UPDATE OF d SKIP LOCKED",
    )
    .bind(limit)
    .fetch_all(&mut *tx)
    .await?;
    if due.is_empty() {
        tx.rollback().await?;
        return Ok(0);
    }
    let (active, disabled): (Vec<_>, Vec<_>) = due.into_iter().partition(|(_, s)| s == "active");
    let active_ids: Vec<i64> = active.into_iter().map(|(id, _)| id).collect();
    let disabled_ids: Vec<i64> = disabled.into_iter().map(|(id, _)| id).collect();

    // Deliveries of a disabled endpoint are not re-queued; they fail visibly.
    if !disabled_ids.is_empty() {
        sqlx::query(
            "UPDATE deliveries SET status = 'failed', last_error = 'endpoint_disabled', completed_at = now(),
                    next_attempt_at = NULL, updated_at = now()
              WHERE id = ANY($1) AND status = 'retry_scheduled'",
        )
        .bind(&disabled_ids)
        .execute(&mut *tx)
        .await?;
    }

    let n = sqlx::query(
        "UPDATE deliveries SET status = 'pending', next_attempt_at = NULL, updated_at = now()
          WHERE id = ANY($1) AND status = 'retry_scheduled'",
    )
    .bind(&active_ids)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    if queue_mode == QueueMode::Kafka {
        outbox::insert_for_deliveries(&mut tx, &active_ids).await?;
    }
    tx.commit().await?;
    Ok(n)
}

/// The reaper. A delivery stuck in `delivering` for longer than `stuck_after`
/// belonged to a worker that crashed (or froze) after claiming it. Put it back
/// to `pending` (and the queue), recording a `worker_lost` attempt so the log
/// explains the gap. The request may or may not have reached the customer: it
/// will be sent again, which is exactly the at-least-once window.
///
/// If the lost attempt was the last one allowed, the delivery becomes `dead`.
pub async fn reap_stuck(
    pool: &PgPool,
    stuck_after_secs: f64,
    limit: i64,
    max_attempts: i32,
    queue_mode: QueueMode,
) -> Result<u64, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let stuck: Vec<(i64, i32, DateTime<Utc>)> = sqlx::query_as(
        "SELECT id, attempt_count, claimed_at FROM deliveries
          WHERE status = 'delivering' AND claimed_at < now() - make_interval(secs => $1)
          ORDER BY claimed_at LIMIT $2
          FOR UPDATE SKIP LOCKED",
    )
    .bind(stuck_after_secs)
    .bind(limit)
    .fetch_all(&mut *tx)
    .await?;
    if stuck.is_empty() {
        tx.rollback().await?;
        return Ok(0);
    }

    let mut requeue = Vec::new();
    for (id, attempt_count, claimed_at) in &stuck {
        // Only add a worker_lost row if the worker did not manage to log the attempt.
        sqlx::query(
            "INSERT INTO attempts (delivery_id, attempt_number, started_at, duration_ms, error_kind, error)
             SELECT $1, $2, $3, 0, 'worker_lost', 'worker stopped responding while delivering'
              WHERE NOT EXISTS (SELECT 1 FROM attempts WHERE delivery_id = $1 AND started_at >= $3)",
        )
        .bind(id)
        .bind(attempt_count)
        .bind(claimed_at)
        .execute(&mut *tx)
        .await?;

        if *attempt_count >= max_attempts {
            sqlx::query(
                "UPDATE deliveries SET status = 'dead', last_error = 'worker_lost', completed_at = now(),
                        claimed_at = NULL, updated_at = now()
                  WHERE id = $1 AND status = 'delivering'",
            )
            .bind(id)
            .execute(&mut *tx)
            .await?;
        } else {
            requeue.push(*id);
        }
    }

    sqlx::query(
        "UPDATE deliveries SET status = 'pending', claimed_at = NULL, last_error = 'worker_lost', updated_at = now()
          WHERE id = ANY($1) AND status = 'delivering'",
    )
    .bind(&requeue)
    .execute(&mut *tx)
    .await?;
    if queue_mode == QueueMode::Kafka {
        outbox::insert_for_deliveries(&mut tx, &requeue).await?;
    }
    tx.commit().await?;
    Ok(stuck.len() as u64)
}

/// Safety net (Kafka mode): a `pending` delivery untouched for a long time,
/// with no unpublished outbox row, has probably lost its Kafka message (for
/// example a broker that lost data). Publish it again. If the original message
/// was merely slow, the duplicate is skipped by the worker's claim.
pub async fn rescue_stale_pending(
    pool: &PgPool,
    older_than_secs: f64,
    limit: i64,
) -> Result<u64, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let ids: Vec<i64> = sqlx::query_scalar(
        "SELECT d.id FROM deliveries d
          WHERE d.status = 'pending' AND d.updated_at < now() - make_interval(secs => $1)
            AND NOT EXISTS (SELECT 1 FROM outbox o WHERE o.delivery_id = d.id AND o.published_at IS NULL)
          ORDER BY d.id LIMIT $2
          FOR UPDATE OF d SKIP LOCKED",
    )
    .bind(older_than_secs)
    .bind(limit)
    .fetch_all(&mut *tx)
    .await?;
    if ids.is_empty() {
        tx.rollback().await?;
        return Ok(0);
    }
    sqlx::query("UPDATE deliveries SET updated_at = now() WHERE id = ANY($1)")
        .bind(&ids)
        .execute(&mut *tx)
        .await?;
    outbox::insert_for_deliveries(&mut tx, &ids).await?;
    tx.commit().await?;
    Ok(ids.len() as u64)
}

// ---------------------------------------------------------------------------
// API side (tenant-scoped)
// ---------------------------------------------------------------------------

pub struct DeliveryFilter {
    pub endpoint_id: Option<Uuid>,
    pub event_id: Option<Uuid>,
    pub status: Option<String>,
}

pub async fn list(
    pool: &PgPool,
    tenant_id: Uuid,
    filter: &DeliveryFilter,
    before: Option<i64>,
    limit: i64,
) -> Result<Vec<DeliveryRow>, sqlx::Error> {
    let sql = format!(
        "SELECT {COLUMNS} FROM deliveries
          WHERE tenant_id = $1
            AND ($2::uuid IS NULL OR endpoint_id = $2)
            AND ($3::uuid IS NULL OR event_id = $3)
            AND ($4::text IS NULL OR status = $4)
            AND ($5::bigint IS NULL OR id < $5)
          ORDER BY id DESC LIMIT $6"
    );
    sqlx::query_as::<_, DeliveryRow>(sqlx::AssertSqlSafe(sql))
        .bind(tenant_id)
        .bind(filter.endpoint_id)
        .bind(filter.event_id)
        .bind(filter.status.as_deref())
        .bind(before)
        .bind(limit)
        .fetch_all(pool)
        .await
}

pub async fn get(
    pool: &PgPool,
    tenant_id: Uuid,
    id: i64,
) -> Result<Option<DeliveryRow>, sqlx::Error> {
    let sql = format!("SELECT {COLUMNS} FROM deliveries WHERE tenant_id = $1 AND id = $2");
    sqlx::query_as::<_, DeliveryRow>(sqlx::AssertSqlSafe(sql))
        .bind(tenant_id)
        .bind(id)
        .fetch_optional(pool)
        .await
}

pub async fn for_event(
    pool: &PgPool,
    tenant_id: Uuid,
    event_id: Uuid,
) -> Result<Vec<DeliveryRow>, sqlx::Error> {
    let sql = format!(
        "SELECT {COLUMNS} FROM deliveries WHERE tenant_id = $1 AND event_id = $2 ORDER BY id"
    );
    sqlx::query_as::<_, DeliveryRow>(sqlx::AssertSqlSafe(sql))
        .bind(tenant_id)
        .bind(event_id)
        .fetch_all(pool)
        .await
}

#[derive(Debug)]
pub enum ResendError {
    NotFound,
    /// The delivery is queued or in flight right now.
    InProgress,
    EndpointDisabled,
    Db(sqlx::Error),
}

impl From<sqlx::Error> for ResendError {
    fn from(e: sqlx::Error) -> Self {
        ResendError::Db(e)
    }
}

/// Start a fresh retry cycle: back to `pending` with the attempt counter reset
/// (the attempt history rows are kept).
pub async fn resend(
    pool: &PgPool,
    tenant_id: Uuid,
    id: i64,
    queue_mode: QueueMode,
) -> Result<(), ResendError> {
    let mut tx = pool.begin().await?;
    let row: Option<(String, String)> = sqlx::query_as(
        "SELECT d.status, e.status FROM deliveries d JOIN endpoints e ON e.id = d.endpoint_id
          WHERE d.tenant_id = $1 AND d.id = $2 FOR UPDATE OF d",
    )
    .bind(tenant_id)
    .bind(id)
    .fetch_optional(&mut *tx)
    .await?;
    let Some((delivery_status, endpoint_status)) = row else {
        return Err(ResendError::NotFound);
    };
    if endpoint_status != "active" {
        return Err(ResendError::EndpointDisabled);
    }
    if delivery_status == status::PENDING || delivery_status == status::DELIVERING {
        return Err(ResendError::InProgress);
    }
    let n = sqlx::query(
        "UPDATE deliveries SET status = 'pending', attempt_count = 0, next_attempt_at = NULL,
                completed_at = NULL, last_error = NULL, updated_at = now()
          WHERE tenant_id = $1 AND id = $2 AND status = $3",
    )
    .bind(tenant_id)
    .bind(id)
    .bind(&delivery_status)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    if n == 1 && queue_mode == QueueMode::Kafka {
        outbox::insert_for_deliveries(&mut tx, &[id]).await?;
    }
    tx.commit().await?;
    Ok(())
}

/// Re-queue an endpoint's `failed` and `dead` deliveries created since `since`.
/// Returns the number re-queued.
pub async fn recover(
    pool: &PgPool,
    tenant_id: Uuid,
    endpoint_id: Uuid,
    since: DateTime<Utc>,
    queue_mode: QueueMode,
) -> Result<u64, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let ids: Vec<i64> = sqlx::query_scalar(
        "UPDATE deliveries SET status = 'pending', attempt_count = 0, next_attempt_at = NULL,
                completed_at = NULL, last_error = NULL, updated_at = now()
          WHERE tenant_id = $1 AND endpoint_id = $2 AND created_at >= $3 AND status IN ('failed', 'dead')
         RETURNING id",
    )
    .bind(tenant_id)
    .bind(endpoint_id)
    .bind(since)
    .fetch_all(&mut *tx)
    .await?;
    if queue_mode == QueueMode::Kafka {
        let mut sorted = ids.clone();
        sorted.sort_unstable();
        outbox::insert_for_deliveries(&mut tx, &sorted).await?;
    }
    tx.commit().await?;
    Ok(ids.len() as u64)
}

// ---------------------------------------------------------------------------
// Gauges
// ---------------------------------------------------------------------------

pub struct Backlog {
    pub retry_scheduled: i64,
    pub oldest_overdue_secs: f64,
    pub dead: i64,
    pub pending: i64,
}

pub async fn backlog(pool: &PgPool) -> Result<Backlog, sqlx::Error> {
    let (retry_scheduled, oldest_overdue_secs): (i64, f64) = sqlx::query_as(
        "SELECT count(*),
                COALESCE(EXTRACT(EPOCH FROM now() - min(next_attempt_at) FILTER (WHERE next_attempt_at <= now())), 0)::float8
           FROM deliveries WHERE status = 'retry_scheduled'",
    )
    .fetch_one(pool)
    .await?;
    let pending: i64 =
        sqlx::query_scalar("SELECT count(*) FROM deliveries WHERE status = 'pending'")
            .fetch_one(pool)
            .await?;
    // `dead` has no partial index; this count is cheap enough at demo scale and
    // runs only every 15 s. At large scale it would be kept as a counter.
    let dead: i64 = sqlx::query_scalar("SELECT count(*) FROM deliveries WHERE status = 'dead'")
        .fetch_one(pool)
        .await?;
    Ok(Backlog {
        retry_scheduled,
        oldest_overdue_secs,
        dead,
        pending,
    })
}
