//! Endpoints: the customer URLs that receive webhooks.
//!
//! Every function here that a tenant can reach through the API takes the
//! `tenant_id` and puts it in the `WHERE` clause. Looking an endpoint up by id
//! alone would let tenant A read tenant B's endpoint by guessing its id.

use chrono::{DateTime, Utc};
use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

use super::{events, status};
use crate::config::QueueMode;

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct EndpointRow {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub url: String,
    pub description: String,
    pub event_types: Option<Vec<String>>,
    pub secret_ciphertext: Vec<u8>,
    pub previous_secret_ciphertext: Option<Vec<u8>>,
    pub previous_secret_expires_at: Option<DateTime<Utc>>,
    pub status: String,
    pub disabled_reason: Option<String>,
    pub rate_limit_per_sec: i32,
    pub failing_since: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

const COLUMNS: &str = "id, tenant_id, url, description, event_types, secret_ciphertext, \
     previous_secret_ciphertext, previous_secret_expires_at, status, disabled_reason, \
     rate_limit_per_sec, failing_since, created_at, updated_at";

pub struct NewEndpoint<'a> {
    pub tenant_id: Uuid,
    pub url: &'a str,
    pub description: &'a str,
    pub event_types: Option<Vec<String>>,
    pub secret_ciphertext: Vec<u8>,
    pub rate_limit_per_sec: i32,
}

pub async fn create(pool: &PgPool, new: NewEndpoint<'_>) -> Result<EndpointRow, sqlx::Error> {
    let sql = format!(
        "INSERT INTO endpoints (id, tenant_id, url, description, event_types, secret_ciphertext, rate_limit_per_sec)
         VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING {COLUMNS}"
    );
    sqlx::query_as::<_, EndpointRow>(sqlx::AssertSqlSafe(sql))
        .bind(Uuid::now_v7())
        .bind(new.tenant_id)
        .bind(new.url)
        .bind(new.description)
        .bind(new.event_types)
        .bind(new.secret_ciphertext)
        .bind(new.rate_limit_per_sec)
        .fetch_one(pool)
        .await
}

pub async fn get(
    pool: &PgPool,
    tenant_id: Uuid,
    id: Uuid,
) -> Result<Option<EndpointRow>, sqlx::Error> {
    let sql = format!("SELECT {COLUMNS} FROM endpoints WHERE tenant_id = $1 AND id = $2");
    sqlx::query_as::<_, EndpointRow>(sqlx::AssertSqlSafe(sql))
        .bind(tenant_id)
        .bind(id)
        .fetch_optional(pool)
        .await
}

/// Worker-side lookup. Not tenant-scoped because the worker is not acting on
/// behalf of a tenant: the id comes from our own queue message.
pub async fn get_for_worker(pool: &PgPool, id: Uuid) -> Result<Option<EndpointRow>, sqlx::Error> {
    let sql = format!("SELECT {COLUMNS} FROM endpoints WHERE id = $1");
    sqlx::query_as::<_, EndpointRow>(sqlx::AssertSqlSafe(sql))
        .bind(id)
        .fetch_optional(pool)
        .await
}

/// Keyset pagination: newest first, `before` is the last id of the previous page.
pub async fn list(
    pool: &PgPool,
    tenant_id: Uuid,
    before: Option<Uuid>,
    limit: i64,
) -> Result<Vec<EndpointRow>, sqlx::Error> {
    let sql = format!(
        "SELECT {COLUMNS} FROM endpoints
         WHERE tenant_id = $1 AND ($2::uuid IS NULL OR id < $2)
         ORDER BY id DESC LIMIT $3"
    );
    sqlx::query_as::<_, EndpointRow>(sqlx::AssertSqlSafe(sql))
        .bind(tenant_id)
        .bind(before)
        .bind(limit)
        .fetch_all(pool)
        .await
}

/// Partial update. `None` = leave unchanged. Re-enabling clears the disabled
/// reason and the failure clock.
pub struct EndpointPatch {
    pub url: Option<String>,
    pub description: Option<String>,
    pub event_types: Option<Option<Vec<String>>>,
    pub rate_limit_per_sec: Option<i32>,
    pub enable: bool,
}

pub async fn update(
    pool: &PgPool,
    tenant_id: Uuid,
    id: Uuid,
    patch: EndpointPatch,
) -> Result<Option<EndpointRow>, sqlx::Error> {
    let sql = format!(
        "UPDATE endpoints SET
            url = COALESCE($3, url),
            description = COALESCE($4, description),
            event_types = CASE WHEN $5 THEN $6 ELSE event_types END,
            rate_limit_per_sec = COALESCE($7, rate_limit_per_sec),
            status = CASE WHEN $8 THEN 'active' ELSE status END,
            disabled_reason = CASE WHEN $8 THEN NULL ELSE disabled_reason END,
            failing_since = CASE WHEN $8 THEN NULL ELSE failing_since END,
            updated_at = now()
         WHERE tenant_id = $1 AND id = $2
         RETURNING {COLUMNS}"
    );
    let set_types = patch.event_types.is_some();
    sqlx::query_as::<_, EndpointRow>(sqlx::AssertSqlSafe(sql))
        .bind(tenant_id)
        .bind(id)
        .bind(patch.url)
        .bind(patch.description)
        .bind(set_types)
        .bind(patch.event_types.flatten())
        .bind(patch.rate_limit_per_sec)
        .bind(patch.enable)
        .fetch_optional(pool)
        .await
}

pub async fn delete(pool: &PgPool, tenant_id: Uuid, id: Uuid) -> Result<bool, sqlx::Error> {
    let r = sqlx::query("DELETE FROM endpoints WHERE tenant_id = $1 AND id = $2")
        .bind(tenant_id)
        .bind(id)
        .execute(pool)
        .await?;
    Ok(r.rows_affected() == 1)
}

/// Replace the secret, keeping the old one valid until `previous_expires_at`.
pub async fn rotate_secret(
    pool: &PgPool,
    tenant_id: Uuid,
    id: Uuid,
    new_ciphertext: Vec<u8>,
    previous_expires_at: DateTime<Utc>,
) -> Result<Option<EndpointRow>, sqlx::Error> {
    let sql = format!(
        "UPDATE endpoints SET
            previous_secret_ciphertext = secret_ciphertext,
            previous_secret_expires_at = $4,
            secret_ciphertext = $3,
            updated_at = now()
         WHERE tenant_id = $1 AND id = $2
         RETURNING {COLUMNS}"
    );
    sqlx::query_as::<_, EndpointRow>(sqlx::AssertSqlSafe(sql))
        .bind(tenant_id)
        .bind(id)
        .bind(new_ciphertext)
        .bind(previous_expires_at)
        .fetch_optional(pool)
        .await
}

/// Record that the endpoint started failing. Only writes when the value
/// actually changes (`failing_since IS NULL`), so a busy, healthy endpoint's
/// row is not rewritten on every delivery.
pub async fn mark_failing(pool: &PgPool, id: Uuid) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE endpoints SET failing_since = now() WHERE id = $1 AND failing_since IS NULL",
    )
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn mark_healthy(pool: &PgPool, id: Uuid) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE endpoints SET failing_since = NULL WHERE id = $1 AND failing_since IS NOT NULL",
    )
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Disable an endpoint, fail its undelivered deliveries, and (unless
/// `suppress_system_event`) emit `hooklane.endpoint.disabled` to the tenant.
/// Runs on the caller's transaction so all of it happens or none of it does.
///
/// Returns false if the endpoint was already disabled (nothing done).
pub async fn disable_in_tx(
    conn: &mut PgConnection,
    endpoint_id: Uuid,
    reason: &str,
    queue_mode: QueueMode,
    suppress_system_event: bool,
) -> Result<bool, sqlx::Error> {
    // Conditional: only the first of several racing disablers does the work.
    let row: Option<(Uuid, String)> = sqlx::query_as(
        "UPDATE endpoints SET status = 'disabled', disabled_reason = $2, updated_at = now()
         WHERE id = $1 AND status = 'active'
         RETURNING tenant_id, url",
    )
    .bind(endpoint_id)
    .bind(reason)
    .fetch_optional(&mut *conn)
    .await?;
    let Some((tenant_id, url)) = row else {
        return Ok(false);
    };

    // Deliveries still waiting would otherwise sit in the queue forever.
    // They become `failed` with a clear reason; POST /recover brings them back.
    let failed = sqlx::query(
        "UPDATE deliveries SET status = 'failed', last_error = 'endpoint_disabled',
                completed_at = now(), next_attempt_at = NULL, updated_at = now()
         WHERE endpoint_id = $1 AND status IN ($2, $3)",
    )
    .bind(endpoint_id)
    .bind(status::PENDING)
    .bind(status::RETRY_SCHEDULED)
    .execute(&mut *conn)
    .await?
    .rows_affected();

    if !suppress_system_event {
        let data = serde_json::json!({
            "endpoint_id": endpoint_id,
            "url": url,
            "reason": reason,
            "failed_deliveries": failed,
        });
        events::emit_system_event(
            conn,
            tenant_id,
            events::SYSTEM_ENDPOINT_DISABLED,
            data,
            queue_mode,
        )
        .await?;
    }
    Ok(true)
}

/// Endpoints that have been failing continuously for longer than `older_than`.
/// `SKIP LOCKED` lets several scheduler replicas run this safely.
pub async fn lock_long_failing(
    conn: &mut PgConnection,
    older_than_secs: f64,
    limit: i64,
) -> Result<Vec<Uuid>, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT id FROM endpoints
         WHERE status = 'active' AND failing_since IS NOT NULL
           AND failing_since < now() - make_interval(secs => $1)
         ORDER BY failing_since LIMIT $2
         FOR UPDATE SKIP LOCKED",
    )
    .bind(older_than_secs)
    .bind(limit)
    .fetch_all(conn)
    .await
}
