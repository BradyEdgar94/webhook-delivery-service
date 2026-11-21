//! The attempt log: one row per HTTP try (or per lost try), kept forever so a
//! customer can see exactly what happened to a delivery.

use chrono::{DateTime, Utc};
use sqlx::{PgConnection, PgPool};

#[derive(Debug, Clone)]
pub struct NewAttempt {
    pub attempt_number: i32,
    pub started_at: DateTime<Utc>,
    pub duration_ms: i32,
    pub response_status: Option<i32>,
    /// Only the first 1 KB of the response body is stored.
    pub response_body_snippet: Option<String>,
    pub error_kind: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, sqlx::FromRow, serde::Serialize)]
pub struct AttemptRow {
    pub id: i64,
    pub delivery_id: i64,
    pub attempt_number: i32,
    pub started_at: DateTime<Utc>,
    pub duration_ms: i32,
    pub response_status: Option<i32>,
    pub response_body_snippet: Option<String>,
    pub error_kind: Option<String>,
    pub error: Option<String>,
}

pub async fn insert(
    conn: &mut PgConnection,
    delivery_id: i64,
    a: &NewAttempt,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO attempts (delivery_id, attempt_number, started_at, duration_ms, response_status,
                               response_body_snippet, error_kind, error)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
    )
    .bind(delivery_id)
    .bind(a.attempt_number)
    .bind(a.started_at)
    .bind(a.duration_ms)
    .bind(a.response_status)
    .bind(a.response_body_snippet.as_deref())
    .bind(a.error_kind.as_deref())
    .bind(a.error.as_deref())
    .execute(conn)
    .await?;
    Ok(())
}

/// Caller must already have checked the delivery belongs to the tenant.
pub async fn for_delivery(pool: &PgPool, delivery_id: i64) -> Result<Vec<AttemptRow>, sqlx::Error> {
    sqlx::query_as::<_, AttemptRow>(
        "SELECT id, delivery_id, attempt_number, started_at, duration_ms, response_status,
                response_body_snippet, error_kind, error
           FROM attempts WHERE delivery_id = $1 ORDER BY id",
    )
    .bind(delivery_id)
    .fetch_all(pool)
    .await
}
