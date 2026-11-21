//! Postgres queue mode: the "queue" is simply the `pending` rows of the
//! `deliveries` table, claimed in batches with `FOR UPDATE SKIP LOCKED` (see
//! `db::deliveries::claim_batch`). The polling loop lives in
//! `worker/postgres_loop.rs`.
//!
//! This is a perfectly respectable design at moderate scale (it is how many
//! job queues work). Its costs, measured in docs/RESULTS.md: every poll is a
//! write-heavy query on the hottest table, and there is no partition ownership,
//! so per-endpoint protections are only approximate across workers.

use std::time::Instant;

use sqlx::PgPool;

use super::DeliveryMessage;
use crate::worker::process::PreClaimed;

/// Claim up to `limit` pending deliveries; each comes back already claimed.
pub async fn claim(
    pool: &PgPool,
    limit: i64,
) -> Result<Vec<(DeliveryMessage, PreClaimed)>, sqlx::Error> {
    let rows = crate::db::deliveries::claim_batch(pool, limit).await?;
    let now = Instant::now();
    Ok(rows
        .into_iter()
        .map(|d| {
            let msg = DeliveryMessage {
                delivery_id: d.id,
                endpoint_id: d.endpoint_id,
                tenant_id: d.tenant_id,
                event_id: d.event_id,
                traceparent: None,
            };
            (
                msg,
                PreClaimed {
                    delivery: d,
                    claimed_at: now,
                },
            )
        })
        .collect())
}
