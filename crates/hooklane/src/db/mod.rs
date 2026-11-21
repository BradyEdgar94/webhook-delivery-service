//! Database access. Postgres is the source of truth for everything: Kafka is
//! only a way of handing work to workers, and can always be rebuilt from here.
//!
//! One file per group of tables. Every query takes a `tenant_id` where the
//! data belongs to a tenant (tenant isolation, guarantee 5). Queries are
//! written with `sqlx::query` / `query_as` (checked at runtime rather than at
//! compile time) so building the Docker image never needs a live database.
//!
//! **Conditional updates.** Every change of a delivery's status is written as
//! `UPDATE ... WHERE id = $1 AND status = '<expected>'`, and the caller checks
//! how many rows changed. If two processes race (a duplicate Kafka message, a
//! reaper and a slow worker...), exactly one of them sees "1 row updated" and
//! proceeds; the other sees 0 and backs off. This is how duplicates are made
//! harmless without any locks held across network calls.

pub mod attempts;
pub mod deliveries;
pub mod endpoints;
pub mod events;
pub mod outbox;
pub mod tenants;

use std::time::Duration;

use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

/// Embedded migrations from the repository's `migrations/` directory.
pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../migrations");

/// Delivery statuses, as stored in `deliveries.status`.
pub mod status {
    pub const PENDING: &str = "pending";
    pub const DELIVERING: &str = "delivering";
    pub const SUCCEEDED: &str = "succeeded";
    pub const RETRY_SCHEDULED: &str = "retry_scheduled";
    pub const FAILED: &str = "failed";
    pub const DEAD: &str = "dead";

    pub const ALL: [&str; 6] = [
        PENDING,
        DELIVERING,
        SUCCEEDED,
        RETRY_SCHEDULED,
        FAILED,
        DEAD,
    ];
}

/// Connect to Postgres, retrying with backoff instead of crashing, so a role
/// started before the database (or during a database restart) just waits.
pub async fn connect_with_retry(url: &str, max_connections: u32) -> PgPool {
    let mut delay = Duration::from_millis(250);
    loop {
        let result = PgPoolOptions::new()
            .max_connections(max_connections)
            .acquire_timeout(Duration::from_secs(5))
            .connect(url)
            .await;
        match result {
            Ok(pool) => return pool,
            Err(e) => {
                tracing::warn!(error = %e, retry_in_ms = delay.as_millis() as u64, "postgres not reachable, retrying");
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(5));
            }
        }
    }
}

/// Run all pending migrations.
pub async fn migrate(pool: &PgPool) -> Result<(), sqlx::migrate::MigrateError> {
    MIGRATOR.run(pool).await
}
