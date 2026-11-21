//! Tenants and their API keys.

use sqlx::PgPool;
use uuid::Uuid;

use crate::crypto::NewApiKey;

pub async fn create_tenant_with_key(
    pool: &PgPool,
    name: &str,
    key: &NewApiKey,
) -> Result<Uuid, sqlx::Error> {
    let tenant_id = Uuid::now_v7();
    // Tenant and its first key together: never a tenant nobody can log in as.
    let mut tx = pool.begin().await?;
    sqlx::query("INSERT INTO tenants (id, name) VALUES ($1, $2)")
        .bind(tenant_id)
        .bind(name)
        .execute(&mut *tx)
        .await?;
    sqlx::query("INSERT INTO api_keys (id, tenant_id, key_hash, prefix) VALUES ($1, $2, $3, $4)")
        .bind(Uuid::now_v7())
        .bind(tenant_id)
        .bind(&key.hash)
        .bind(&key.prefix)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(tenant_id)
}

/// Look up the tenant owning a (hashed) API key. Revoked keys do not match.
pub async fn tenant_for_key_hash(
    pool: &PgPool,
    key_hash: &[u8],
) -> Result<Option<Uuid>, sqlx::Error> {
    sqlx::query_scalar("SELECT tenant_id FROM api_keys WHERE key_hash = $1 AND revoked_at IS NULL")
        .bind(key_hash)
        .fetch_optional(pool)
        .await
}
