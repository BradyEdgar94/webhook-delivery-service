//! Administrative commands: `migrate` and `admin create-tenant`.

use sqlx::PgPool;

use crate::config::{Config, QueueMode};
use crate::crypto::generate_api_key;
use crate::{db, queue};

/// Run database migrations and, in Kafka mode, create the topic.
pub async fn migrate(config: &Config, pool: &PgPool, kafka_replication: i32) -> anyhow::Result<()> {
    db::migrate(pool).await?;
    tracing::info!("database migrations applied");
    if config.queue_mode == QueueMode::Kafka {
        queue::kafka::ensure_topic_with_retry(config, kafka_replication).await?;
    }
    Ok(())
}

/// Create a tenant and its first API key. The key is returned once and never
/// stored in plain text.
pub async fn create_tenant(pool: &PgPool, name: &str) -> anyhow::Result<(uuid::Uuid, String)> {
    let key = generate_api_key();
    let tenant_id = db::tenants::create_tenant_with_key(pool, name, &key).await?;
    Ok((tenant_id, key.key))
}
