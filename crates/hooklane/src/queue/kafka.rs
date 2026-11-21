//! Kafka client setup: the producer (used only by the relay), the consumer
//! (used by workers), and topic creation (used by `hooklane migrate`).
//!
//! Locally and in Kubernetes the broker is Redpanda, which speaks the Kafka
//! protocol, so the same `rdkafka` (librdkafka) client works unchanged against
//! real Kafka, Redpanda, or a managed service.

use std::time::Duration;

use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::ConsumerContext;
use rdkafka::consumer::StreamConsumer;
use rdkafka::error::KafkaError;
use rdkafka::producer::FutureProducer;
use rdkafka::types::RDKafkaErrorCode;

use crate::config::Config;

/// Producer settings that make publishing safe:
/// * `acks=all` – the broker confirms only once the message is replicated, so
///   an acknowledged message survives a broker failure;
/// * `enable.idempotence=true` – the producer's own internal retries cannot
///   create duplicates or reorder messages within a partition.
pub fn producer(config: &Config) -> Result<FutureProducer, KafkaError> {
    ClientConfig::new()
        .set("bootstrap.servers", &config.kafka_brokers)
        .set("acks", "all")
        .set("enable.idempotence", "true")
        .set("linger.ms", "5")
        .set("message.timeout.ms", "30000")
        .set("client.id", "hooklane-relay")
        .create()
}

/// Consumer settings:
/// * `enable.auto.commit=false` – we commit offsets ourselves, only after a
///   message is fully handled (see `worker/offsets.rs`). Auto-commit would
///   commit on a timer, possibly before the work is done, losing it on a crash.
/// * `partition.assignment.strategy=cooperative-sticky` – when workers are
///   added or removed, only the partitions that must move are revoked, instead
///   of every worker stopping everything ("stop the world"). Important when an
///   autoscaler changes the worker count often.
/// * `statistics.interval.ms` – librdkafka reports consumer lag every few
///   seconds; we export it as a metric.
pub fn consumer<C: ConsumerContext + 'static>(
    config: &Config,
    context: C,
) -> Result<StreamConsumer<C>, KafkaError> {
    ClientConfig::new()
        .set("bootstrap.servers", &config.kafka_brokers)
        .set("group.id", &config.kafka_consumer_group)
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("auto.offset.reset", "earliest")
        .set("partition.assignment.strategy", "cooperative-sticky")
        .set("session.timeout.ms", "10000")
        .set("heartbeat.interval.ms", "2000")
        .set("statistics.interval.ms", "5000")
        .set("client.id", "hooklane-worker")
        .create_with_context(context)
}

/// Create the deliveries topic if it does not exist. Partition count is fixed
/// up front (64 by default): adding partitions later would change which
/// partition each endpoint hashes to, breaking per-endpoint ordering during the
/// transition.
pub async fn ensure_topic(config: &Config, replication: i32) -> anyhow::Result<()> {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", &config.kafka_brokers)
        .create()?;
    let topic = NewTopic::new(
        &config.kafka_topic,
        config.kafka_partitions,
        TopicReplication::Fixed(replication),
    );
    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(20)));
    let results = admin.create_topics([&topic], &opts).await?;
    for r in results {
        match r {
            Ok(name) => {
                tracing::info!(topic = %name, partitions = config.kafka_partitions, "created topic")
            }
            Err((name, RDKafkaErrorCode::TopicAlreadyExists)) => {
                tracing::info!(topic = %name, "topic already exists")
            }
            Err((name, code)) => anyhow::bail!("could not create topic {name}: {code}"),
        }
    }
    Ok(())
}

/// Retry `ensure_topic` with backoff while the broker starts up.
pub async fn ensure_topic_with_retry(config: &Config, replication: i32) -> anyhow::Result<()> {
    let mut delay = Duration::from_millis(500);
    for attempt in 1..=30 {
        match ensure_topic(config, replication).await {
            Ok(()) => return Ok(()),
            Err(e) if attempt < 30 => {
                tracing::warn!(error = %e, "kafka not ready, retrying");
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(5));
            }
            Err(e) => return Err(e),
        }
    }
    Ok(())
}
