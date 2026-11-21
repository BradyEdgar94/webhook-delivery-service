//! The queue between Postgres and the workers, behind a small abstraction.
//!
//! Two modes, chosen by `QUEUE_MODE`:
//!
//! * **`kafka`** (default): the ingest transaction writes outbox rows; the
//!   relay publishes them to the `hooklane.deliveries` topic (64 partitions,
//!   key = endpoint id); workers consume with per-endpoint lanes. Because the
//!   key is the endpoint id, all of one endpoint's deliveries are in one
//!   partition and one worker owns that endpoint at a time.
//! * **`postgres`**: no outbox, no relay, no Kafka. Workers claim `pending`
//!   rows straight from `deliveries` with `FOR UPDATE SKIP LOCKED`. Simpler,
//!   and kept so the two designs can be load-tested against each other.
//!   **Known limit:** without partition ownership, two workers can serve the
//!   same endpoint at once, so the in-memory rate limit and circuit breaker are
//!   only approximate (each worker enforces its own). That is one of the
//!   reasons for Kafka.
//!
//! In both modes Postgres is the source of truth and the conditional claim is
//! what prevents duplicate sends.

pub mod kafka;
pub mod postgres;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// The message carried on the queue. Deliberately tiny: just ids. The worker
/// loads everything else from Postgres at claim time, so a message can never
/// carry stale data, and replaying one is harmless.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveryMessage {
    pub delivery_id: i64,
    pub endpoint_id: Uuid,
    pub tenant_id: Uuid,
    pub event_id: Uuid,
    /// W3C trace context; travels as a Kafka header, not in the JSON body.
    #[serde(skip)]
    pub traceparent: Option<String>,
}
