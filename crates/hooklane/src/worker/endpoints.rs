//! Per-worker, in-memory state about endpoints:
//!
//! * [`EndpointCache`] – endpoint rows (with decrypted secrets) cached for
//!   ~30 s, so a busy endpoint does not cost a database read per delivery.
//!   Consequence: an endpoint edit (new URL, disable) reaches workers within
//!   30 s, not instantly.
//! * [`Guards`] – the protection state for each endpoint: its token bucket,
//!   circuit breaker and "paused until" time (after a 429 with `Retry-After`).
//!   See `delivery/breaker.rs` for why this can live in memory.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chrono::Utc;
use sqlx::PgPool;
use uuid::Uuid;

use crate::crypto::SecretBox;
use crate::db;
use crate::delivery::breaker::{BreakerSettings, CircuitBreaker, Decision};
use crate::delivery::ratelimit::TokenBucket;
use crate::delivery::retry::BreakerSignal;

const CACHE_TTL: Duration = Duration::from_secs(30);
/// Forget protection state for endpoints not seen for this long.
const GUARD_IDLE_EXPIRY: Duration = Duration::from_secs(15 * 60);

/// What the worker needs to know about an endpoint.
#[derive(Debug)]
pub struct EndpointInfo {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub url: String,
    pub active: bool,
    pub rate_limit_per_sec: u32,
    pub failing: bool,
    pub secret: Vec<u8>,
    /// Present only while a rotation's 24-hour overlap window is open.
    pub previous_secret: Option<Vec<u8>>,
}

#[derive(Debug, thiserror::Error)]
pub enum LookupError {
    #[error(transparent)]
    Db(#[from] sqlx::Error),
    #[error(transparent)]
    Crypto(#[from] crate::crypto::CryptoError),
}

pub struct EndpointCache {
    pool: PgPool,
    secrets: SecretBox,
    entries: Mutex<HashMap<Uuid, (Instant, Arc<EndpointInfo>)>>,
}

impl EndpointCache {
    pub fn new(pool: PgPool, secrets: SecretBox) -> EndpointCache {
        EndpointCache {
            pool,
            secrets,
            entries: Mutex::new(HashMap::new()),
        }
    }

    /// `Ok(None)` means the endpoint was deleted.
    pub async fn get(&self, id: Uuid) -> Result<Option<Arc<EndpointInfo>>, LookupError> {
        if let Ok(entries) = self.entries.lock()
            && let Some((at, info)) = entries.get(&id)
            && at.elapsed() < CACHE_TTL
        {
            return Ok(Some(info.clone()));
        }
        let Some(row) = db::endpoints::get_for_worker(&self.pool, id).await? else {
            return Ok(None);
        };
        let secret = self.secrets.decrypt(&row.secret_ciphertext)?;
        let previous_secret = match (
            &row.previous_secret_ciphertext,
            row.previous_secret_expires_at,
        ) {
            (Some(ct), Some(exp)) if exp > Utc::now() => Some(self.secrets.decrypt(ct)?),
            _ => None,
        };
        let info = Arc::new(EndpointInfo {
            id: row.id,
            tenant_id: row.tenant_id,
            url: row.url,
            active: row.status == "active",
            rate_limit_per_sec: row.rate_limit_per_sec.max(1) as u32,
            failing: row.failing_since.is_some(),
            secret,
            previous_secret,
        });
        if let Ok(mut entries) = self.entries.lock() {
            if entries.len() > 50_000 {
                entries.retain(|_, (at, _)| at.elapsed() < CACHE_TTL);
            }
            entries.insert(id, (Instant::now(), info.clone()));
        }
        Ok(Some(info))
    }

    /// Drop a cached entry (e.g. after this worker disabled the endpoint).
    pub fn invalidate(&self, id: Uuid) {
        if let Ok(mut entries) = self.entries.lock() {
            entries.remove(&id);
        }
    }
}

/// Protection state for one endpoint.
struct Guard {
    bucket: TokenBucket,
    breaker: CircuitBreaker,
    /// Set by a 429 with Retry-After: don't call the endpoint before this.
    paused_until: Option<Instant>,
    /// What we last wrote to `endpoints.failing_since` (None = not known yet).
    failing_known: Option<bool>,
    last_used: Instant,
}

/// Should this delivery go ahead?
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GuardDecision {
    Go {
        probe: bool,
    },
    Park {
        retry_in: Duration,
        reason: &'static str,
    },
}

pub struct Guards {
    settings: BreakerSettings,
    map: Mutex<HashMap<Uuid, Guard>>,
}

impl Guards {
    pub fn new(settings: BreakerSettings) -> Guards {
        Guards {
            settings,
            map: Mutex::new(HashMap::new()),
        }
    }

    /// Run `f` on the endpoint's guard, creating it if needed. The lock is a
    /// plain std Mutex held only for a few instructions, never across `.await`.
    fn with<R>(&self, id: Uuid, rate: u32, f: impl FnOnce(&mut Guard) -> R) -> R {
        let now = Instant::now();
        let mut map = match self.map.lock() {
            Ok(m) => m,
            Err(poisoned) => poisoned.into_inner(),
        };
        let guard = map.entry(id).or_insert_with(|| Guard {
            bucket: TokenBucket::new(rate, now),
            breaker: CircuitBreaker::new(self.settings),
            paused_until: None,
            failing_known: None,
            last_used: now,
        });
        if guard.bucket.rate_per_sec() != rate {
            // The endpoint's configured rate changed: start a new bucket.
            guard.bucket = TokenBucket::new(rate, now);
        }
        guard.last_used = now;
        f(guard)
    }

    /// Check the 429 pause and the circuit breaker.
    pub fn check(&self, id: Uuid, rate: u32) -> GuardDecision {
        let now = Instant::now();
        self.with(id, rate, |g| {
            if let Some(until) = g.paused_until {
                if now < until {
                    return GuardDecision::Park {
                        retry_in: until - now,
                        reason: "endpoint_rate_limited",
                    };
                }
                g.paused_until = None;
            }
            match g.breaker.check(now) {
                Decision::Allow => GuardDecision::Go { probe: false },
                Decision::AllowProbe => GuardDecision::Go { probe: true },
                Decision::Reject { retry_in } => GuardDecision::Park {
                    retry_in,
                    reason: "circuit_open",
                },
            }
        })
    }

    /// Take a rate-limit token, or learn how long to wait for one.
    pub fn take_token(&self, id: Uuid, rate: u32) -> Result<(), Duration> {
        self.with(id, rate, |g| g.bucket.try_take(Instant::now()))
    }

    pub fn cancel_probe(&self, id: Uuid, rate: u32) {
        self.with(id, rate, |g| g.breaker.cancel_probe());
    }

    pub fn record(&self, id: Uuid, rate: u32, signal: BreakerSignal, pause_for: Option<Duration>) {
        let now = Instant::now();
        self.with(id, rate, |g| {
            g.breaker.record(now, signal);
            if let Some(d) = pause_for {
                g.paused_until = Some(now + d);
            }
        });
    }

    /// Returns true if `failing_since` needs writing to become `failing`.
    pub fn failing_changed(&self, id: Uuid, rate: u32, db_value: bool, failing: bool) -> bool {
        self.with(id, rate, |g| {
            let known = g.failing_known.unwrap_or(db_value);
            g.failing_known = Some(failing);
            known != failing
        })
    }

    pub fn open_breakers(&self) -> i64 {
        match self.map.lock() {
            Ok(m) => m.values().filter(|g| g.breaker.is_open()).count() as i64,
            Err(_) => 0,
        }
    }

    pub fn expire_idle(&self) {
        if let Ok(mut m) = self.map.lock() {
            m.retain(|_, g| g.last_used.elapsed() < GUARD_IDLE_EXPIRY || g.breaker.is_open());
        }
    }
}
