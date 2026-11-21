//! API key authentication.
//!
//! `Authorization: Bearer <key>`. We hash the presented key and look the hash
//! up; the plain key is never stored. Successful lookups are cached in memory
//! for 60 seconds so ingest does not hit the database for auth on every
//! request. (Trade-off: a revoked key keeps working for up to a minute on each
//! API replica.)
//!
//! Using `Tenant` as a handler argument is what enforces authentication: axum
//! runs `from_request_parts` before the handler, and a handler cannot get a
//! tenant id any other way. Every query then filters by that id.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use uuid::Uuid;

use super::AppState;
use super::error::ApiError;
use crate::crypto::hash_api_key;
use crate::db;

const CACHE_TTL: Duration = Duration::from_secs(60);
const CACHE_MAX_ENTRIES: usize = 10_000;

/// key hash -> (tenant id, cached at)
#[derive(Default)]
pub struct KeyCache {
    entries: Mutex<HashMap<Vec<u8>, (Uuid, Instant)>>,
}

impl KeyCache {
    fn get(&self, hash: &[u8]) -> Option<Uuid> {
        let entries = self.entries.lock().ok()?;
        match entries.get(hash) {
            Some((tenant, at)) if at.elapsed() < CACHE_TTL => Some(*tenant),
            _ => None,
        }
    }

    fn put(&self, hash: Vec<u8>, tenant: Uuid) {
        if let Ok(mut entries) = self.entries.lock() {
            if entries.len() >= CACHE_MAX_ENTRIES {
                entries.retain(|_, (_, at)| at.elapsed() < CACHE_TTL);
            }
            entries.insert(hash, (tenant, Instant::now()));
        }
    }
}

/// The authenticated tenant for this request.
#[derive(Debug, Clone, Copy)]
pub struct Tenant(pub Uuid);

impl FromRequestParts<AppState> for Tenant {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let header = parts
            .headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .ok_or_else(ApiError::unauthorized)?;
        let key = header
            .strip_prefix("Bearer ")
            .ok_or_else(ApiError::unauthorized)?
            .trim();
        if key.is_empty() {
            return Err(ApiError::unauthorized());
        }
        let hash = hash_api_key(key);
        if let Some(tenant) = state.key_cache.get(&hash) {
            return Ok(Tenant(tenant));
        }
        let tenant = db::tenants::tenant_for_key_hash(&state.pool, &hash)
            .await?
            .ok_or_else(ApiError::unauthorized)?;
        state.key_cache.put(hash, tenant);
        tracing::Span::current().record("tenant_id", tracing::field::display(tenant));
        Ok(Tenant(tenant))
    }
}
