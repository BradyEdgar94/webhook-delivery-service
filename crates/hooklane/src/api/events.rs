//! `POST /v1/events` (ingest) and the event log.

use std::sync::OnceLock;

use axum::Json;
use axum::body::Bytes;
use axum::extract::rejection::BytesRejection;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use regex::Regex;
use serde::Deserialize;
use uuid::Uuid;

use super::auth::Tenant;
use super::error::ApiError;
use super::pagination::{Page, PageParams, decode_cursor, page};
use super::{AppState, MAX_PAYLOAD_BYTES};
use crate::db::{self, events::EventRow};
use crate::{metrics, telemetry};

pub fn event_type_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    // A literal pattern; it is known to compile.
    RE.get_or_init(|| Regex::new(r"^[a-zA-Z0-9_.-]{1,128}$").unwrap_or_else(|_| unreachable!()))
}

#[derive(Debug, Deserialize)]
struct IngestBody {
    #[serde(rename = "type")]
    event_type: String,
    data: serde_json::Value,
}

/// Validate everything at the boundary, then save in one transaction.
#[tracing::instrument(name = "api.ingest", skip_all, fields(tenant_id = %tenant.0, event_id = tracing::field::Empty))]
pub async fn ingest(
    State(state): State<AppState>,
    tenant: Tenant,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> Result<(StatusCode, Json<serde_json::Value>), ApiError> {
    // 413 if the body exceeded the limit, 400 for other read errors.
    let body = body.map_err(|e| {
        if e.status() == StatusCode::PAYLOAD_TOO_LARGE {
            ApiError::new(
                StatusCode::PAYLOAD_TOO_LARGE,
                "payload_too_large",
                "payload must be at most 256 KB",
            )
        } else {
            ApiError::bad_request(e.body_text())
        }
    })?;
    let parsed: IngestBody = match serde_json::from_slice(&body) {
        Ok(b) => b,
        Err(e) if e.is_data() => {
            return Err(ApiError::unprocessable(format!("invalid event: {e}")));
        }
        Err(e) => return Err(ApiError::bad_request(format!("invalid JSON: {e}"))),
    };
    if !event_type_regex().is_match(&parsed.event_type) {
        return Err(ApiError::unprocessable(
            "type must match ^[a-zA-Z0-9_.-]{1,128}$",
        ));
    }
    if db::events::is_system_event_type(&parsed.event_type) {
        return Err(ApiError::unprocessable(
            "event types starting with 'hooklane.' are reserved",
        ));
    }
    let payload_len = serde_json::to_vec(&parsed.data)
        .map(|v| v.len())
        .unwrap_or(usize::MAX);
    if payload_len > MAX_PAYLOAD_BYTES {
        return Err(ApiError::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "payload_too_large",
            "data must be at most 256 KB",
        ));
    }
    let idempotency_key = match headers.get("idempotency-key") {
        None => None,
        Some(v) => {
            let s = v
                .to_str()
                .map_err(|_| ApiError::bad_request("Idempotency-Key must be ASCII"))?;
            if s.is_empty() || s.len() > 255 {
                return Err(ApiError::bad_request(
                    "Idempotency-Key must be 1-255 characters",
                ));
            }
            Some(s.to_string())
        }
    };

    let traceparent = telemetry::current_traceparent();
    let result = db::events::ingest(
        &state.pool,
        tenant.0,
        &parsed.event_type,
        &parsed.data,
        idempotency_key.as_deref(),
        state.config.queue_mode,
        traceparent.as_deref(),
    )
    .await?;

    tracing::Span::current().record("event_id", tracing::field::display(result.event_id));
    if result.created {
        metrics::get().events_ingested.add(1, &[]);
        tracing::debug!(deliveries = result.deliveries, "event accepted");
    }
    Ok((
        StatusCode::ACCEPTED,
        Json(serde_json::json!({
            "id": result.event_id,
            "duplicate": !result.created,
        })),
    ))
}

pub async fn list(
    State(state): State<AppState>,
    tenant: Tenant,
    Query(params): Query<PageParams>,
) -> Result<Json<Page<EventRow>>, ApiError> {
    let limit = params.limit()?;
    let before = decode_cursor::<Uuid>(params.cursor.as_deref())?;
    let rows = db::events::list(&state.pool, tenant.0, before, limit + 1).await?;
    Ok(Json(page(rows, limit, |r| r.id.to_string(), |r| r)))
}

pub async fn get_one(
    State(state): State<AppState>,
    tenant: Tenant,
    Path(id): Path<Uuid>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let event = db::events::get(&state.pool, tenant.0, id)
        .await?
        .ok_or_else(|| ApiError::not_found("event"))?;
    let deliveries = db::deliveries::for_event(&state.pool, tenant.0, id).await?;
    let mut v = serde_json::to_value(&event).map_err(|_| ApiError::internal())?;
    v["deliveries"] = serde_json::to_value(&deliveries).map_err(|_| ApiError::internal())?;
    Ok(Json(v))
}
