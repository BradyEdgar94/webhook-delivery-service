//! Endpoint management: CRUD, secrets, rotation and recovery.

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use uuid::Uuid;

use super::AppState;
use super::auth::Tenant;
use super::error::ApiError;
use super::events::event_type_regex;
use super::pagination::{Page, PageParams, decode_cursor, page};
use crate::crypto::{format_signing_secret, generate_signing_secret};
use crate::db::{self, endpoints::EndpointRow};
use crate::delivery::ssrf;

/// How long the previous secret keeps working after a rotation.
const ROTATION_OVERLAP_HOURS: i64 = 24;

/// What the API shows for an endpoint (never the encrypted secret columns).
#[derive(Debug, Serialize)]
pub struct EndpointView {
    pub id: Uuid,
    pub url: String,
    pub description: String,
    pub event_types: Option<Vec<String>>,
    pub status: String,
    pub disabled_reason: Option<String>,
    pub rate_limit_per_sec: i32,
    pub failing_since: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl From<EndpointRow> for EndpointView {
    fn from(r: EndpointRow) -> Self {
        EndpointView {
            id: r.id,
            url: r.url,
            description: r.description,
            event_types: r.event_types,
            status: r.status,
            disabled_reason: r.disabled_reason,
            rate_limit_per_sec: r.rate_limit_per_sec,
            failing_since: r.failing_since,
            created_at: r.created_at,
            updated_at: r.updated_at,
        }
    }
}

fn validate_url(state: &AppState, url: &str) -> Result<(), ApiError> {
    let policy = ssrf::Policy {
        allow_http: state.config.allow_http,
        allow_private: state.config.allow_private_endpoints,
    };
    ssrf::check_url(url, policy)
        .map(|_| ())
        .map_err(|e| ApiError::unprocessable(e.to_string()))
}

fn validate_event_types(types: &Option<Vec<String>>) -> Result<(), ApiError> {
    if let Some(list) = types {
        if list.len() > 100 {
            return Err(ApiError::unprocessable("at most 100 event types"));
        }
        if let Some(bad) = list.iter().find(|t| !event_type_regex().is_match(t)) {
            return Err(ApiError::unprocessable(format!(
                "invalid event type: {bad}"
            )));
        }
    }
    Ok(())
}

fn validate_rate(rate: i32) -> Result<(), ApiError> {
    if (1..=10_000).contains(&rate) {
        Ok(())
    } else {
        Err(ApiError::unprocessable(
            "rate_limit_per_sec must be between 1 and 10000",
        ))
    }
}

#[derive(Debug, Deserialize)]
pub struct CreateEndpoint {
    url: String,
    #[serde(default)]
    description: String,
    event_types: Option<Vec<String>>,
    rate_limit_per_sec: Option<i32>,
}

pub async fn create(
    State(state): State<AppState>,
    tenant: Tenant,
    Json(body): Json<CreateEndpoint>,
) -> Result<(StatusCode, Json<serde_json::Value>), ApiError> {
    validate_url(&state, &body.url)?;
    validate_event_types(&body.event_types)?;
    let rate = body.rate_limit_per_sec.unwrap_or(50);
    validate_rate(rate)?;

    let secret = generate_signing_secret();
    let row = db::endpoints::create(
        &state.pool,
        db::endpoints::NewEndpoint {
            tenant_id: tenant.0,
            url: &body.url,
            description: &body.description,
            event_types: body.event_types,
            secret_ciphertext: state.secrets.encrypt(&secret)?,
            rate_limit_per_sec: rate,
        },
    )
    .await?;
    let mut v = serde_json::to_value(EndpointView::from(row)).map_err(|_| ApiError::internal())?;
    v["secret"] = serde_json::Value::String(format_signing_secret(&secret));
    Ok((StatusCode::CREATED, Json(v)))
}

pub async fn list(
    State(state): State<AppState>,
    tenant: Tenant,
    Query(params): Query<PageParams>,
) -> Result<Json<Page<EndpointView>>, ApiError> {
    let limit = params.limit()?;
    let before = decode_cursor::<Uuid>(params.cursor.as_deref())?;
    let rows = db::endpoints::list(&state.pool, tenant.0, before, limit + 1).await?;
    Ok(Json(page(
        rows,
        limit,
        |r| r.id.to_string(),
        EndpointView::from,
    )))
}

pub async fn get_one(
    State(state): State<AppState>,
    tenant: Tenant,
    Path(id): Path<Uuid>,
) -> Result<Json<EndpointView>, ApiError> {
    let row = db::endpoints::get(&state.pool, tenant.0, id)
        .await?
        .ok_or_else(|| ApiError::not_found("endpoint"))?;
    Ok(Json(row.into()))
}

/// Distinguishes "field absent" (outer None) from "field: null" (Some(None)).
fn double_option<'de, D, T>(de: D) -> Result<Option<Option<T>>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(de).map(Some)
}

#[derive(Debug, Deserialize)]
pub struct UpdateEndpoint {
    url: Option<String>,
    description: Option<String>,
    #[serde(default, deserialize_with = "double_option")]
    event_types: Option<Option<Vec<String>>>,
    rate_limit_per_sec: Option<i32>,
    /// "active" re-enables a disabled endpoint; "disabled" disables it.
    status: Option<String>,
}

pub async fn update(
    State(state): State<AppState>,
    tenant: Tenant,
    Path(id): Path<Uuid>,
    Json(body): Json<UpdateEndpoint>,
) -> Result<Json<EndpointView>, ApiError> {
    if let Some(url) = &body.url {
        validate_url(&state, url)?;
    }
    if let Some(types) = &body.event_types {
        validate_event_types(types)?;
    }
    if let Some(rate) = body.rate_limit_per_sec {
        validate_rate(rate)?;
    }
    let enable = match body.status.as_deref() {
        None => false,
        Some("active") => true,
        Some("disabled") => {
            // Make sure the endpoint is the caller's before touching it.
            db::endpoints::get(&state.pool, tenant.0, id)
                .await?
                .ok_or_else(|| ApiError::not_found("endpoint"))?;
            let mut tx = state.pool.begin().await?;
            db::endpoints::disable_in_tx(
                &mut tx,
                id,
                "disabled via API",
                state.config.queue_mode,
                false,
            )
            .await?;
            tx.commit().await?;
            false
        }
        Some(_) => {
            return Err(ApiError::unprocessable(
                "status must be 'active' or 'disabled'",
            ));
        }
    };
    let row = db::endpoints::update(
        &state.pool,
        tenant.0,
        id,
        db::endpoints::EndpointPatch {
            url: body.url,
            description: body.description,
            event_types: body.event_types,
            rate_limit_per_sec: body.rate_limit_per_sec,
            enable,
        },
    )
    .await?
    .ok_or_else(|| ApiError::not_found("endpoint"))?;
    Ok(Json(row.into()))
}

pub async fn delete(
    State(state): State<AppState>,
    tenant: Tenant,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    if db::endpoints::delete(&state.pool, tenant.0, id).await? {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ApiError::not_found("endpoint"))
    }
}

pub async fn get_secret(
    State(state): State<AppState>,
    tenant: Tenant,
    Path(id): Path<Uuid>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let row = db::endpoints::get(&state.pool, tenant.0, id)
        .await?
        .ok_or_else(|| ApiError::not_found("endpoint"))?;
    let current = state.secrets.decrypt(&row.secret_ciphertext)?;
    let mut v = serde_json::json!({ "secret": format_signing_secret(&current) });
    if let (Some(prev), Some(expires)) = (
        &row.previous_secret_ciphertext,
        row.previous_secret_expires_at,
    ) && expires > Utc::now()
    {
        v["previous_secret"] =
            serde_json::Value::String(format_signing_secret(&state.secrets.decrypt(prev)?));
        v["previous_secret_expires_at"] = serde_json::Value::String(expires.to_rfc3339());
    }
    Ok(Json(v))
}

/// New secret now; the old one stays valid for 24 hours, during which every
/// request carries both signatures so the customer can switch with no downtime.
pub async fn rotate_secret(
    State(state): State<AppState>,
    tenant: Tenant,
    Path(id): Path<Uuid>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let secret = generate_signing_secret();
    let expires = Utc::now() + ChronoDuration::hours(ROTATION_OVERLAP_HOURS);
    db::endpoints::rotate_secret(
        &state.pool,
        tenant.0,
        id,
        state.secrets.encrypt(&secret)?,
        expires,
    )
    .await?
    .ok_or_else(|| ApiError::not_found("endpoint"))?;
    Ok(Json(serde_json::json!({
        "secret": format_signing_secret(&secret),
        "previous_secret_expires_at": expires.to_rfc3339(),
    })))
}

#[derive(Debug, Deserialize)]
pub struct RecoverBody {
    since: DateTime<Utc>,
}

pub async fn recover(
    State(state): State<AppState>,
    tenant: Tenant,
    Path(id): Path<Uuid>,
    Json(body): Json<RecoverBody>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let endpoint = db::endpoints::get(&state.pool, tenant.0, id)
        .await?
        .ok_or_else(|| ApiError::not_found("endpoint"))?;
    if endpoint.status != "active" {
        return Err(ApiError::conflict(
            "endpoint is disabled; re-enable it with PATCH {\"status\":\"active\"} first",
        ));
    }
    let n = db::deliveries::recover(
        &state.pool,
        tenant.0,
        id,
        body.since,
        state.config.queue_mode,
    )
    .await?;
    Ok(Json(serde_json::json!({ "requeued": n })))
}
