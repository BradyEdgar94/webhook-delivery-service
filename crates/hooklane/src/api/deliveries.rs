//! The delivery log: list, inspect (with every attempt), and resend.

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use serde::Deserialize;
use uuid::Uuid;

use super::AppState;
use super::auth::Tenant;
use super::error::ApiError;
use super::pagination::{Page, decode_cursor, page};
use crate::db::{self, deliveries::DeliveryRow, deliveries::ResendError, status};

#[derive(Debug, Deserialize)]
pub struct ListParams {
    endpoint_id: Option<Uuid>,
    event_id: Option<Uuid>,
    status: Option<String>,
    cursor: Option<String>,
    limit: Option<i64>,
}

pub async fn list(
    State(state): State<AppState>,
    tenant: Tenant,
    Query(params): Query<ListParams>,
) -> Result<Json<Page<DeliveryRow>>, ApiError> {
    let page_params = super::pagination::PageParams {
        cursor: params.cursor.clone(),
        limit: params.limit,
    };
    let limit = page_params.limit()?;
    let before = decode_cursor::<i64>(params.cursor.as_deref())?;
    if let Some(s) = &params.status
        && !status::ALL.contains(&s.as_str())
    {
        return Err(ApiError::bad_request(format!(
            "status must be one of {:?}",
            status::ALL
        )));
    }
    let filter = db::deliveries::DeliveryFilter {
        endpoint_id: params.endpoint_id,
        event_id: params.event_id,
        status: params.status,
    };
    let rows = db::deliveries::list(&state.pool, tenant.0, &filter, before, limit + 1).await?;
    Ok(Json(page(rows, limit, |r| r.id.to_string(), |r| r)))
}

pub async fn get_one(
    State(state): State<AppState>,
    tenant: Tenant,
    Path(id): Path<i64>,
) -> Result<Json<serde_json::Value>, ApiError> {
    // Tenant check first: attempts are only fetched for a delivery we know is theirs.
    let delivery = db::deliveries::get(&state.pool, tenant.0, id)
        .await?
        .ok_or_else(|| ApiError::not_found("delivery"))?;
    let attempts = db::attempts::for_delivery(&state.pool, id).await?;
    let mut v = serde_json::to_value(&delivery).map_err(|_| ApiError::internal())?;
    v["attempts"] = serde_json::to_value(&attempts).map_err(|_| ApiError::internal())?;
    Ok(Json(v))
}

pub async fn resend(
    State(state): State<AppState>,
    tenant: Tenant,
    Path(id): Path<i64>,
) -> Result<(StatusCode, Json<serde_json::Value>), ApiError> {
    match db::deliveries::resend(&state.pool, tenant.0, id, state.config.queue_mode).await {
        Ok(()) => Ok((
            StatusCode::ACCEPTED,
            Json(serde_json::json!({ "id": id, "status": "pending" })),
        )),
        Err(ResendError::NotFound) => Err(ApiError::not_found("delivery")),
        Err(ResendError::InProgress) => Err(ApiError::conflict(
            "delivery is already queued or being delivered",
        )),
        Err(ResendError::EndpointDisabled) => Err(ApiError::conflict(
            "endpoint is disabled; re-enable it first",
        )),
        Err(ResendError::Db(e)) => Err(e.into()),
    }
}
