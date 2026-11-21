//! The API role: authenticate, validate, save, answer `202`.
//!
//! The API is **stateless**: everything it knows is in Postgres, so any number
//! of replicas can run behind a load balancer and be scaled on CPU (HPA).
//! The only in-memory state is a short-lived API key cache.

pub mod auth;
pub mod deliveries;
pub mod endpoints;
pub mod error;
pub mod events;
pub mod pagination;

use std::sync::Arc;
use std::time::Instant;

use axum::Router;
use axum::extract::{DefaultBodyLimit, MatchedPath, Request, State};
use axum::http::StatusCode;
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;
use tower_http::trace::TraceLayer;

use crate::config::Config;
use crate::crypto::SecretBox;
use crate::metrics;

/// Max size of the event payload (`data`), and of the whole request body
/// with a little room for the envelope.
pub const MAX_PAYLOAD_BYTES: usize = 256 * 1024;
const MAX_BODY_BYTES: usize = MAX_PAYLOAD_BYTES + 4 * 1024;

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
    pub config: Arc<Config>,
    pub secrets: SecretBox,
    pub key_cache: Arc<auth::KeyCache>,
}

pub fn router(state: AppState) -> Router {
    let v1 = Router::new()
        .route("/events", post(events::ingest).get(events::list))
        .route("/events/{id}", get(events::get_one))
        .route("/endpoints", post(endpoints::create).get(endpoints::list))
        .route(
            "/endpoints/{id}",
            get(endpoints::get_one)
                .patch(endpoints::update)
                .delete(endpoints::delete),
        )
        .route("/endpoints/{id}/secret", get(endpoints::get_secret))
        .route(
            "/endpoints/{id}/rotate-secret",
            post(endpoints::rotate_secret),
        )
        .route("/endpoints/{id}/recover", post(endpoints::recover))
        .route("/deliveries", get(deliveries::list))
        .route("/deliveries/{id}", get(deliveries::get_one))
        .route("/deliveries/{id}/resend", post(deliveries::resend));

    Router::new()
        .nest("/v1", v1)
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .layer(middleware::from_fn(record_latency))
        .layer(TraceLayer::new_for_http().make_span_with(|req: &Request| {
            let route = req
                .extensions()
                .get::<MatchedPath>()
                .map(|p| p.as_str().to_string())
                .unwrap_or_else(|| req.uri().path().to_string());
            tracing::info_span!("http.request", method = %req.method(), route = %route, tenant_id = tracing::field::Empty)
        }))
        .with_state(state)
}

/// Process is up.
async fn healthz() -> &'static str {
    "ok"
}

/// Database is reachable. A load balancer stops sending traffic to a replica
/// whose readiness fails, instead of the replica crashing.
async fn readyz(State(state): State<AppState>) -> Response {
    match sqlx::query("SELECT 1").execute(&state.pool).await {
        Ok(_) => "ready".into_response(),
        Err(_) => (StatusCode::SERVICE_UNAVAILABLE, "database unreachable").into_response(),
    }
}

async fn record_latency(req: Request, next: Next) -> Response {
    let route = req
        .extensions()
        .get::<MatchedPath>()
        .map(|p| p.as_str().to_string())
        .unwrap_or_else(|| "unmatched".to_string());
    let start = Instant::now();
    let response = next.run(req).await;
    metrics::get().api_latency.record(
        start.elapsed().as_secs_f64(),
        &[
            metrics::kv("route", route),
            metrics::kv(
                "status_class",
                metrics::status_class(response.status().as_u16()),
            ),
        ],
    );
    response
}

/// Run the API until `shutdown` is cancelled, then stop accepting connections
/// and let in-flight requests finish.
pub async fn run(
    config: Arc<Config>,
    pool: PgPool,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(&config.listen_addr).await?;
    serve(config, pool, listener, shutdown).await
}

/// Like `run`, on an already-bound listener (tests bind port 0).
pub async fn serve(
    config: Arc<Config>,
    pool: PgPool,
    listener: tokio::net::TcpListener,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let secrets = SecretBox::from_base64_key(&config.secret_encryption_key)?;
    let state = AppState {
        pool,
        config: config.clone(),
        secrets,
        key_cache: Arc::new(auth::KeyCache::default()),
    };
    tracing::info!(addr = %listener.local_addr()?, queue_mode = config.queue_mode.as_str(), "api listening");
    axum::serve(listener, router(state))
        .with_graceful_shutdown(async move { shutdown.cancelled().await })
        .await?;
    Ok(())
}
