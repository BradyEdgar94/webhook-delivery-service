//! Processing one delivery: the heart of the worker. Used by both queue modes.
//!
//! Steps (section 10 of the spec):
//! 1. load the endpoint (cached); disabled -> `failed`;
//! 2. circuit breaker / 429 pause -> *park* (no attempt used);
//! 3. rate limit -> wait for a token;
//! 4. **claim** (conditional `pending -> delivering`); lost the race -> skip;
//! 5. sign and POST with a 15 s timeout;
//! 6. classify, and in one transaction write the attempt + new status;
//! 7. keep `endpoints.failing_since` up to date;
//! 8. the caller marks the Kafka offset done.
//!
//! **The unavoidable at-least-once window.** If the worker dies between 4 and
//! 6, the delivery stays `delivering`. The request may or may not have reached
//! the customer. The reaper puts it back after 2 minutes and it is sent again.
//! That is why receivers dedupe on `webhook-id`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use tracing::Instrument;

use super::WorkerCtx;
use super::endpoints::{GuardDecision, LookupError};
use crate::db::attempts::NewAttempt;
use crate::db::deliveries::{AttemptResult, ClaimedDelivery};
use crate::db::{self, status};
use crate::delivery::http::{self, WebhookRequest};
use crate::delivery::retry::{self, AttemptOutcome, Verdict};
use crate::metrics::{self, kv};
use crate::queue::DeliveryMessage;

/// What happened to a delivery in this pass (for logs, metrics and tests).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Succeeded,
    RetryScheduled,
    Failed,
    Dead,
    /// Put aside without using an attempt (breaker open, 429 pause, overflow).
    Parked,
    /// Nothing to do: already handled elsewhere, or the endpoint was deleted.
    Skipped,
}

impl Outcome {
    pub fn as_str(&self) -> &'static str {
        match self {
            Outcome::Succeeded => "succeeded",
            Outcome::RetryScheduled => "retry_scheduled",
            Outcome::Failed => "failed",
            Outcome::Dead => "dead",
            Outcome::Parked => "parked",
            Outcome::Skipped => "skipped",
        }
    }
}

/// Errors that mean "could not finish, try this message again" (the database
/// is down, a secret could not be decrypted). The caller must NOT mark the
/// Kafka offset done for these, or the delivery could be stranded.
#[derive(Debug, thiserror::Error)]
pub enum ProcessError {
    #[error(transparent)]
    Db(#[from] sqlx::Error),
    #[error(transparent)]
    Lookup(#[from] LookupError),
}

/// A row already claimed by the Postgres-mode batch query, and when.
pub struct PreClaimed {
    pub delivery: ClaimedDelivery,
    pub claimed_at: Instant,
}

/// Process a delivery. `pre_claimed` is `Some` in Postgres queue mode, where
/// the batch query already claimed the row.
pub async fn process(
    ctx: &Arc<WorkerCtx>,
    msg: &DeliveryMessage,
    pre_claimed: Option<PreClaimed>,
) -> Result<Outcome, ProcessError> {
    let span = tracing::info_span!(
        "worker.deliver",
        delivery_id = msg.delivery_id,
        endpoint_id = %msg.endpoint_id,
        tenant_id = %msg.tenant_id,
        event_id = %msg.event_id,
        outcome = tracing::field::Empty,
    );
    crate::telemetry::set_parent_from_traceparent(&span, msg.traceparent.as_deref());
    let result = process_inner(ctx, msg, pre_claimed)
        .instrument(span.clone())
        .await;
    if let Ok(outcome) = &result {
        span.record("outcome", outcome.as_str());
        metrics::get()
            .deliveries
            .add(1, &[kv("outcome", outcome.as_str())]);
    }
    result
}

async fn process_inner(
    ctx: &Arc<WorkerCtx>,
    msg: &DeliveryMessage,
    pre_claimed: Option<PreClaimed>,
) -> Result<Outcome, ProcessError> {
    // Status the row is in right now, for the conditional updates below.
    let from_status = if pre_claimed.is_some() {
        status::DELIVERING
    } else {
        status::PENDING
    };

    // 1. Endpoint.
    let Some(endpoint) = ctx.endpoints.get(msg.endpoint_id).await? else {
        // Endpoint deleted; its deliveries were deleted with it (ON DELETE CASCADE).
        return Ok(Outcome::Skipped);
    };
    if !endpoint.active {
        return Ok(
            if db::deliveries::fail_endpoint_disabled(&ctx.pool, msg.delivery_id, from_status)
                .await?
            {
                Outcome::Failed
            } else {
                Outcome::Skipped
            },
        );
    }
    let rate = endpoint.rate_limit_per_sec;

    // Postgres mode only: a row claimed long ago risks being taken back by the
    // reaper while we still hold it. Release it instead of racing the reaper.
    if let Some(c) = &pre_claimed
        && c.claimed_at.elapsed() > Duration::from_millis(ctx.config.reaper_stuck_after_ms / 2)
    {
        let until = Utc::now() + chrono::Duration::milliseconds(500);
        db::deliveries::park(
            &ctx.pool,
            msg.delivery_id,
            until,
            from_status,
            "claim_expired",
        )
        .await?;
        return Ok(Outcome::Parked);
    }

    // 2. Circuit breaker and 429 pause.
    let probe = match ctx.guards.check(endpoint.id, rate) {
        GuardDecision::Go { probe } => probe,
        GuardDecision::Park { retry_in, reason } => {
            // Park until the next probe time plus jitter, WITHOUT using an attempt.
            let delay = retry::jitter(retry_in.max(Duration::from_millis(100)));
            let until = Utc::now() + chrono::Duration::from_std(delay).unwrap_or_default();
            let parked =
                db::deliveries::park(&ctx.pool, msg.delivery_id, until, from_status, reason)
                    .await?;
            return Ok(if parked {
                Outcome::Parked
            } else {
                Outcome::Skipped
            });
        }
    };

    // 3. Rate limit: wait for a token. This holds only this endpoint's lane.
    while let Err(wait) = ctx.guards.take_token(endpoint.id, rate) {
        tokio::time::sleep(wait).await;
    }

    // 4. Claim.
    let claimed = match pre_claimed {
        Some(c) => c.delivery,
        None => {
            let claim = db::deliveries::claim(&ctx.pool, msg.delivery_id).await;
            if probe && !matches!(claim, Ok(Some(_))) {
                ctx.guards.cancel_probe(endpoint.id, rate);
            }
            match claim? {
                Some(c) => c,
                // Someone else already handled it (duplicate message or reaped
                // row): sending now would be a duplicate HTTP request.
                None => return Ok(Outcome::Skipped),
            }
        }
    };

    // 5. Send.
    let request = WebhookRequest {
        url: &endpoint.url,
        event_id: claimed.event_id,
        event_type: &claimed.event_type,
        event_created_at: claimed.event_created_at,
        payload: &claimed.payload,
        delivery_id: claimed.id,
        attempt: claimed.attempt_count,
        secret: &endpoint.secret,
        previous_secret: endpoint.previous_secret.as_deref(),
    };
    let started_at = Utc::now();
    let t0 = Instant::now();
    let outcome = http::send(&ctx.http, &request, ctx.policy)
        .instrument(tracing::info_span!(
            "http.post",
            attempt = claimed.attempt_count
        ))
        .await;
    let duration = t0.elapsed();

    // 6. Classify and record.
    let verdict = retry::classify(&outcome);
    let pause_for = match (&outcome, &verdict) {
        (
            AttemptOutcome::Response {
                status: 429,
                retry_after: Some(d),
                ..
            },
            _,
        ) => Some(*d),
        _ => None,
    };
    ctx.guards.record(
        endpoint.id,
        rate,
        retry::breaker_signal(&outcome),
        pause_for,
    );

    let result = match verdict {
        Verdict::Succeeded => AttemptResult::Succeeded,
        Verdict::Fail { disable_endpoint } => AttemptResult::Failed { disable_endpoint },
        Verdict::Retry { retry_after } => {
            match retry::next_delay(&ctx.config.retry_schedule_ms, claimed.attempt_count) {
                None => AttemptResult::Dead,
                Some(scheduled) => {
                    let delay = retry_after.unwrap_or_else(|| retry::jitter(scheduled));
                    AttemptResult::RetryAt(
                        Utc::now() + chrono::Duration::from_std(delay).unwrap_or_default(),
                    )
                }
            }
        }
    };

    let (attempt, last_error) = attempt_row(&claimed, &outcome, started_at, duration);
    record_with_retry(ctx, &claimed, &attempt, &result, last_error.as_deref()).await?;
    if let AttemptResult::Failed {
        disable_endpoint: true,
    } = result
    {
        ctx.endpoints.invalidate(endpoint.id);
    }

    // 7. failing_since, written only when it changes.
    let succeeded = matches!(result, AttemptResult::Succeeded);
    if ctx
        .guards
        .failing_changed(endpoint.id, rate, endpoint.failing, !succeeded)
    {
        if succeeded {
            db::endpoints::mark_healthy(&ctx.pool, endpoint.id).await?;
        } else {
            db::endpoints::mark_failing(&ctx.pool, endpoint.id).await?;
        }
    }

    // Metrics.
    let m = metrics::get();
    let class = match &outcome {
        AttemptOutcome::Response { status, .. } => metrics::status_class(*status),
        AttemptOutcome::Error { .. } => "error",
    };
    m.attempt_duration
        .record(duration.as_secs_f64(), &[kv("status_class", class)]);
    m.attempts.add(
        1,
        &[
            kv("first_attempt", claimed.attempt_count == 1),
            kv("result", if succeeded { "success" } else { "failure" }),
        ],
    );
    if succeeded {
        let latency = (Utc::now() - claimed.event_created_at)
            .num_milliseconds()
            .max(0) as f64
            / 1000.0;
        m.delivery_latency
            .record(latency, &[kv("first_attempt", claimed.attempt_count == 1)]);
    }

    Ok(match result {
        AttemptResult::Succeeded => Outcome::Succeeded,
        AttemptResult::RetryAt(_) => Outcome::RetryScheduled,
        AttemptResult::Failed { .. } => Outcome::Failed,
        AttemptResult::Dead => Outcome::Dead,
    })
}

fn attempt_row(
    claimed: &ClaimedDelivery,
    outcome: &AttemptOutcome,
    started_at: chrono::DateTime<Utc>,
    duration: Duration,
) -> (NewAttempt, Option<String>) {
    let duration_ms = duration.as_millis().min(i32::MAX as u128) as i32;
    match outcome {
        AttemptOutcome::Response {
            status,
            body_snippet,
            ..
        } => {
            let last_error = if (200..300).contains(status) {
                None
            } else {
                Some(format!("HTTP {status}"))
            };
            (
                NewAttempt {
                    attempt_number: claimed.attempt_count,
                    started_at,
                    duration_ms,
                    response_status: Some(i32::from(*status)),
                    response_body_snippet: Some(body_snippet.clone()),
                    error_kind: None,
                    error: None,
                },
                last_error,
            )
        }
        AttemptOutcome::Error { kind, message } => (
            NewAttempt {
                attempt_number: claimed.attempt_count,
                started_at,
                duration_ms,
                response_status: None,
                response_body_snippet: None,
                error_kind: Some(kind.as_str().to_string()),
                error: Some(message.clone()),
            },
            Some(format!("{}: {}", kind.as_str(), message)),
        ),
    }
}

/// The request has been sent; losing its result now would mean sending it
/// again. So retry the write a few times through brief database hiccups
/// before giving up (after which the reaper eventually re-queues it).
async fn record_with_retry(
    ctx: &Arc<WorkerCtx>,
    claimed: &ClaimedDelivery,
    attempt: &NewAttempt,
    result: &AttemptResult,
    last_error: Option<&str>,
) -> Result<(), ProcessError> {
    let mut delay = Duration::from_millis(200);
    let mut tries = 0;
    loop {
        match db::deliveries::record_attempt(
            &ctx.pool,
            claimed,
            attempt,
            result,
            last_error,
            ctx.config.queue_mode,
        )
        .await
        {
            Ok(true) => return Ok(()),
            Ok(false) => {
                tracing::warn!(
                    "delivery was re-queued by the reaper before its result was recorded; it will be sent again"
                );
                return Ok(());
            }
            Err(e) if tries < 6 => {
                tries += 1;
                tracing::warn!(error = %e, "failed to record attempt, retrying");
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(5));
            }
            Err(e) => return Err(e.into()),
        }
    }
}
