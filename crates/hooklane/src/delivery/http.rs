//! Building and sending the outbound webhook request.
//!
//! The HTTP client is configured for safety first:
//! * **no redirects** – a public URL could redirect to an internal one;
//! * **no proxy** – a proxy would resolve DNS itself and bypass our SSRF resolver;
//! * **custom DNS resolver** – drops private addresses at connect time (`ssrf.rs`);
//! * **total timeout** (15 s default) – one slow endpoint cannot hold a worker.
//!
//! Every request carries the Standard Webhooks headers (`webhook-id`,
//! `webhook-timestamp`, `webhook-signature`) plus `webhook-sequence` and
//! `webhook-attempt`, and a body of the form
//! `{"type": ..., "timestamp": <event created, RFC 3339>, "data": <payload>}`.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use chrono::{DateTime, SecondsFormat, Utc};
use uuid::Uuid;

use super::retry::{AttemptOutcome, ErrorKind, parse_retry_after};
use super::signing::signature_header;
use super::ssrf::{self, Policy, SafeResolver};

/// Store only this much of a response body in the attempt log.
pub const BODY_SNIPPET_BYTES: usize = 1024;

pub const USER_AGENT: &str = concat!("Hooklane-Webhooks/", env!("CARGO_PKG_VERSION"));

pub fn build_client(
    timeout: Duration,
    allow_private: bool,
) -> Result<reqwest::Client, reqwest::Error> {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .dns_resolver(Arc::new(SafeResolver { allow_private }))
        .timeout(timeout)
        .connect_timeout(timeout.min(Duration::from_secs(10)))
        .user_agent(USER_AGENT)
        .pool_idle_timeout(Duration::from_secs(60))
        .pool_max_idle_per_host(32)
        .build()
}

/// Everything needed to send one attempt.
pub struct WebhookRequest<'a> {
    pub url: &'a str,
    pub event_id: Uuid,
    pub event_type: &'a str,
    pub event_created_at: DateTime<Utc>,
    pub payload: &'a serde_json::Value,
    pub delivery_id: i64,
    pub attempt: i32,
    pub secret: &'a [u8],
    pub previous_secret: Option<&'a [u8]>,
}

/// The exact JSON bytes we send (and sign).
pub fn build_body(
    event_type: &str,
    created_at: DateTime<Utc>,
    payload: &serde_json::Value,
) -> Vec<u8> {
    let body = serde_json::json!({
        "type": event_type,
        "timestamp": created_at.to_rfc3339_opts(SecondsFormat::Millis, true),
        "data": payload,
    });
    // Serialising a serde_json::Value cannot fail.
    serde_json::to_vec(&body).unwrap_or_default()
}

/// Send one attempt and describe what happened. Never returns an error: every
/// failure mode is an `AttemptOutcome::Error` so the caller can classify it.
pub async fn send(
    client: &reqwest::Client,
    req: &WebhookRequest<'_>,
    policy: Policy,
) -> AttemptOutcome {
    // Literal-IP hosts skip DNS, so the resolver cannot see them: check here.
    if let Err(e) = ssrf::check_url(req.url, policy) {
        let kind = match e {
            ssrf::UrlError::BlockedAddress(_) | ssrf::UrlError::BlockedHost(_) => {
                ErrorKind::SsrfBlocked
            }
            _ => ErrorKind::Other,
        };
        return AttemptOutcome::Error {
            kind,
            message: e.to_string(),
        };
    }

    let body = build_body(req.event_type, req.event_created_at, req.payload);
    let msg_id = req.event_id.to_string();
    let timestamp = Utc::now().timestamp();
    let signature = signature_header(req.secret, req.previous_secret, &msg_id, timestamp, &body);

    let result = client
        .post(req.url)
        .header("content-type", "application/json")
        .header("webhook-id", &msg_id)
        .header("webhook-timestamp", timestamp.to_string())
        .header("webhook-signature", signature)
        .header("webhook-sequence", req.delivery_id.to_string())
        .header("webhook-attempt", req.attempt.to_string())
        .body(body)
        .send()
        .await;

    let response = match result {
        Ok(r) => r,
        Err(e) => return classify_error(&e),
    };

    let status = response.status().as_u16();
    let retry_after = response
        .headers()
        .get("retry-after")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| parse_retry_after(v, SystemTime::now()));

    // Read at most BODY_SNIPPET_BYTES; we never need (or store) more.
    let body_snippet = read_snippet(response).await;
    AttemptOutcome::Response {
        status,
        retry_after,
        body_snippet,
    }
}

async fn read_snippet(mut response: reqwest::Response) -> String {
    let mut buf: Vec<u8> = Vec::new();
    while buf.len() < BODY_SNIPPET_BYTES {
        match response.chunk().await {
            Ok(Some(chunk)) => buf.extend_from_slice(&chunk),
            _ => break,
        }
    }
    buf.truncate(BODY_SNIPPET_BYTES);
    String::from_utf8_lossy(&buf).into_owned()
}

fn classify_error(e: &reqwest::Error) -> AttemptOutcome {
    let message = error_chain(e);
    let kind = if ssrf::is_ssrf_error(e) {
        ErrorKind::SsrfBlocked
    } else if e.is_timeout() {
        ErrorKind::Timeout
    } else if message.to_ascii_lowercase().contains("certificate")
        || message.contains("tls")
        || message.contains("TLS")
    {
        ErrorKind::Tls
    } else if e.is_connect() || e.is_request() {
        ErrorKind::Connect
    } else {
        ErrorKind::Other
    };
    AttemptOutcome::Error { kind, message }
}

/// "error: cause: cause" in one line, truncated, for the attempt log.
fn error_chain(e: &(dyn std::error::Error + 'static)) -> String {
    let mut parts = vec![e.to_string()];
    let mut current = e.source();
    while let Some(c) = current {
        parts.push(c.to_string());
        current = c.source();
    }
    let mut s = parts.join(": ");
    s.truncate(500);
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn body_shape() {
        let created = DateTime::parse_from_rfc3339("2026-03-01T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let body = build_body("form.submitted", created, &serde_json::json!({"a": 1}));
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["type"], "form.submitted");
        assert_eq!(v["timestamp"], "2026-03-01T12:00:00.000Z");
        assert_eq!(v["data"]["a"], 1);
    }
}
