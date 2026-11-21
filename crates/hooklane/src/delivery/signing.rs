//! Webhook signing, following the Standard Webhooks specification
//! (<https://www.standardwebhooks.com/>) exactly.
//!
//! **Why sign at all.** A receiver's webhook URL is public. Without a
//! signature, anyone who learns it could POST fake events. With HMAC signing,
//! only someone holding the endpoint's secret can produce a valid signature.
//!
//! **What is signed.** The string `{webhook-id}.{webhook-timestamp}.{body}`.
//! Including the timestamp lets receivers reject old messages (a *replay
//! attack* re-sends a captured, validly signed request later); receivers
//! typically allow 5 minutes of clock skew. Including the id lets them dedupe.
//!
//! **Format.** HMAC-SHA256 keyed with the *base64-decoded* bytes of the
//! `whsec_...` secret, sent as `v1,<base64 signature>`. During a secret
//! rotation we send two signatures separated by a space; a receiver accepts
//! the request if any one of them verifies, so customers can switch secrets
//! with no downtime.
//!
//! The body that is signed must be the exact bytes that are sent, so the
//! caller builds the body once and passes the same buffer to both.

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// One `v1,<base64>` signature.
pub fn sign(secret: &[u8], msg_id: &str, timestamp: i64, body: &[u8]) -> String {
    // HMAC accepts keys of any length, so `new_from_slice` cannot fail here;
    // we still avoid unwrap() and fall back to an empty signature, which every
    // receiver would reject (fail closed).
    let Ok(mut mac) = HmacSha256::new_from_slice(secret) else {
        return String::from("v1,");
    };
    mac.update(msg_id.as_bytes());
    mac.update(b".");
    mac.update(timestamp.to_string().as_bytes());
    mac.update(b".");
    mac.update(body);
    let sig = mac.finalize().into_bytes();
    format!("v1,{}", STANDARD.encode(sig))
}

/// The full `webhook-signature` header value: the current secret's
/// signature, plus the previous secret's while a rotation window is open.
pub fn signature_header(
    current: &[u8],
    previous: Option<&[u8]>,
    msg_id: &str,
    timestamp: i64,
    body: &[u8],
) -> String {
    let mut header = sign(current, msg_id, timestamp, body);
    if let Some(prev) = previous {
        header.push(' ');
        header.push_str(&sign(prev, msg_id, timestamp, body));
    }
    header
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::{format_signing_secret, generate_signing_secret};

    /// Build the headers a receiver would see, for the official library.
    fn headers(id: &str, ts: i64, sig: &str) -> http::HeaderMap {
        let mut h = http::HeaderMap::new();
        h.insert("webhook-id", id.parse().unwrap());
        h.insert("webhook-timestamp", ts.to_string().parse().unwrap());
        h.insert("webhook-signature", sig.parse().unwrap());
        h
    }

    fn now() -> i64 {
        chrono::Utc::now().timestamp()
    }

    #[test]
    fn verified_by_official_standardwebhooks_library() {
        let raw = generate_signing_secret();
        let secret = format_signing_secret(&raw);
        let body =
            br#"{"type":"form.submitted","timestamp":"2026-01-01T00:00:00Z","data":{"a":1}}"#;
        let ts = now();
        let sig = signature_header(&raw, None, "msg_123", ts, body);

        let wh = standardwebhooks::Webhook::new(&secret).unwrap();
        wh.verify(body, &headers("msg_123", ts, &sig))
            .expect("official library accepts our signature");
    }

    #[test]
    fn tampered_body_fails() {
        let raw = generate_signing_secret();
        let wh = standardwebhooks::Webhook::new(&format_signing_secret(&raw)).unwrap();
        let ts = now();
        let sig = sign(&raw, "msg_1", ts, b"{\"amount\":1}");
        assert!(
            wh.verify(b"{\"amount\":9}", &headers("msg_1", ts, &sig))
                .is_err()
        );
    }

    #[test]
    fn wrong_secret_fails() {
        let raw = generate_signing_secret();
        let other = generate_signing_secret();
        let wh = standardwebhooks::Webhook::new(&format_signing_secret(&other)).unwrap();
        let ts = now();
        let sig = sign(&raw, "msg_1", ts, b"{}");
        assert!(wh.verify(b"{}", &headers("msg_1", ts, &sig)).is_err());
    }

    #[test]
    fn changed_id_or_timestamp_fails() {
        let raw = generate_signing_secret();
        let wh = standardwebhooks::Webhook::new(&format_signing_secret(&raw)).unwrap();
        let ts = now();
        let sig = sign(&raw, "msg_1", ts, b"{}");
        assert!(wh.verify(b"{}", &headers("msg_2", ts, &sig)).is_err());
        assert!(wh.verify(b"{}", &headers("msg_1", ts - 1, &sig)).is_err());
    }

    #[test]
    fn old_timestamp_rejected_by_receiver() {
        // Replay protection is the receiver's job, but we check our timestamp
        // is what it checks: 10 minutes old is outside the 5-minute tolerance.
        let raw = generate_signing_secret();
        let wh = standardwebhooks::Webhook::new(&format_signing_secret(&raw)).unwrap();
        let ts = now() - 600;
        let sig = sign(&raw, "msg_1", ts, b"{}");
        assert!(wh.verify(b"{}", &headers("msg_1", ts, &sig)).is_err());
    }

    #[test]
    fn rotation_sends_two_signatures_and_either_secret_verifies() {
        let old = generate_signing_secret();
        let new = generate_signing_secret();
        let ts = now();
        let body = b"{\"x\":true}";
        let sig = signature_header(&new, Some(&old), "msg_9", ts, body);
        assert_eq!(sig.split(' ').count(), 2);
        assert!(sig.split(' ').all(|s| s.starts_with("v1,")));

        for secret in [&old, &new] {
            let wh = standardwebhooks::Webhook::new(&format_signing_secret(secret)).unwrap();
            wh.verify(body, &headers("msg_9", ts, &sig))
                .expect("both old and new secret verify");
        }
        let stranger =
            standardwebhooks::Webhook::new(&format_signing_secret(&generate_signing_secret()))
                .unwrap();
        assert!(stranger.verify(body, &headers("msg_9", ts, &sig)).is_err());
    }

    #[test]
    fn known_answer() {
        // Fixed vector so a refactor cannot silently change the algorithm.
        let secret = STANDARD.decode("MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw").unwrap();
        let sig = sign(
            &secret,
            "msg_p5jXN8AQM9LWM0D4loKWxJek",
            1614265330,
            br#"{"test": 2432232314}"#,
        );
        assert_eq!(sig, "v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE=");
    }
}
