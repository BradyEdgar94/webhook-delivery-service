//! Retry policy: what to do after each attempt, and when to try again.
//!
//! **Schedule.** 8 attempts over about 28 hours. Attempt 1 is immediate; the
//! delays before attempts 2..8 are `5s, 5m, 30m, 2h, 5h, 10h, 10h`, each with
//! ±20% random jitter. Jitter matters: if a customer's server goes down for a
//! minute, thousands of deliveries fail together; without jitter they would all
//! retry at exactly the same instant and knock the server over again (the
//! "thundering herd").
//!
//! **Why the retry time lives in Postgres, not Kafka.** Kafka has no delayed
//! delivery. Storing `next_attempt_at` on the delivery row gives exact delays
//! (including a customer's `Retry-After`), survives crashes, and lets the API
//! show when the next attempt is due. The scheduler moves due rows back into
//! the queue. The rejected alternative was a chain of Kafka "retry topics".
//!
//! **Classification** turns the raw result of an HTTP attempt into a verdict:
//!
//! | Outcome                               | Verdict                          |
//! |---------------------------------------|----------------------------------|
//! | 2xx                                   | succeeded                        |
//! | timeout / connection / TLS error      | retry                            |
//! | 5xx, 408                              | retry                            |
//! | 429                                   | retry, honouring `Retry-After`   |
//! | 410 Gone                              | failed + disable the endpoint    |
//! | any other 4xx                         | failed (it will not fix itself)  |
//! | 3xx                                   | failed (redirects not followed)  |
//! | blocked by SSRF rules                 | failed                           |

use std::time::{Duration, SystemTime};

use rand::RngExt;

/// Shortest and longest `Retry-After` we will honour.
pub const RETRY_AFTER_MIN: Duration = Duration::from_secs(1);
pub const RETRY_AFTER_MAX: Duration = Duration::from_secs(6 * 3600);

/// Why an attempt failed before (or instead of) getting an HTTP response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    Timeout,
    Connect,
    Tls,
    /// The URL or its resolved address is not allowed (SSRF protection).
    SsrfBlocked,
    /// Anything else that went wrong while sending (bad URL, body error...).
    Other,
}

impl ErrorKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            ErrorKind::Timeout => "timeout",
            ErrorKind::Connect => "connect",
            ErrorKind::Tls => "tls",
            ErrorKind::SsrfBlocked => "ssrf_blocked",
            ErrorKind::Other => "other",
        }
    }
}

/// What actually happened when we tried to send.
#[derive(Debug, Clone)]
pub enum AttemptOutcome {
    Response {
        status: u16,
        /// Parsed `Retry-After` header (already clamped), if present.
        retry_after: Option<Duration>,
        body_snippet: String,
    },
    Error {
        kind: ErrorKind,
        message: String,
    },
}

/// What we decide to do about it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    Succeeded,
    /// Try again later. `retry_after` overrides the schedule when the receiver
    /// told us how long to wait.
    Retry {
        retry_after: Option<Duration>,
    },
    /// Give up on this delivery now. `disable_endpoint` is set for 410 Gone.
    Fail {
        disable_endpoint: bool,
    },
}

/// How an outcome should count towards the endpoint's circuit breaker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BreakerSignal {
    /// The endpoint is reachable and answering (2xx, and also 4xx: a 404 is a
    /// configuration problem, not an outage).
    Healthy,
    /// Timeouts, connection errors and 5xx: the endpoint looks down.
    Failure,
    /// 429 is rate-limit feedback, not an outage; SSRF blocks never reached it.
    Neutral,
}

pub fn classify(outcome: &AttemptOutcome) -> Verdict {
    match outcome {
        AttemptOutcome::Response {
            status,
            retry_after,
            ..
        } => match *status {
            200..=299 => Verdict::Succeeded,
            408 | 500..=599 => Verdict::Retry { retry_after: None },
            429 => Verdict::Retry {
                retry_after: *retry_after,
            },
            410 => Verdict::Fail {
                disable_endpoint: true,
            },
            _ => Verdict::Fail {
                disable_endpoint: false,
            }, // other 4xx, 3xx, 1xx
        },
        AttemptOutcome::Error { kind, .. } => match kind {
            ErrorKind::Timeout | ErrorKind::Connect | ErrorKind::Tls => {
                Verdict::Retry { retry_after: None }
            }
            ErrorKind::SsrfBlocked | ErrorKind::Other => Verdict::Fail {
                disable_endpoint: false,
            },
        },
    }
}

pub fn breaker_signal(outcome: &AttemptOutcome) -> BreakerSignal {
    match outcome {
        AttemptOutcome::Response { status, .. } => match *status {
            429 => BreakerSignal::Neutral,
            500..=599 | 408 => BreakerSignal::Failure,
            _ => BreakerSignal::Healthy,
        },
        AttemptOutcome::Error { kind, .. } => match kind {
            ErrorKind::Timeout | ErrorKind::Connect | ErrorKind::Tls => BreakerSignal::Failure,
            ErrorKind::SsrfBlocked | ErrorKind::Other => BreakerSignal::Neutral,
        },
    }
}

/// The delay before the next attempt, given how many attempts have been made
/// so far (`attempts_made` >= 1). `None` means the schedule is used up and the
/// delivery should become `dead`.
pub fn next_delay(schedule_ms: &[u64], attempts_made: i32) -> Option<Duration> {
    if attempts_made < 1 {
        return Some(Duration::ZERO);
    }
    schedule_ms
        .get((attempts_made - 1) as usize)
        .map(|ms| Duration::from_millis(*ms))
}

/// Multiply `d` by a random factor in [0.8, 1.2].
pub fn jitter(d: Duration) -> Duration {
    let factor: f64 = rand::rng().random_range(0.8..=1.2);
    d.mul_f64(factor)
}

/// Parse a `Retry-After` header: either whole seconds ("120") or an HTTP date
/// ("Wed, 21 Oct 2015 07:28:00 GMT"). The result is clamped to 1 s..6 h so a
/// receiver can neither make us hammer it nor park a delivery for a week.
pub fn parse_retry_after(value: &str, now: SystemTime) -> Option<Duration> {
    let value = value.trim();
    let raw = if let Ok(secs) = value.parse::<u64>() {
        Duration::from_secs(secs)
    } else {
        let at = httpdate::parse_http_date(value).ok()?;
        at.duration_since(now).unwrap_or(Duration::ZERO)
    };
    Some(raw.clamp(RETRY_AFTER_MIN, RETRY_AFTER_MAX))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resp(status: u16) -> AttemptOutcome {
        AttemptOutcome::Response {
            status,
            retry_after: None,
            body_snippet: String::new(),
        }
    }
    fn err(kind: ErrorKind) -> AttemptOutcome {
        AttemptOutcome::Error {
            kind,
            message: String::new(),
        }
    }

    const DEFAULT: [u64; 7] = [
        5_000, 300_000, 1_800_000, 7_200_000, 18_000_000, 36_000_000, 36_000_000,
    ];

    #[test]
    fn schedule_has_eight_attempts_over_about_28_hours() {
        assert_eq!(next_delay(&DEFAULT, 1), Some(Duration::from_secs(5)));
        assert_eq!(next_delay(&DEFAULT, 2), Some(Duration::from_secs(300)));
        assert_eq!(next_delay(&DEFAULT, 7), Some(Duration::from_secs(36_000)));
        assert_eq!(next_delay(&DEFAULT, 8), None, "8th failure -> dead");
        let total: u64 = DEFAULT.iter().sum();
        let hours = total as f64 / 3_600_000.0;
        assert!((27.0..29.0).contains(&hours), "total {hours} h");
    }

    #[test]
    fn jitter_stays_within_twenty_percent() {
        let base = Duration::from_secs(100);
        let mut seen_low = false;
        let mut seen_high = false;
        for _ in 0..10_000 {
            let j = jitter(base);
            assert!(
                j >= Duration::from_secs(80) && j <= Duration::from_secs(120),
                "{j:?}"
            );
            seen_low |= j < Duration::from_secs(95);
            seen_high |= j > Duration::from_secs(105);
        }
        assert!(
            seen_low && seen_high,
            "jitter should actually spread values"
        );
    }

    #[test]
    fn classification_table() {
        assert_eq!(classify(&resp(200)), Verdict::Succeeded);
        assert_eq!(classify(&resp(204)), Verdict::Succeeded);
        assert_eq!(classify(&resp(500)), Verdict::Retry { retry_after: None });
        assert_eq!(classify(&resp(503)), Verdict::Retry { retry_after: None });
        assert_eq!(classify(&resp(408)), Verdict::Retry { retry_after: None });
        assert_eq!(
            classify(&resp(410)),
            Verdict::Fail {
                disable_endpoint: true
            }
        );
        assert_eq!(
            classify(&resp(400)),
            Verdict::Fail {
                disable_endpoint: false
            }
        );
        assert_eq!(
            classify(&resp(404)),
            Verdict::Fail {
                disable_endpoint: false
            }
        );
        assert_eq!(
            classify(&resp(301)),
            Verdict::Fail {
                disable_endpoint: false
            }
        );
        assert_eq!(
            classify(&resp(302)),
            Verdict::Fail {
                disable_endpoint: false
            }
        );
        assert_eq!(
            classify(&err(ErrorKind::Timeout)),
            Verdict::Retry { retry_after: None }
        );
        assert_eq!(
            classify(&err(ErrorKind::Connect)),
            Verdict::Retry { retry_after: None }
        );
        assert_eq!(
            classify(&err(ErrorKind::Tls)),
            Verdict::Retry { retry_after: None }
        );
        assert_eq!(
            classify(&err(ErrorKind::SsrfBlocked)),
            Verdict::Fail {
                disable_endpoint: false
            }
        );
        let r429 = AttemptOutcome::Response {
            status: 429,
            retry_after: Some(Duration::from_secs(7)),
            body_snippet: String::new(),
        };
        assert_eq!(
            classify(&r429),
            Verdict::Retry {
                retry_after: Some(Duration::from_secs(7))
            }
        );
    }

    #[test]
    fn breaker_signals() {
        assert_eq!(breaker_signal(&resp(200)), BreakerSignal::Healthy);
        assert_eq!(breaker_signal(&resp(404)), BreakerSignal::Healthy);
        assert_eq!(breaker_signal(&resp(500)), BreakerSignal::Failure);
        assert_eq!(breaker_signal(&resp(429)), BreakerSignal::Neutral);
        assert_eq!(
            breaker_signal(&err(ErrorKind::Timeout)),
            BreakerSignal::Failure
        );
        assert_eq!(
            breaker_signal(&err(ErrorKind::SsrfBlocked)),
            BreakerSignal::Neutral
        );
    }

    #[test]
    fn retry_after_seconds_and_clamping() {
        let now = SystemTime::now();
        assert_eq!(parse_retry_after("30", now), Some(Duration::from_secs(30)));
        assert_eq!(
            parse_retry_after(" 30 ", now),
            Some(Duration::from_secs(30))
        );
        assert_eq!(parse_retry_after("0", now), Some(RETRY_AFTER_MIN));
        assert_eq!(parse_retry_after("999999", now), Some(RETRY_AFTER_MAX));
        assert_eq!(parse_retry_after("soon", now), None);
    }

    #[test]
    fn retry_after_http_date() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let in_two_minutes = httpdate::fmt_http_date(now + Duration::from_secs(120));
        assert_eq!(
            parse_retry_after(&in_two_minutes, now),
            Some(Duration::from_secs(120))
        );
        let in_the_past = httpdate::fmt_http_date(now - Duration::from_secs(120));
        assert_eq!(parse_retry_after(&in_the_past, now), Some(RETRY_AFTER_MIN));
    }
}
