//! Per-endpoint rate limiting with a token bucket, written by hand.
//!
//! **The token bucket.** Picture a bucket that holds at most `capacity` tokens
//! and is refilled at a steady `rate` tokens per second. Sending one request
//! costs one token. If the bucket is empty you must wait until enough time has
//! passed for a token to drip back in. This allows short bursts (up to
//! `capacity` requests at once after a quiet period) while holding the
//! long-run average to `rate` per second.
//!
//! **Why per endpoint.** Each customer's server has its own capacity. A
//! default of 50 requests/second protects small servers from being flooded
//! when a burst of events arrives, and is configurable per endpoint via
//! `rate_limit_per_sec`.
//!
//! **Why in memory.** In Kafka mode exactly one worker owns an endpoint's
//! partition at a time, so one in-memory bucket per endpoint is the whole
//! truth. See `breaker.rs` for the full reasoning.
//!
//! Time is passed in (`now`) instead of read from the clock so the tests can
//! drive the bucket deterministically.

use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct TokenBucket {
    capacity: f64,
    rate_per_sec: f64,
    tokens: f64,
    last_refill: Instant,
}

impl TokenBucket {
    /// A full bucket. Capacity equals one second's worth of tokens.
    pub fn new(rate_per_sec: u32, now: Instant) -> TokenBucket {
        let rate = f64::from(rate_per_sec.max(1));
        TokenBucket {
            capacity: rate,
            rate_per_sec: rate,
            tokens: rate,
            last_refill: now,
        }
    }

    pub fn rate_per_sec(&self) -> u32 {
        self.rate_per_sec as u32
    }

    /// Add the tokens that have "dripped in" since the last call.
    fn refill(&mut self, now: Instant) {
        let elapsed = now
            .saturating_duration_since(self.last_refill)
            .as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.rate_per_sec).min(self.capacity);
        self.last_refill = now;
    }

    /// Take one token if available. Otherwise return how long until one will be.
    pub fn try_take(&mut self, now: Instant) -> Result<(), Duration> {
        self.refill(now);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            Ok(())
        } else {
            let missing = 1.0 - self.tokens;
            Err(Duration::from_secs_f64(missing / self.rate_per_sec))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn starts_full_and_allows_a_burst_of_capacity() {
        let t0 = Instant::now();
        let mut b = TokenBucket::new(5, t0);
        for _ in 0..5 {
            assert!(b.try_take(t0).is_ok());
        }
        assert!(b.try_take(t0).is_err());
    }

    #[test]
    fn reports_wait_time_until_next_token() {
        let t0 = Instant::now();
        let mut b = TokenBucket::new(10, t0);
        for _ in 0..10 {
            b.try_take(t0).unwrap();
        }
        let wait = b.try_take(t0).unwrap_err();
        assert!((wait.as_secs_f64() - 0.1).abs() < 1e-9, "{wait:?}");
    }

    #[test]
    fn refills_at_the_configured_rate() {
        let t0 = Instant::now();
        let mut b = TokenBucket::new(10, t0);
        for _ in 0..10 {
            b.try_take(t0).unwrap();
        }
        // After 0.5 s, 5 tokens are back.
        let t1 = t0 + Duration::from_millis(500);
        for _ in 0..5 {
            assert!(b.try_take(t1).is_ok());
        }
        assert!(b.try_take(t1).is_err());
    }

    #[test]
    fn never_exceeds_capacity_after_long_idle() {
        let t0 = Instant::now();
        let mut b = TokenBucket::new(3, t0);
        let later = t0 + Duration::from_secs(3600);
        for _ in 0..3 {
            assert!(b.try_take(later).is_ok());
        }
        assert!(b.try_take(later).is_err());
    }

    #[test]
    fn long_run_average_matches_rate() {
        let t0 = Instant::now();
        let mut b = TokenBucket::new(50, t0);
        let mut sent = 0;
        // Simulate 10 seconds in 1 ms steps, sending whenever allowed.
        for ms in 0..10_000u64 {
            let now = t0 + Duration::from_millis(ms);
            while b.try_take(now).is_ok() {
                sent += 1;
            }
        }
        // 50 initial burst + 50/s * 10 s, within rounding.
        assert!((540..=551).contains(&sent), "sent {sent}");
    }
}
