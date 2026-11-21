//! Per-endpoint circuit breaker.
//!
//! **The pattern.** Like an electrical breaker: when an endpoint keeps failing
//! we "trip" and stop calling it for a while, instead of spending a 15-second
//! timeout on every delivery to a server we already know is down. Three states:
//!
//! * **Closed** – normal; requests flow.
//! * **Open** – entered after 5 consecutive failures, or when more than 50% of
//!   at least 10 requests in the last minute failed. No requests are sent. The
//!   worker *parks* deliveries (`retry_scheduled` at the next probe time)
//!   **without consuming an attempt**, because the endpoint never got a chance.
//! * **Half-open** – after a cooldown (30 s, doubling after each failed probe,
//!   capped at 10 min) exactly one delivery is let through as a probe. Success
//!   closes the breaker; failure re-opens it with a longer cooldown.
//!
//! Only timeouts, connection errors and 5xx count as failures. A 429 is the
//! receiver asking us to slow down, which is not the same as being down.
//!
//! **Why the state lives in the worker's memory and not in Redis.** In Kafka
//! mode, messages are keyed by endpoint id, so all of an endpoint's deliveries
//! land in one partition, and a partition is owned by exactly one worker at a
//! time. That worker is therefore the only process talking to the endpoint, so
//! its local view is the whole truth and no coordination is needed. If the
//! partition moves to another worker (scale up/down, crash) the new owner
//! starts with a fresh, closed breaker. The worst case is a few extra failed
//! attempts against an endpoint that is still down; nothing is lost, because
//! every delivery's state is in Postgres. (In Postgres queue mode several
//! workers may serve one endpoint, so the breaker is only approximate there.)
//!
//! Time is passed in (`now`) so every transition can be unit tested.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use super::retry::BreakerSignal;

const WINDOW: Duration = Duration::from_secs(60);
const WINDOW_MIN_REQUESTS: usize = 10;
const WINDOW_FAILURE_RATIO: f64 = 0.5;

#[derive(Debug, Clone, Copy)]
pub struct BreakerSettings {
    pub consecutive_failures: u32,
    pub base_cooldown: Duration,
    pub max_cooldown: Duration,
}

impl Default for BreakerSettings {
    fn default() -> Self {
        BreakerSettings {
            consecutive_failures: 5,
            base_cooldown: Duration::from_secs(30),
            max_cooldown: Duration::from_secs(600),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    Closed,
    /// Not calling the endpoint until `until`.
    Open {
        until: Instant,
    },
    /// One probe is allowed; `probe_in_flight` is true once it has been handed out.
    HalfOpen {
        probe_in_flight: bool,
    },
}

/// What the caller should do with the delivery in hand.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    /// Send it normally.
    Allow,
    /// Send it as the single half-open probe. The caller must later call
    /// `record` (or `cancel_probe` if it ended up not sending).
    AllowProbe,
    /// Do not send. Park the delivery until (roughly) `retry_in` from now.
    Reject { retry_in: Duration },
}

#[derive(Debug, Clone)]
pub struct CircuitBreaker {
    settings: BreakerSettings,
    state: State,
    consecutive_failures: u32,
    /// (time, was_failure) for requests in the last minute.
    window: VecDeque<(Instant, bool)>,
    /// Cooldown to use the next time the breaker opens.
    cooldown: Duration,
}

impl CircuitBreaker {
    pub fn new(settings: BreakerSettings) -> CircuitBreaker {
        CircuitBreaker {
            settings,
            state: State::Closed,
            consecutive_failures: 0,
            window: VecDeque::new(),
            cooldown: settings.base_cooldown,
        }
    }

    pub fn state(&self) -> State {
        self.state
    }

    pub fn is_open(&self) -> bool {
        !matches!(self.state, State::Closed)
    }

    pub fn check(&mut self, now: Instant) -> Decision {
        match self.state {
            State::Closed => Decision::Allow,
            State::Open { until } if now >= until => {
                // Cooldown over: let exactly one probe through.
                self.state = State::HalfOpen {
                    probe_in_flight: true,
                };
                Decision::AllowProbe
            }
            State::Open { until } => Decision::Reject {
                retry_in: until - now,
            },
            State::HalfOpen {
                probe_in_flight: false,
            } => {
                self.state = State::HalfOpen {
                    probe_in_flight: true,
                };
                Decision::AllowProbe
            }
            // A probe is already out; everyone else waits about one cooldown.
            State::HalfOpen {
                probe_in_flight: true,
            } => Decision::Reject {
                retry_in: self.cooldown,
            },
        }
    }

    /// The probe was handed out but never sent (e.g. another worker had already
    /// claimed the delivery). Let the next delivery be the probe instead.
    pub fn cancel_probe(&mut self) {
        if let State::HalfOpen { .. } = self.state {
            self.state = State::HalfOpen {
                probe_in_flight: false,
            };
        }
    }

    pub fn record(&mut self, now: Instant, signal: BreakerSignal) {
        // Half-open: the probe's result decides everything.
        if let State::HalfOpen { .. } = self.state {
            match signal {
                BreakerSignal::Healthy => self.close(),
                BreakerSignal::Failure => {
                    // Failed probe: back off harder before the next one.
                    self.cooldown = (self.cooldown * 2).min(self.settings.max_cooldown);
                    self.open(now);
                }
                // Inconclusive (e.g. 429): stay cautious, same cooldown.
                BreakerSignal::Neutral => self.open(now),
            }
            return;
        }

        if signal == BreakerSignal::Neutral {
            return;
        }
        let failed = signal == BreakerSignal::Failure;
        self.window.push_back((now, failed));
        while let Some(&(t, _)) = self.window.front() {
            if now.saturating_duration_since(t) > WINDOW {
                self.window.pop_front();
            } else {
                break;
            }
        }

        if failed {
            self.consecutive_failures += 1;
        } else {
            self.consecutive_failures = 0;
        }

        if let State::Closed = self.state
            && (self.consecutive_failures >= self.settings.consecutive_failures
                || self.window_tripped())
        {
            self.open(now);
        }
        // Results arriving while Open (requests sent just before it opened)
        // only update the counters above.
    }

    fn window_tripped(&self) -> bool {
        let total = self.window.len();
        if total < WINDOW_MIN_REQUESTS {
            return false;
        }
        let failures = self.window.iter().filter(|(_, f)| *f).count();
        failures as f64 / total as f64 > WINDOW_FAILURE_RATIO
    }

    fn open(&mut self, now: Instant) {
        self.state = State::Open {
            until: now + self.cooldown,
        };
    }

    fn close(&mut self) {
        self.state = State::Closed;
        self.consecutive_failures = 0;
        self.window.clear();
        self.cooldown = self.settings.base_cooldown;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use BreakerSignal::*;

    fn breaker() -> CircuitBreaker {
        CircuitBreaker::new(BreakerSettings::default())
    }

    #[test]
    fn closed_allows() {
        let mut b = breaker();
        assert_eq!(b.check(Instant::now()), Decision::Allow);
    }

    #[test]
    fn opens_after_five_consecutive_failures() {
        let mut b = breaker();
        let t = Instant::now();
        for _ in 0..4 {
            b.record(t, Failure);
        }
        assert_eq!(b.state(), State::Closed);
        b.record(t, Failure);
        assert!(matches!(b.state(), State::Open { .. }));
        assert_eq!(
            b.check(t),
            Decision::Reject {
                retry_in: Duration::from_secs(30)
            }
        );
    }

    #[test]
    fn a_success_resets_the_consecutive_count() {
        let mut b = breaker();
        let t = Instant::now();
        for _ in 0..4 {
            b.record(t, Failure);
        }
        b.record(t, Healthy);
        for _ in 0..4 {
            b.record(t, Failure);
        }
        assert_eq!(b.state(), State::Closed);
    }

    #[test]
    fn opens_on_failure_ratio_over_the_window() {
        let mut b = breaker();
        let t = Instant::now();
        // Alternate so we never hit 5 in a row: F F F F S, F F F F S, F
        // => 9 failures of 11 requests.
        for i in 0..11 {
            b.record(t, if i % 5 == 4 { Healthy } else { Failure });
        }
        assert!(matches!(b.state(), State::Open { .. }));
    }

    #[test]
    fn ratio_needs_at_least_ten_requests() {
        let mut b = breaker();
        let t = Instant::now();
        // 6 failures of 9 (>50%) but fewer than 10 requests, never 5 in a row.
        for s in [
            Failure, Failure, Healthy, Failure, Failure, Healthy, Failure, Failure, Healthy,
        ] {
            b.record(t, s);
        }
        assert_eq!(b.state(), State::Closed);
    }

    #[test]
    fn old_results_fall_out_of_the_window() {
        let mut b = breaker();
        let t0 = Instant::now();
        // 9 requests, 5 failures, never 5 in a row: below the 10-request minimum.
        for s in [
            Failure, Healthy, Failure, Healthy, Failure, Healthy, Failure, Healthy, Failure,
        ] {
            b.record(t0, s);
        }
        // Two minutes later those 9 have aged out, so 2 more failures make
        // 2 requests in the window, not 11, and the breaker stays closed.
        let t1 = t0 + Duration::from_secs(120);
        b.record(t1, Failure);
        b.record(t1, Failure);
        assert_eq!(b.state(), State::Closed);
        // Whereas without the time gap, the same 11 requests (7 failures) trip it.
        let mut b2 = breaker();
        for s in [
            Failure, Healthy, Failure, Healthy, Failure, Healthy, Failure, Healthy, Failure,
        ] {
            b2.record(t0, s);
        }
        b2.record(t0, Failure);
        b2.record(t0, Failure);
        assert!(matches!(b2.state(), State::Open { .. }));
    }

    #[test]
    fn neutral_429_does_not_count() {
        let mut b = breaker();
        let t = Instant::now();
        for _ in 0..50 {
            b.record(t, Neutral);
        }
        assert_eq!(b.state(), State::Closed);
    }

    #[test]
    fn half_open_lets_exactly_one_probe_through() {
        let mut b = breaker();
        let t = Instant::now();
        for _ in 0..5 {
            b.record(t, Failure);
        }
        let later = t + Duration::from_secs(31);
        assert_eq!(b.check(later), Decision::AllowProbe);
        assert!(matches!(b.check(later), Decision::Reject { .. }));
        assert!(matches!(b.check(later), Decision::Reject { .. }));
    }

    #[test]
    fn successful_probe_closes() {
        let mut b = breaker();
        let t = Instant::now();
        for _ in 0..5 {
            b.record(t, Failure);
        }
        let later = t + Duration::from_secs(31);
        assert_eq!(b.check(later), Decision::AllowProbe);
        b.record(later, Healthy);
        assert_eq!(b.state(), State::Closed);
        assert_eq!(b.check(later), Decision::Allow);
    }

    #[test]
    fn failed_probe_reopens_with_doubled_cooldown_capped() {
        let mut b = breaker();
        let mut t = Instant::now();
        for _ in 0..5 {
            b.record(t, Failure);
        }
        let mut expected = Duration::from_secs(30);
        for _ in 0..10 {
            t += expected + Duration::from_millis(1);
            assert_eq!(b.check(t), Decision::AllowProbe);
            b.record(t, Failure);
            expected = (expected * 2).min(Duration::from_secs(600));
            assert_eq!(
                b.state(),
                State::Open {
                    until: t + expected
                }
            );
        }
        assert_eq!(expected, Duration::from_secs(600));
    }

    #[test]
    fn cooldown_resets_after_closing() {
        let mut b = breaker();
        let mut t = Instant::now();
        for _ in 0..5 {
            b.record(t, Failure);
        }
        t += Duration::from_secs(31);
        b.check(t);
        b.record(t, Failure); // cooldown now 60 s
        t += Duration::from_secs(61);
        b.check(t);
        b.record(t, Healthy); // closed, cooldown back to 30 s
        for _ in 0..5 {
            b.record(t, Failure);
        }
        assert_eq!(
            b.state(),
            State::Open {
                until: t + Duration::from_secs(30)
            }
        );
    }

    #[test]
    fn cancelled_probe_lets_the_next_one_through() {
        let mut b = breaker();
        let t = Instant::now();
        for _ in 0..5 {
            b.record(t, Failure);
        }
        let later = t + Duration::from_secs(31);
        assert_eq!(b.check(later), Decision::AllowProbe);
        b.cancel_probe();
        assert_eq!(b.check(later), Decision::AllowProbe);
    }

    #[test]
    fn neutral_probe_reopens_without_doubling() {
        let mut b = breaker();
        let t = Instant::now();
        for _ in 0..5 {
            b.record(t, Failure);
        }
        let later = t + Duration::from_secs(31);
        b.check(later);
        b.record(later, Neutral);
        assert_eq!(
            b.state(),
            State::Open {
                until: later + Duration::from_secs(30)
            }
        );
    }
}
