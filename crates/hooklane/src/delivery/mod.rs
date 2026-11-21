//! Everything about sending one webhook: signing it, keeping it away from
//! private networks, the HTTP call itself, deciding whether to retry, and the
//! per-endpoint protections (rate limit and circuit breaker).

pub mod breaker;
pub mod http;
pub mod ratelimit;
pub mod retry;
pub mod signing;
pub mod ssrf;
