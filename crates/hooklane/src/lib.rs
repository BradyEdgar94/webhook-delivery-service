//! Hooklane: a self-hosted outbound webhook delivery service.
//!
//! One binary runs one of four roles (api, relay, scheduler, worker). See
//! `docs/CODE_TOUR.md` for a guided walk through these modules.

pub mod admin;
pub mod api;
pub mod config;
pub mod crypto;
pub mod db;
pub mod delivery;
pub mod metrics;
pub mod queue;
pub mod relay;
pub mod scheduler;
pub mod shutdown;
pub mod telemetry;
pub mod worker;
