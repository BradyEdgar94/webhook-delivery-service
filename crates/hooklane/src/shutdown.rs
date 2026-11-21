//! Graceful shutdown.
//!
//! Kubernetes stops a pod by sending SIGTERM, waiting for the termination
//! grace period, then sending SIGKILL. Every role listens for SIGTERM (and
//! Ctrl-C locally), then: stops taking new work, finishes what is in flight,
//! commits Kafka offsets, and exits. Autoscaling depends on this: when KEDA
//! scales workers down, the removed pods must not drop work on the floor.
//! (Even if they did, nothing would be lost: the reaper and Kafka redelivery
//! cover crashes. Graceful shutdown just avoids needless duplicates.)

use tokio_util::sync::CancellationToken;

/// A token that is cancelled when the process receives SIGTERM or Ctrl-C.
pub fn on_signal() -> CancellationToken {
    let token = CancellationToken::new();
    let t = token.clone();
    tokio::spawn(async move {
        wait_for_signal().await;
        tracing::info!("shutdown signal received, finishing in-flight work");
        t.cancel();
    });
    token
}

#[cfg(unix)]
async fn wait_for_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    match signal(SignalKind::terminate()) {
        Ok(mut term) => {
            tokio::select! {
                _ = term.recv() => {}
                _ = tokio::signal::ctrl_c() => {}
            }
        }
        Err(_) => {
            let _ = tokio::signal::ctrl_c().await;
        }
    }
}

#[cfg(not(unix))]
async fn wait_for_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
