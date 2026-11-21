//! `sink` binary: serves the fake endpoints on `SINK_ADDR` (default 0.0.0.0:8080).

#[tokio::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt().with_target(false).init();
    let addr = std::env::var("SINK_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".to_string());
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!(%addr, "sink listening");
    let sink = sink::Sink::new();
    // Stop cleanly on SIGTERM/Ctrl-C so `docker compose stop` is quick.
    axum::serve(listener, sink::router(sink))
        .with_graceful_shutdown(async {
            let mut term =
                match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                    Ok(t) => t,
                    Err(_) => return,
                };
            tokio::select! {
                _ = term.recv() => {}
                _ = tokio::signal::ctrl_c() => {}
            }
        })
        .await
}
