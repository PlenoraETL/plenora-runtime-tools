//! HTTP listener and bounded graceful shutdown tests.

use std::{error::Error, future::pending, net::SocketAddr, sync::Arc, time::Duration};

use axum::{Router, http::StatusCode, routing::get};
use plenora_runtime_core::{RuntimeHandle, ServiceMetadata};
use plenora_runtime_http::{HttpBootstrap, HttpServeOutcome, HttpServePhase, HttpServerConfig};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
    sync::Notify,
    time::{sleep, timeout},
};

#[tokio::test]
async fn listener_stops_when_shutdown_was_already_requested() -> Result<(), Box<dyn Error>> {
    let runtime = runtime();
    let bootstrap = bootstrap(&runtime, Duration::from_millis(100))?;
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await?;
    assert!(runtime.request_shutdown());

    let outcome = timeout(
        Duration::from_secs(1),
        bootstrap.serve_listener(listener, Router::new()),
    )
    .await??;

    assert_eq!(outcome, HttpServeOutcome::GracefulShutdown);
    Ok(())
}

#[tokio::test]
async fn serve_does_not_bind_or_admit_after_shutdown() -> Result<(), Box<dyn Error>> {
    let occupied = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await?;
    let occupied_address = occupied.local_addr()?;
    let runtime = runtime();
    let config = HttpServerConfig::new(occupied_address, Duration::from_millis(100))?;
    let bootstrap = HttpBootstrap::new(&runtime, config)?;
    assert!(runtime.request_shutdown());

    let outcome = timeout(Duration::from_secs(1), bootstrap.serve(Router::new())).await??;

    assert_eq!(outcome, HttpServeOutcome::GracefulShutdown);
    drop(occupied);
    Ok(())
}

#[tokio::test]
async fn bind_failure_preserves_phase_and_io_source_without_address_leakage()
-> Result<(), Box<dyn Error>> {
    let occupied = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await?;
    let occupied_address = occupied.local_addr()?;
    let runtime = runtime();
    let config = HttpServerConfig::new(occupied_address, Duration::from_millis(100))?;
    let bootstrap = HttpBootstrap::new(&runtime, config)?;

    let error = bootstrap
        .serve(Router::new())
        .await
        .err()
        .ok_or("binding an occupied address unexpectedly succeeded")?;

    assert_eq!(error.phase(), HttpServePhase::Bind);
    assert_eq!(error.source_error().kind(), std::io::ErrorKind::AddrInUse);
    assert!(error.source().is_some());
    assert_eq!(error.to_string(), "failed to bind HTTP listener");
    let debug = format!("{error:?}");
    assert!(debug.contains("HttpServeError"));
    assert!(!debug.contains(&occupied_address.to_string()));
    drop(occupied);
    Ok(())
}

#[tokio::test]
async fn active_request_is_bounded_by_http_shutdown_grace() -> Result<(), Box<dyn Error>> {
    let grace_period = Duration::from_millis(25);
    let runtime = runtime();
    let bootstrap = bootstrap(&runtime, grace_period)?;
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await?;
    let address = listener.local_addr()?;
    let started = Arc::new(Notify::new());
    let handler_started = Arc::clone(&started);
    let application = Router::new().route(
        "/hang",
        get(move || {
            let started = Arc::clone(&handler_started);
            async move {
                started.notify_one();
                pending::<()>().await;
                StatusCode::OK
            }
        }),
    );
    let client_runtime = runtime.clone();
    let client = async move {
        let mut stream = TcpStream::connect(address).await?;
        stream
            .write_all(b"GET /hang HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await?;
        started.notified().await;
        let _started_shutdown = client_runtime.request_shutdown();
        sleep(Duration::from_millis(100)).await;
        drop(stream);
        Ok::<(), std::io::Error>(())
    };

    let (outcome, client_result) = timeout(Duration::from_secs(2), async {
        tokio::join!(bootstrap.serve_listener(listener, application), client)
    })
    .await?;
    client_result?;

    assert_eq!(
        outcome?,
        HttpServeOutcome::ShutdownTimedOut { grace_period }
    );
    Ok(())
}

fn bootstrap(
    runtime: &RuntimeHandle,
    grace_period: Duration,
) -> Result<HttpBootstrap, Box<dyn Error>> {
    let config = HttpServerConfig::new(SocketAddr::from(([127, 0, 0, 1], 0)), grace_period)?;
    Ok(HttpBootstrap::new(runtime, config)?)
}

fn runtime() -> RuntimeHandle {
    RuntimeHandle::new(ServiceMetadata::new("http-adapter-test", "0.1.0", "test-1"))
}
