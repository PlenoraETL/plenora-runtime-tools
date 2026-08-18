//! Minimal HTTP service example.

#![forbid(unsafe_code)]

use std::{error::Error, net::SocketAddr, time::Duration};

use axum::{Router, routing::get};
use plenora_runtime_core::{RuntimeHandle, ServiceMetadata};
use plenora_runtime_http::{HttpBootstrap, HttpServerConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let runtime = RuntimeHandle::new(ServiceMetadata::new(
        "example-http-service",
        env!("CARGO_PKG_VERSION"),
        "local",
    ));
    let config = HttpServerConfig::new(
        SocketAddr::from(([127, 0, 0, 1], 3_000)),
        Duration::from_secs(10),
    )?;
    let bootstrap = HttpBootstrap::new(&runtime, config)?;
    let application = Router::new().route("/", get(|| async { "plenora runtime is ready" }));
    let mut server = Box::pin(bootstrap.serve(application));

    tokio::select! {
        result = &mut server => {
            println!("HTTP server stopped: {:?}", result?);
        }
        signal = tokio::signal::ctrl_c() => {
            signal?;
            let _shutdown_started = runtime.request_shutdown();
            println!("HTTP server stopped: {:?}", server.await?);
        }
    }

    Ok(())
}
