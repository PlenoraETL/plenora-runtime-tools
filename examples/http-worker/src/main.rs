//! Combined HTTP service and worker example.

#![forbid(unsafe_code)]

use std::{error::Error, net::SocketAddr, time::Duration};

use axum::{Router, routing::get};
use plenora_runtime_core::{RuntimeHandle, ServiceMetadata};
use plenora_runtime_http::{HttpBootstrap, HttpServerConfig};
use plenora_runtime_worker::{WorkerConfig, WorkerExecutor};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let runtime = RuntimeHandle::new(ServiceMetadata::new(
        "example-http-worker",
        env!("CARGO_PKG_VERSION"),
        "local",
    ));
    let worker = WorkerExecutor::new((), (), WorkerConfig::default())?;
    let config = HttpServerConfig::new(
        SocketAddr::from(([127, 0, 0, 1], 3_001)),
        Duration::from_secs(10),
    )?;
    let bootstrap = HttpBootstrap::new(&runtime, config)?;
    let application = Router::new().route("/", get(|| async { "HTTP and worker are running" }));
    let mut server = Box::pin(bootstrap.serve(application));

    tokio::select! {
        result = &mut server => {
            println!("HTTP server stopped: {:?}", result?);
        }
        signal = tokio::signal::ctrl_c() => {
            signal?;
            let _worker_drain_started = worker.begin_drain();
            let _shutdown_started = runtime.request_shutdown();
            let (http_outcome, worker_outcome) = tokio::join!(server, worker.drain());
            println!("HTTP server stopped: {:?}", http_outcome?);
            println!("worker stopped: {worker_outcome:?}");
        }
    }

    Ok(())
}
