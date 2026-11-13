//! Machine facade; stdin is the closed Request JSON and stdout is only the shared Response JSON.
#![forbid(unsafe_code)]
use plenora_runtime_chain_host::{RemoteClient, ScriptedConfig, ScriptedHost};
use plenora_runtime_chains::{Error, Response};
use std::io::{Read, Write};

fn main() -> std::process::ExitCode {
    let response = execute().unwrap_or_else(|error| Response {
        value: serde_json::Value::Null,
        error: Some(error),
    });
    let code = match response.error {
        None => 0,
        Some(Error::Invalid | Error::Unsupported | Error::Conflict | Error::ResourceLimit) => 2,
        Some(Error::Recovery) => 4,
        Some(Error::Failed) => 3,
        Some(Error::Cancelled) => 5,
        _ => 6,
    };
    let result = serde_json::to_writer(std::io::stdout().lock(), &response);
    if result.is_err() || std::io::stdout().write_all(b"\n").is_err() {
        return std::process::ExitCode::from(6);
    }
    std::process::ExitCode::from(code)
}
fn execute() -> Result<Response, Error> {
    let args: Vec<_> = std::env::args().collect();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .map_err(|_| Error::Storage)?;
    if args.len() == 4 && args[1] == "remote-call" {
        let mut input = Vec::new();
        std::io::stdin()
            .take(1024 * 1024 + 1)
            .read_to_end(&mut input)
            .map_err(|_| Error::Invalid)?;
        return runtime.block_on(async {
            RemoteClient::connect(&args[2], &args[3])
                .await?
                .call(input)
                .await
        });
    }
    if args.len() != 3 || args[1] != "scripted-call" {
        return Err(Error::Invalid);
    }
    let mut config = Vec::new();
    std::fs::File::open(&args[2])
        .map_err(|_| Error::Invalid)?
        .take(4 * 1024 * 1024 + 1)
        .read_to_end(&mut config)
        .map_err(|_| Error::Invalid)?;
    if config.len() > 4 * 1024 * 1024 {
        return Err(Error::ResourceLimit);
    }
    let config: ScriptedConfig = serde_json::from_slice(&config).map_err(|_| Error::Invalid)?;
    let host = ScriptedHost::new(config)?;
    let mut input = Vec::new();
    std::io::stdin()
        .take(1024 * 1024 + 1)
        .read_to_end(&mut input)
        .map_err(|_| Error::Invalid)?;

    Ok(runtime.block_on(host.call(&input)))
}
