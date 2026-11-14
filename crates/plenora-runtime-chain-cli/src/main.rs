//! Machine facade: bounded synchronous file IO stays outside the asynchronous host runtime.
#![forbid(unsafe_code)]
use plenora_runtime_chain_cli::{Endpoint, parse};
use plenora_runtime_chain_host::{
    DatabaseConfig, IoConfig, RemoteClient, ScriptedConfig, ScriptedHost,
};
use plenora_runtime_chains::{Error, EventCursor, Request, Response};
use std::io::{Read, Write};

fn main() -> std::process::ExitCode {
    let response = match execute() {
        Ok(None) => return std::process::ExitCode::SUCCESS,
        Ok(Some(response)) => response,
        Err(error) => Response {
            value: serde_json::Value::Null,
            error: Some(error),
        },
    };
    let code = response.exit_code();
    if emit(&response).is_err() {
        return std::process::ExitCode::from(6);
    }
    std::process::ExitCode::from(code)
}
fn emit(value: &impl serde::Serialize) -> Result<(), Error> {
    let mut out = std::io::stdout().lock();
    serde_json::to_writer(&mut out, value).map_err(|_| Error::Storage)?;
    out.write_all(b"\n").map_err(|_| Error::Storage)
}
fn read_file(path: &str) -> Result<String, Error> {
    let mut content = String::new();
    std::fs::File::open(path)
        .map_err(|_| Error::Invalid)?
        .take(4 * 1024 * 1024 + 1)
        .read_to_string(&mut content)
        .map_err(|_| Error::Invalid)?;
    if content.len() > 4 * 1024 * 1024 {
        return Err(Error::ResourceLimit);
    }
    Ok(content)
}
enum Session {
    Scripted(Box<ScriptedHost>),
    Remote(RemoteClient),
}
impl Session {
    async fn call(&self, bytes: Vec<u8>) -> Result<Response, Error> {
        match self {
            Self::Scripted(host) => Ok(host.call(&bytes).await),
            Self::Remote(client) => client.call(bytes).await,
        }
    }
}
fn execute() -> Result<Option<Response>, Error> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let (endpoint, request, json_lines) = match args.as_slice() {
        [mode, file] if mode == "scripted-call" => (Endpoint::Scripted(file.clone()), None, false),
        [mode, file] if mode == "io-call" => (Endpoint::Io(file.clone()), None, false),
        [mode, file] if mode == "database-call" => (Endpoint::Database(file.clone()), None, false),
        [mode, server, subject] if mode == "remote-call" => (
            Endpoint::Remote {
                server: server.clone(),
                subject: subject.clone(),
                observation_timeout_ms: 10_000,
            },
            None,
            false,
        ),
        _ => {
            let command = parse(&args, read_file)?;
            (command.endpoint, Some(command.request), command.json_lines)
        }
    };
    // Read/parse configuration before entering async code. No blocking file operations in futures.
    let scripted = match &endpoint {
        Endpoint::Scripted(file) => {
            let config: ScriptedConfig =
                serde_json::from_str(&read_file(file)?).map_err(|_| Error::Invalid)?;
            Some(Box::new(ScriptedHost::new(config)?))
        }
        Endpoint::Remote { .. } => None,
        Endpoint::Io(file) => {
            let config: IoConfig =
                serde_json::from_str(&read_file(file)?).map_err(|_| Error::Invalid)?;
            Some(Box::new(ScriptedHost::io(config)?))
        }
        Endpoint::Database(file) => {
            let config: DatabaseConfig =
                serde_json::from_str(&read_file(file)?).map_err(|_| Error::Invalid)?;
            Some(Box::new(ScriptedHost::database(config)?))
        }
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .map_err(|_| Error::Storage)?;
    let session = match (endpoint, scripted) {
        (_, Some(host)) => Session::Scripted(host),
        (
            Endpoint::Remote {
                server,
                subject,
                observation_timeout_ms,
            },
            None,
        ) => Session::Remote(runtime.block_on(RemoteClient::connect_with_timeout(
            &server,
            &subject,
            observation_timeout_ms,
        ))?),
        _ => return Err(Error::Invalid),
    };
    if json_lines {
        let Some(Request::Events {
            run_id,
            cursor,
            limit,
        }) = request
        else {
            return Err(Error::Invalid);
        };
        let mut cursor = EventCursor::new(&run_id, cursor, limit)?;
        loop {
            let bytes = serde_json::to_vec(&cursor.request()).map_err(|_| Error::Invalid)?;
            let reply = runtime.block_on(session.call(bytes))?;
            if reply.error.is_some() {
                return Ok(Some(reply));
            }
            let events = cursor.accept(&reply.value)?;
            if events.is_empty() {
                return Ok(None);
            }
            for event in events {
                emit(&event)?;
            }
        }
    }
    let bytes = if let Some(request) = request {
        serde_json::to_vec(&request).map_err(|_| Error::Invalid)?
    } else {
        let mut bytes = Vec::new();
        std::io::stdin()
            .take(1024 * 1024 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| Error::Invalid)?;
        bytes
    };
    runtime.block_on(session.call(bytes)).map(Some)
}
