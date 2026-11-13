//! CLI parsing only; admission, event cursors and execution belong to the shared native facade.
#![forbid(unsafe_code)]
use plenora_runtime_chains::{Error, RunOptions};
pub use plenora_runtime_chains::{Request, Response};
use std::collections::BTreeMap;

/// Explicit local fake profile or remote endpoint, never inferred from environment variables.
pub enum Endpoint {
    /// Read the declared scripted configuration from this file.
    Scripted(String),
    /// Connect to the exact host endpoint and subject.
    Remote {
        /// Broker connection address, excluded from errors.
        server: String,
        /// Exact host-authorized request subject.
        subject: String,
    },
}
/// Parsed presentation and transport controls; no domain execution occurs during parsing.
pub struct Command {
    /// Host selection.
    pub endpoint: Endpoint,
    /// Closed call shared with Rust and the SDK.
    pub request: Request,
    /// Emit event records individually and consume all currently available pages.
    pub json_lines: bool,
}
/// Parse the public command syntax with an injected bounded UTF-8 file reader.
/// # Errors
/// Rejects duplicate/unknown options, mixed endpoints, noncanonical numbers and unused arguments.
#[allow(
    clippy::too_many_lines,
    reason = "One closed option map ensures every argument is consumed exactly once"
)]
pub fn parse(
    args: &[String],
    read: impl Fn(&str) -> Result<String, Error>,
) -> Result<Command, Error> {
    let (function, rest) = match args {
        [first, rest @ ..] if first == "capabilities" => ("capabilities", rest),
        [first, second, rest @ ..] if first == "chain" => (second.as_str(), rest),
        [first, second, rest @ ..]
            if matches!(first.as_str(), "artifact" | "report") && second == "get" =>
        {
            (first.as_str(), rest)
        }
        _ => return Err(Error::Invalid),
    };
    let mut options = BTreeMap::new();
    let mut at = 0;
    while at < rest.len() {
        let name = rest.get(at).ok_or(Error::Invalid)?;
        if !name.starts_with("--") {
            return Err(Error::Invalid);
        }
        at += 1;
        let value = if matches!(name.as_str(), "--json" | "--json-lines") {
            String::new()
        } else {
            let value = rest.get(at).ok_or(Error::Invalid)?.clone();
            at += 1;
            value
        };
        if options.insert(name.as_str(), value).is_some() {
            return Err(Error::Invalid);
        }
    }
    let endpoint = if let Some(file) = options.remove("--config") {
        Endpoint::Scripted(file)
    } else {
        Endpoint::Remote {
            server: required(&mut options, "--server")?,
            subject: required(&mut options, "--subject")?,
        }
    };
    let json_lines = options.remove("--json-lines").is_some();
    let json = options.remove("--json").is_some();
    if json == json_lines || (json_lines && function != "events") {
        return Err(Error::Invalid);
    }
    let request = match function {
        "capabilities" => Request::Capabilities,
        "validate" => Request::Validate {
            plan: read(&required(&mut options, "--plan")?)?,
        },
        "submit" | "run" => {
            let plan = read(&required(&mut options, "--plan")?)?;
            let options = RunOptions {
                run_id: required(&mut options, "--run")?,
                idempotency_key: options.remove("--idempotency-key"),
                deadline_ms: options
                    .remove("--deadline-ms")
                    .map(|s| number(&s))
                    .transpose()?,
            };
            if function == "run" {
                Request::Run { plan, options }
            } else {
                Request::Submit { plan, options }
            }
        }
        "status" => Request::Status {
            run_id: required(&mut options, "--run")?,
        },
        "result" => Request::Result {
            run_id: required(&mut options, "--run")?,
        },
        "report" => Request::Report {
            cursor: options
                .remove("--report-index")
                .map(|index| {
                    let offset = required(&mut options, "--offset")?;
                    Ok::<_, Error>(plenora_runtime_chains::ReportCursor {
                        index: usize::try_from(number(&index)?).map_err(|_| Error::Invalid)?,
                        offset: usize::try_from(number(&offset)?).map_err(|_| Error::Invalid)?,
                    })
                })
                .transpose()?,
            run_id: required(&mut options, "--run")?,
        },
        "artifact" => Request::Artifact {
            reference: required(&mut options, "--ref")?,
        },
        "events" => Request::Events {
            run_id: required(&mut options, "--run")?,
            cursor: number(&required(&mut options, "--cursor")?)?,
            limit: usize::try_from(
                options
                    .remove("--limit")
                    .map(|s| number(&s))
                    .transpose()?
                    .unwrap_or(256),
            )
            .map_err(|_| Error::Invalid)?,
        },
        "cancel" | "resume" => {
            let run_id = required(&mut options, "--run")?;
            let revision = number(&required(&mut options, "--revision")?)?;
            if function == "cancel" {
                Request::Cancel { run_id, revision }
            } else {
                Request::Resume { run_id, revision }
            }
        }
        _ => return Err(Error::Invalid),
    };
    if !options.is_empty() {
        return Err(Error::Invalid);
    }
    Ok(Command {
        endpoint,
        request,
        json_lines,
    })
}
fn required(options: &mut BTreeMap<&str, String>, name: &str) -> Result<String, Error> {
    options
        .remove(name)
        .filter(|s| !s.is_empty())
        .ok_or(Error::Invalid)
}
fn number(s: &str) -> Result<u64, Error> {
    if s.is_empty() || !s.bytes().all(|c| c.is_ascii_digit()) || (s.len() > 1 && s.starts_with('0'))
    {
        return Err(Error::Invalid);
    }
    s.parse().map_err(|_| Error::Invalid)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_control_commands_map_to_the_shared_request() -> Result<(), Error> {
        let cases = [
            ("capabilities", "capabilities"),
            ("chain validate --plan p", "validate"),
            ("chain submit --plan p --run id", "submit"),
            (
                "chain run --plan p --run id --idempotency-key key --deadline-ms 123",
                "run",
            ),
            ("chain status --run id", "status"),
            ("chain result --run id", "result"),
            ("chain events --run id --cursor 0 --limit 2", "events"),
            ("chain cancel --run id --revision 1", "cancel"),
            ("chain resume --run id --revision 1", "resume"),
            ("artifact get --ref ref:a", "artifact"),
            ("report get --run id", "report"),
        ];
        assert_eq!(cases.len(), plenora_runtime_chains::FUNCTIONS.len());
        for (command, function) in cases {
            for endpoint in ["--config fake", "--server local --subject requests"] {
                let args: Vec<_> = format!("{command} {endpoint} --json")
                    .split_whitespace()
                    .map(str::to_owned)
                    .collect();
                let parsed = parse(&args, |_| Ok("{}".into()))?;
                let request = serde_json::to_value(parsed.request).map_err(|_| Error::Invalid)?;
                assert_eq!(request["function"], function);
                assert!(!parsed.json_lines);
            }
        }
        for extra in [
            "--json --json",
            "--json --json-lines",
            "--json --unused x",
            "--json --server local --subject requests",
            "--json --config duplicate",
        ] {
            let args: Vec<_> = format!("capabilities --config fake {extra}")
                .split_whitespace()
                .map(str::to_owned)
                .collect();
            assert!(parse(&args, |_| Ok("{}".into())).is_err());
        }
        for cursor in ["01", "+1", "-1", "18446744073709551616"] {
            let args: Vec<_> =
                format!("chain events --run id --cursor {cursor} --config fake --json-lines")
                    .split_whitespace()
                    .map(str::to_owned)
                    .collect();
            assert!(parse(&args, |_| Ok("{}".into())).is_err());
        }
        Ok(())
    }
    #[test]
    fn report_cursor_is_only_parsed_here_and_shared_by_both_endpoints() -> Result<(), Error> {
        for endpoint in ["--config fake", "--server local --subject requests"] {
            let args =
                format!("chain report --run id --report-index 2 --offset 65536 {endpoint} --json")
                    .split_whitespace()
                    .map(str::to_owned)
                    .collect::<Vec<_>>();
            let parsed = parse(&args, |_| Err(Error::Invalid))?;
            assert!(matches!(
                parsed.request,
                Request::Report {
                    cursor: Some(plenora_runtime_chains::ReportCursor {
                        index: 2,
                        offset: 65536
                    }),
                    ..
                }
            ));
        }
        for flags in [
            "--report-index 0",
            "--offset 0",
            "--report-index 01 --offset 0",
            "--report-index 0 --offset -1",
        ] {
            let args = format!("chain report --run id {flags} --config fake --json")
                .split_whitespace()
                .map(str::to_owned)
                .collect::<Vec<_>>();
            assert!(parse(&args, |_| Err(Error::Invalid)).is_err());
        }
        Ok(())
    }
}
