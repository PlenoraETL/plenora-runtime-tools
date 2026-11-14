//! Same complete plans on real PostgreSQL/NATS and installed CLI/wheel.
#![allow(clippy::too_many_lines)]
#[path = "support/interop_fixture.rs"]
mod fixture;
#[path = "support/interop_oracle.rs"]
mod oracle;
#[path = "support/postgres_fixture.rs"]
mod postgres_fixture;
use plenora_runtime_chain_host::{InteropConfig, InteropHost, InteropProfile, RemoteClient};
use plenora_runtime_chains::{Request, RunOptions, Transport};
use serde_json::{Value, json};
use std::{io::Write, sync::Arc, time::Duration};
type Test = Result<(), Box<dyn std::error::Error>>;
fn options() -> RunOptions {
    RunOptions {
        run_id: uuid::Uuid::new_v4().to_string(),
        idempotency_key: None,
        deadline_ms: None,
    }
}
fn configured(
    profile: &InteropProfile,
    root: &std::path::Path,
) -> Result<InteropConfig, Box<dyn std::error::Error>> {
    std::fs::create_dir(root)?;
    let mut adapter: InteropProfile = serde_json::from_slice(&serde_json::to_vec(profile)?)?;
    // Isolate the primary namespace as well as files: a NATS worker must never claim
    // another surface's run against a different trusted physical destination mapping.
    adapter.tenant = format!("s4-{}", uuid::Uuid::new_v4());
    adapter.io.staging_directory = root.to_str().ok_or("staging")?.into();
    adapter
        .io
        .destinations
        .get_mut("ref:destination")
        .ok_or("destination")?
        .directory = root.join("bundle").to_str().ok_or("path")?.into();
    Ok(InteropConfig {
        database: std::env::var("PLENORA_POSTGRES_URL")?,
        adapter,
    })
}
fn axes(value: &Value) -> Value {
    json!({"state":value["state"],"remote_effect":value["remote_effect"],"reports_complete":value["reports_complete"],"losses":value["losses"],"steps":value["steps"].as_array().map(|steps|steps.iter().map(|s|json!({"state":s["state"],"attempt":s["attempt"],"effect":s["remote_effect"],"rows_read":s["counters"]["rows_read"],"rows_produced":s["counters"]["rows_produced"],"category":s["error"]["category"],"phase":s["error"]["phase"],"retry":s["retry"],"stop_confirmed":s["stop_confirmed"],"reports":s["reports"].as_array().map(|r|r.iter().map(|r|json!([r["kind"],r["availability"]])).collect::<Vec<_>>())})).collect::<Vec<_>>())})
}
async fn evidence(
    host: &InteropHost,
    value: &Value,
) -> Result<Vec<Value>, Box<dyn std::error::Error>> {
    let mut docs = Vec::new();
    for step in value["steps"].as_array().ok_or("steps")? {
        if step["upstream_result"].is_object() {
            let d = &step["upstream_result"];
            let r = host
                .execute(Request::Artifact {
                    reference: d["artifact_ref"]
                        .as_str()
                        .ok_or("upstream reference")?
                        .into(),
                })
                .await;
            assert!(r.error.is_none());
            let bytes: Vec<u8> = serde_json::from_value(r.value)?;
            assert_eq!(fixture::digest(&bytes), d["sha256"]);
        }
        for report in step["reports"].as_array().ok_or("reports")? {
            if report["availability"] == "available" {
                let d = &report["document"];
                let r = host
                    .execute(Request::Artifact {
                        reference: d["artifact_ref"].as_str().ok_or("ref")?.into(),
                    })
                    .await;
                assert!(r.error.is_none());
                let bytes: Vec<u8> = serde_json::from_value(r.value)?;
                assert_eq!(fixture::digest(&bytes), d["sha256"]);
                let doc: Value = serde_json::from_slice(&bytes)?;
                // Loss/fidelity and localized effects must compare structurally, excluding pathful native manifests.
                if report["kind"] == "fidelity"
                    || report["kind"] == "loss_report"
                    || report["kind"] == "diagnostics"
                {
                    if step["operation"] == "data.run" {
                        docs.push(doc);
                    } else if step["operation"] == "database.read" {
                        assert!(
                            doc["read_loss"].is_object(),
                            "database read evidence absent"
                        );
                        docs.push(json!({"read_loss":doc["read_loss"]}));
                    } else {
                        docs.push(json!({"fidelity":doc["fidelity"],"loss":doc["loss"],"input_fidelity":doc["input_fidelity"],"input_loss":doc["input_loss"],"write_fidelity":doc["write_fidelity"],"write_loss":doc["write_loss"],"rows_read":doc["rows_read"],"rows_written":doc["rows_written"],"truncated":doc["truncated"]}));
                    }
                }
            }
        }
    }
    Ok(docs)
}
#[tokio::test]
#[ignore = "requires real PostgreSQL/PostGIS CI service"]
async fn postgres_real_interop_reports_restart() -> Test {
    let root = tempfile::tempdir()?;
    let (profile, plan) = fixture::fixture(root.path(), "ipc", false)?;
    let config = configured(&profile, &root.path().join("rust"))?;
    let raw = serde_json::to_vec(&config)?;
    let h = InteropHost::interop(config)?;
    h.migrate().await?;
    let o = options();
    let accepted = h
        .execute(Request::Submit {
            plan: serde_json::to_string(&plan)?,
            options: o.clone(),
        })
        .await;
    assert!(accepted.error.is_none());
    drop(h);
    let restored = InteropHost::interop(serde_json::from_slice(&raw)?)?;
    let r = restored
        .execute(Request::Run {
            plan: serde_json::to_string(&plan)?,
            options: o.clone(),
        })
        .await;
    assert_eq!(r.value["state"], "succeeded");
    assert_eq!(r.value["reports_complete"], true);
    let original = evidence(&restored, &r.value).await?;
    drop(restored);
    let restored = InteropHost::interop(serde_json::from_slice(&raw)?)?;
    assert_eq!(
        restored
            .execute(Request::Result { run_id: o.run_id })
            .await
            .value,
        r.value
    );
    assert_eq!(evidence(&restored, &r.value).await?, original);
    let accepted = restored
        .execute(Request::Submit {
            plan: serde_json::to_string(&plan)?,
            options: options(),
        })
        .await;
    let run_id = accepted.value["run_id"].as_str().ok_or("run")?.to_owned();
    let cancelled = restored
        .execute(Request::Cancel {
            run_id: run_id.clone(),
            revision: accepted.value["revision"].as_u64().ok_or("revision")?,
        })
        .await;
    assert!(cancelled.error.is_none());
    let result = restored.execute(Request::Result { run_id }).await;
    assert_eq!(result.value["state"], "cancelled");
    assert_eq!(result.value["counters"]["max_attempts"], 0);
    Ok(())
}
#[tokio::test]
#[ignore = "requires real PostgreSQL/NATS and installed CLI/wheel from CI"]
async fn five_surfaces_same_real_plan_and_independent_error_oracles() -> Test {
    let server = std::env::var("PLENORA_NATS_URL")?;
    let _binary = std::env::var("PLENORA_CHAIN_BIN")?;
    let connection = std::env::var("PLENORA_POSTGRES_URL")?;
    let (pg, driver) = tokio_postgres::connect(&connection, tokio_postgres::NoTls).await?;
    let driver = tokio::spawn(driver);
    let pg_source = format!("s4_surfaces_{}", uuid::Uuid::new_v4().simple());
    postgres_fixture::source(&connection, &pg_source).await?;
    let unregistered = format!("s4_unregistered_{}", uuid::Uuid::new_v4().simple());
    pg.batch_execute(&format!(
        "CREATE TABLE {unregistered} (value text); INSERT INTO {unregistered} VALUES ('hello');"
    ))
    .await?;
    for (format, geometry, target, expected, failed) in [
        ("ipc", false, "ipc", "succeeded", None),
        ("ipc", true, "ipc", "succeeded", None),
        ("ipc", true, "geoparquet", "succeeded", None),
        ("ipc", true, "csv", "partial", Some((2, "data_mapping"))),
        ("ipc", true, "xls", "partial", Some((2, "data_mapping"))),
        ("csv", false, "csv", "failed", Some((0, "crs"))),
        ("csv", true, "csv", "failed", Some((0, "data_mapping"))),
        ("xls", true, "xls", "failed", Some((0, "data_mapping"))),
        (
            "geoparquet",
            false,
            "geoparquet",
            "failed",
            Some((0, "data_mapping")),
        ),
        (
            "geoparquet",
            true,
            "geoparquet",
            "partial",
            Some((1, "schema")),
        ),
        ("pg", false, "ipc", "succeeded", None),
        (
            "pg_unregistered",
            false,
            "ipc",
            "failed",
            Some((0, "data_mapping")),
        ),
    ] {
        let root = tempfile::tempdir()?;
        let (mut profile, mut plan) = fixture::fixture(
            root.path(),
            if format.starts_with("pg") {
                "ipc"
            } else {
                format
            },
            geometry,
        )?;
        if target != format && !format.starts_with("pg") {
            fixture::target(&mut profile, &mut plan, target)?;
        }
        if format.starts_with("pg") {
            fixture::postgres(
                &mut profile,
                &mut plan,
                if format == "pg" {
                    &pg_source
                } else {
                    &unregistered
                },
                connection.clone(),
            )?;
        }
        let mut reference = None;
        for surface in ["rust", "runtime", "cli", "sync", "async"] {
            let directory = root.path().join(surface);
            let config = configured(&profile, &directory)?;
            let raw = serde_json::to_vec(&config)?;
            let h = Arc::new(InteropHost::interop(config)?);
            h.migrate().await?;
            let o = options();
            let request = Request::Run {
                plan: serde_json::to_string(&plan)?,
                options: o.clone(),
            };
            let response = if surface == "rust" {
                serde_json::to_value(h.execute(request).await)?
            } else if surface == "runtime" {
                let subject = format!("s4.{}", uuid::Uuid::new_v4().simple());
                let (ready, received) = tokio::sync::oneshot::channel();
                let serving = h.clone();
                let address = server.clone();
                let name = subject.clone();
                let task = tokio::spawn(async move { serving.serve(&address, &name, ready).await });
                tokio::time::timeout(Duration::from_secs(20), received).await??;
                let client = RemoteClient::connect_with_timeout(&server, &subject, 240_000).await?;
                let response = client.exchange(request).await;
                task.abort();
                let _stopped = task.await;
                serde_json::to_value(response?)?
            } else {
                let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../plenora-runtime-chain-python/tests/interop_surface_probe.py");
                let input = serde_json::to_vec(
                    &json!({"config":serde_json::from_slice::<Value>(&raw)?,"plan":plan,"options":o}),
                )?;
                let surface = surface.to_owned();
                let output = tokio::task::spawn_blocking(move || -> std::io::Result<_> {
                    let mut child = std::process::Command::new("python")
                        .arg(script)
                        .arg(surface)
                        .stdin(std::process::Stdio::piped())
                        .stdout(std::process::Stdio::piped())
                        .stderr(std::process::Stdio::piped())
                        .spawn()?;
                    child
                        .stdin
                        .take()
                        .ok_or_else(|| std::io::Error::other("stdin"))?
                        .write_all(&input)?;
                    child.wait_with_output()
                })
                .await??;
                assert!(
                    output.status.success(),
                    "installed qualification failed: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
                serde_json::from_slice(&output.stdout)?
            };
            let result = &response["value"];
            assert_eq!(result["state"], expected, "{format}/{surface}");
            if let Some((index, category)) = failed {
                assert_eq!(result["steps"][index]["error"]["category"], category);
                if index == 2 {
                    let e = &result["steps"][index]["error"];
                    assert_eq!(e["phase"], "validate");
                    assert_eq!(e["remote_effect"], "none");
                    assert_eq!(e["retry"], json!({"kind":"never"}));
                }
                for s in &result["steps"].as_array().ok_or("steps")?[index + 1..] {
                    assert_eq!(s["state"], "blocked");
                    assert_eq!(s["attempt"], 0);
                }
                assert!(!directory.join("bundle").exists());
            } else {
                assert_eq!(result["counters"]["max_attempts"], 3);
                if format == "pg" {
                    let mut reader = arrow_ipc::reader::StreamReader::try_new(
                        std::fs::File::open(directory.join("bundle/result.arrow"))?,
                        None,
                    )?;
                    let batch = reader.next().ok_or("batch")??;
                    assert!(reader.next().is_none());
                    let a = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<arrow_array::StringArray>()
                        .ok_or("text")?;
                    assert_eq!(
                        a.iter().collect::<Vec<_>>(),
                        vec![Some(""), Some("hello"), None]
                    );
                } else {
                    oracle::output(&directory, target, geometry)?;
                }
            }
            let observed = (axes(result), evidence(&h, result).await?);
            if let Some(reference) = &reference {
                assert_eq!(&observed, reference, "{format}/{surface}");
            } else {
                reference = Some(observed);
            }
        }
    }
    pg.batch_execute(&format!(
        "DROP TABLE {pg_source}; DROP TABLE {unregistered}"
    ))
    .await?;
    drop(pg);
    driver.await??;
    Ok(())
}

#[tokio::test]
#[ignore = "requires real PostgreSQL/PostGIS service; reads a real table with database.read@1"]
async fn postgres_real_pg_data_io_read1_reports_and_restart() -> Test {
    let connection = std::env::var("PLENORA_POSTGRES_URL")?;
    let (client, driver) = tokio_postgres::connect(&connection, tokio_postgres::NoTls).await?;
    let driver = tokio::spawn(driver);
    let postgis: bool = client
        .query_one(
            "SELECT EXISTS (SELECT FROM pg_catalog.pg_extension WHERE extname='postgis')",
            &[],
        )
        .await?
        .get(0);
    assert!(
        postgis,
        "CI database requires PostGIS for the pinned database provider"
    );
    let source = format!("s4_pg_{}", uuid::Uuid::new_v4().simple());
    postgres_fixture::source(&connection, &source).await?;
    let root = tempfile::tempdir()?;
    let (mut profile, mut plan) = fixture::fixture(root.path(), "ipc", false)?;
    fixture::postgres(&mut profile, &mut plan, &source, connection)?;
    let config = configured(&profile, &root.path().join("pg"))?;
    let raw = serde_json::to_vec(&config)?;
    let host = InteropHost::interop(config)?;
    host.migrate().await?;
    let o = options();
    let result = host
        .execute(Request::Run {
            plan: serde_json::to_string(&plan)?,
            options: o.clone(),
        })
        .await;
    assert_eq!(
        result.value["state"], "succeeded",
        "PG -> DATA -> IO did not complete"
    );
    assert_eq!(result.value["steps"][0]["operation_version"], 1);
    assert_eq!(result.value["reports_complete"], true);
    let mut reader = arrow_ipc::reader::StreamReader::try_new(
        std::fs::File::open(root.path().join("pg/bundle/result.arrow"))?,
        None,
    )?;
    let batch = reader.next().ok_or("batch")??;
    assert!(reader.next().is_none());
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .ok_or("text")?;
    assert_eq!(
        values.iter().collect::<Vec<_>>(),
        vec![Some(""), Some("hello"), None]
    );
    let reports = evidence(&host, &result.value).await?;
    drop(host);
    let restored = InteropHost::interop(serde_json::from_slice(&raw)?)?;
    assert_eq!(
        restored
            .execute(Request::Result { run_id: o.run_id })
            .await
            .value,
        result.value
    );
    assert_eq!(evidence(&restored, &result.value).await?, reports);
    client
        .batch_execute(&format!("DROP TABLE {source}"))
        .await?;
    drop(client);
    driver.await??;
    Ok(())
}
