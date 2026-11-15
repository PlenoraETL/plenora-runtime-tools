//! Explicit private forensic artifacts, separate from redacted diagnostics.
use serde_json::{Value, json};
use std::{
    io::Write,
    path::{Path, PathBuf},
};

pub fn write(path: &Path, value: &Value) -> std::io::Result<()> {
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)?;
    serde_json::to_writer_pretty(&mut file, value)?;
    file.write_all(b"\n")?;
    file.sync_all()
}

pub fn begin(case: &str, metadata: &Value) -> Result<Option<PathBuf>, Box<dyn std::error::Error>> {
    let Some(root) = std::env::var_os("PLENORA_INTEROP_CAPTURE") else {
        return Ok(None);
    };
    let directory = PathBuf::from(root).join(case);
    std::fs::create_dir(&directory)?;
    write(&directory.join("started.json"), metadata)?;
    Ok(Some(directory))
}

/// Snapshot the journal directly in a read-only transaction: no maintenance,
/// Result/Events control messages, recovery, lease renewal or provider invocation.
pub async fn finish(
    directory: &Path,
    connection: &str,
    tenant: &str,
    run: &str,
    elapsed_ms: u128,
) -> Result<(), Box<dyn std::error::Error>> {
    write(
        &directory.join("timing.json"),
        &json!({"elapsed_ms":elapsed_ms}),
    )?;
    let (mut client, driver) = tokio_postgres::connect(connection, tokio_postgres::NoTls).await?;
    let driver = tokio::spawn(driver);
    let tx = client
        .build_transaction()
        .read_only(true)
        .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
        .start()
        .await?;
    let row = tx.query_opt(
        "SELECT to_jsonb(r)::text, journal FROM plenora_chain_runs r WHERE tenant=$1 AND run_id=$2",
        &[&tenant, &run],
    ).await?;
    let mut snapshot = json!({"run_row":null,"journal":null,"events":[],"resources":[]});
    if let Some(row) = row {
        snapshot["run_row"] = serde_json::from_str(row.try_get::<_, &str>(0)?)?;
        write(&directory.join("run-row.json"), &snapshot["run_row"])?;
        if let Some(bytes) = row.try_get::<_, Option<Vec<u8>>>(1)? {
            let journal: Value = serde_json::from_slice(&bytes)?;
            snapshot["events"] = decode_list(&journal["events"])?;
            snapshot["view"] = decode(&journal["view"])?;
            snapshot["original_evidence"] = decode(&journal["evidence"])?;
            snapshot["history"] = decode_list(&journal["history"])?;
            snapshot["journal"] = journal;
        }
    }
    write(
        &directory.join("primary-run.partial.json"),
        &json!({"complete":false,"snapshot":snapshot}),
    )?;
    let rows = tx.query("SELECT to_jsonb(r)::text, ledger FROM plenora_chain_resources r WHERE tenant=$1 AND run_id=$2", &[&tenant, &run]).await?;
    for row in rows {
        snapshot["resources"]
            .as_array_mut()
            .ok_or("resources")?
            .push(json!({
                "row":serde_json::from_str::<Value>(row.try_get::<_, &str>(0)?)?,
                "ledger":serde_json::from_slice::<Value>(&row.try_get::<_, Vec<u8>>(1)?)?
            }));
    }
    write(
        &directory.join("primary-resources.partial.json"),
        &json!({"complete":false,"snapshot":snapshot}),
    )?;
    let prefix = format!("{}:{tenant}:", tenant.len());
    let rows = tx.query("SELECT to_jsonb(d)::text, journal FROM plenora_chain_delivery d WHERE left(key,length($1))=$1 ORDER BY key", &[&prefix]).await?;
    let mut delivery = Vec::new();
    for row in rows {
        delivery.push(json!({
            "row":serde_json::from_str::<Value>(row.try_get::<_, &str>(0)?)?,
            "journal":serde_json::from_slice::<Value>(&row.try_get::<_, Vec<u8>>(1)?)?
        }));
    }
    snapshot["delivery"] = json!(delivery);
    snapshot["captured_at"] = json!(
        tx.query_one("SELECT clock_timestamp()::text", &[])
            .await?
            .try_get::<_, String>(0)?
    );
    write(
        &directory.join("primary-before-commit.partial.json"),
        &json!({"complete":false,"snapshot":snapshot}),
    )?;
    tx.commit().await?;
    drop(client);
    driver.await??;
    snapshot["complete"] = json!(true);
    write(&directory.join("primary.json"), &snapshot)?;
    Ok(())
}
fn decode(value: &Value) -> Result<Value, Box<dyn std::error::Error>> {
    if value.is_null() {
        return Ok(Value::Null);
    }
    let bytes: Vec<u8> = serde_json::from_value(value.clone())?;
    Ok(serde_json::from_slice(&bytes)?)
}
fn decode_list(value: &Value) -> Result<Value, Box<dyn std::error::Error>> {
    Ok(Value::Array(
        value
            .as_array()
            .ok_or("document list")?
            .iter()
            .map(decode)
            .collect::<Result<_, _>>()?,
    ))
}

#[test]
fn capture_is_lossless_and_refuses_overwriting() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("response.json");
    let result = json!({"value":{"state":"requires_recovery","payload":"PRIVATE_SENTINEL"},"error":"recovery"});
    write(&path, &result)?;
    assert_eq!(
        serde_json::from_slice::<Value>(&std::fs::read(&path)?)?,
        result
    );
    assert!(write(&path, &json!({})).is_err());
    assert_eq!(decode(&json!(serde_json::to_vec(&result)?))?, result);
    assert!(decode(&json!([255])).is_err());
    Ok(())
}
