//! Real PostgreSQL ETL over the production journal and the common host dispatcher.
#![allow(clippy::too_many_lines)]
#[path = "../../plenora-runtime-adapter-database/tests/support/fixture.rs"]
mod fixture;
use plenora_runtime_adapter_database::{Connection, SecretString};
use plenora_runtime_chain_host::DatabaseHost;
use plenora_runtime_chain_store::{PostgresStore, Retention};
use plenora_runtime_chains::{Request, RunOptions};
use plenora_runtime_core::SystemClock;
use std::{collections::BTreeMap, sync::Arc};

#[tokio::test]
#[ignore = "requires the existing ephemeral PostgreSQL CI service"]
async fn postgres_real_database_read_write_and_restart_reports()
-> Result<(), Box<dyn std::error::Error>> {
    let connection = std::env::var("PLENORA_POSTGRES_URL")?;
    let (client, driver) = tokio_postgres::connect(&connection, tokio_postgres::NoTls).await?;
    let driver = tokio::spawn(driver);
    // Names generated locally, never interpolated from provider data or configuration.
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let source = format!("s3_source_{suffix}");
    let target = format!("s3_target_{suffix}");
    client.batch_execute(&format!("CREATE TABLE {source} (value text); CREATE TABLE {target} (value text); INSERT INTO {source} VALUES (NULL), (''), ('hello');")).await?;
    let store = Arc::new(PostgresStore::new(&connection)?);
    store.migrate(Retention::default()).await?;
    let (config, plan) = fixture::fixture_names(true, true, &source, &target)?;
    let raw_config = serde_json::to_vec(&config)?;
    let make_connections = || {
        BTreeMap::from([(
            "DB".into(),
            Connection {
                destination: "ref:destination".into(),
                secret: SecretString::new(connection.clone()),
                // Explicitly limited to the ephemeral trusted-loopback CI server, not production TLS.
                provider: Arc::new(plenora_db_postgres::PostgresProvider::insecure_local()),
            },
        )])
    };
    let host = DatabaseHost::with_database(
        config,
        make_connections(),
        store.clone(),
        Arc::new(SystemClock),
    )?;
    let run_id = uuid::Uuid::new_v4().to_string();
    let request = Request::Run {
        plan: serde_json::to_string(&plan)?,
        options: RunOptions {
            run_id: run_id.clone(),
            idempotency_key: None,
            deadline_ms: None,
        },
    };
    let response = host.execute(request).await;
    assert!(
        response.error.is_none(),
        "database host returned a categorized failure"
    );
    assert_eq!(response.value["state"], "succeeded");
    assert_eq!(response.value["steps"][1]["remote_effect"], "committed");
    assert_eq!(response.value["reports_complete"], true);
    let rows = client
        .query(
            &format!("SELECT value FROM {target} ORDER BY value NULLS FIRST"),
            &[],
        )
        .await?;
    let values: Vec<Option<String>> = rows.iter().map(|row| row.get(0)).collect();
    assert_eq!(
        values,
        vec![None, Some(String::new()), Some("hello".into())]
    );
    drop(host);
    let restored = DatabaseHost::with_database(
        serde_json::from_slice(&raw_config)?,
        make_connections(),
        store,
        Arc::new(SystemClock),
    )?;
    let result = restored.execute(Request::Result { run_id }).await;
    assert_eq!(result.value, response.value);
    for step in result.value["steps"].as_array().ok_or("steps")? {
        let reference = step["upstream_result"]["artifact_ref"]
            .as_str()
            .ok_or("document")?;
        let document = restored
            .execute(Request::Artifact {
                reference: reference.into(),
            })
            .await;
        assert!(document.error.is_none());
    }
    client
        .batch_execute(&format!("DROP TABLE {source}; DROP TABLE {target};"))
        .await?;
    drop(client);
    driver.await??;
    Ok(())
}
