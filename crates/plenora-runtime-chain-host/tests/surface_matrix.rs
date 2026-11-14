//! Public Rust and transport facade matrix; primary and domain providers are declared doubles.
use plenora_runtime_chain_host::{ScriptedConfig, ScriptedHost};
use plenora_runtime_chains::{ChainClient, Error, Request, Response, Transport};
use plenora_runtime_testkit::ManualClock;
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    error::Error as StdError,
    sync::Arc,
    time::{Duration, UNIX_EPOCH},
};
#[path = "../../plenora-runtime-chains/tests/support/primary.rs"]
mod primary;

struct Loopback(Arc<ScriptedHost<primary::ScriptStore>>);
#[async_trait::async_trait]
impl Transport for Loopback {
    async fn exchange(&self, request: Request) -> Result<Response, Error> {
        Ok(self.0.execute(request).await)
    }
}

#[tokio::test]
async fn public_rust_and_client_visit_every_rt_and_catalog_cell() -> Result<(), Box<dyn StdError>> {
    let matrix: Value = serde_json::from_str(include_str!("fixtures/surfaces.json"))?;
    for surface in ["rust", "runtime"] {
        let mut visited = BTreeSet::new();
        for case in matrix["cases"].as_array().ok_or("cases")? {
            let profile = case["profile"].as_str().ok_or("profile")?;
            let mut config: ScriptedConfig =
                serde_json::from_value(matrix["profiles"][profile].clone())?;
            config.tenant = uuid::Uuid::new_v4().to_string();
            let store = Arc::new(primary::ScriptStore::default());
            store.3.advance(Duration::from_hours(498_492))?;
            let host = Arc::new(ScriptedHost::with_store(
                config,
                store,
                Arc::new(ManualClock::new(UNIX_EPOCH + Duration::from_hours(498_492))),
            )?);
            let client = ChainClient::new(Arc::new(Loopback(host.clone())));
            let mut previous = None;
            for (index, request) in case["requests"]
                .as_array()
                .ok_or("requests")?
                .iter()
                .enumerate()
            {
                let response = if surface == "rust" {
                    host.call(&serde_json::to_vec(request)?).await
                } else {
                    match serde_json::from_value::<Request>(request.clone()) {
                        Ok(request) => client.execute(request).await?,
                        Err(_) => Response {
                            value: Value::Null,
                            error: Some(Error::Invalid),
                        },
                    }
                };
                let actual = serde_json::to_value(&response)?;
                assert_eq!(
                    actual["error"], case["errors"][index],
                    "{} {surface}",
                    case["id"]
                );
                check_axes(case, &actual, previous.as_ref())?;
                if case["profile"] == "catalog" {
                    assert_eq!(actual["value"]["state"], "succeeded");
                    let operations: BTreeSet<_> = actual["value"]["steps"]
                        .as_array()
                        .ok_or("steps")?
                        .iter()
                        .map(|s| {
                            format!(
                                "{}@{}",
                                s["operation"].as_str().unwrap_or(""),
                                s["operation_version"]
                            )
                        })
                        .collect();
                    let expected: BTreeSet<_> = case["checks"]["operations"]
                        .as_array()
                        .ok_or("operations")?
                        .iter()
                        .filter_map(Value::as_str)
                        .map(str::to_owned)
                        .collect();
                    assert_eq!(operations, expected);
                    assert_eq!(
                        actual["value"]["counters"]["max_attempts"],
                        json!(operations.len())
                    );
                    visited.extend(operations);
                }
                previous = Some(actual);
            }
            if case["profile"] != "catalog" {
                visited.insert(case["id"].as_str().ok_or("id")?.to_owned());
            }
        }
        let cells: BTreeSet<_> = matrix["cells"]
            .as_array()
            .ok_or("cells")?
            .iter()
            .filter(|c| c["surface"] == surface)
            .map(|c| c["item"].as_str().unwrap_or("").to_owned())
            .collect();
        assert_eq!(visited, cells);
    }
    Ok(())
}

fn check_axes(
    case: &Value,
    actual: &Value,
    previous: Option<&Value>,
) -> Result<(), Box<dyn StdError>> {
    if case["checks"]["same"] == true
        && let Some(previous) = previous
    {
        assert_eq!(actual, previous);
    }
    if let Some(state) = case["checks"]["state"].as_str() {
        assert_eq!(actual["value"]["state"], state);
    }
    if let Some(category) = case["checks"]["category"].as_str() {
        assert_eq!(actual["failure"]["category"], category);
    }
    if let Some(effect) = case["checks"]["effect"].as_str() {
        assert_eq!(actual["failure"]["remote_effect"], effect);
    }
    if let Some(sentinel) = case["checks"]["no_data"].as_str() {
        assert!(!serde_json::to_string(&actual)?.contains(sentinel));
    }
    if let Some(version) = case["checks"]["operation_version"].as_u64() {
        assert!(
            actual["value"]["operations"]
                .as_array()
                .ok_or("operations")?
                .iter()
                .any(|o| o["version"].as_u64() == Some(version))
        );
    }
    Ok(())
}
