//! Real broker qualification of the explicitly scripted facade, separate from P2 settlement.
use plenora_runtime_chain_host::{RemoteClient, ScriptedConfig, ScriptedHost};
use plenora_runtime_chains::{Request, Transport};
use std::{collections::BTreeMap, error::Error, sync::Arc, time::Duration};

#[tokio::test]
#[ignore = "Requires ephemeral NATS; no fake broker replacement"]
async fn nats_real_scripted_calls_preserve_errors_and_discovery() -> Result<(), Box<dyn Error>> {
    let server = std::env::var("PLENORA_NATS_URL")?;
    let subject = format!("scripted.{}", uuid::Uuid::new_v4().simple());
    let host = Arc::new(ScriptedHost::new(ScriptedConfig {
        database: "host=127.0.0.1 port=1 dbname=not_running".into(),
        tenant: "qualification".into(),
        registry: include_str!("../../../contracts/chains-v2/fixtures/context.json").into(),
        outcomes: include_str!("../../../contracts/chains-v2/fixtures/result-valid.json").into(),
        recovery: None,
        artifacts: BTreeMap::new(),
    })?);
    let (ready, received) = tokio::sync::oneshot::channel();
    let serving = host.clone();
    let address = server.clone();
    let destination = subject.clone();
    let task = tokio::spawn(async move { serving.serve(&address, &destination, ready).await });
    tokio::time::timeout(Duration::from_secs(10), received).await??;
    let client = RemoteClient::connect(&server, &subject).await?;
    for request in [
        Request::Capabilities,
        Request::Validate { plan: "{}".into() },
        Request::Status {
            run_id: "bad".into(),
        },
    ] {
        let bytes = serde_json::to_vec(&request)?;
        let local = host.call(&bytes).await;
        let remote = client.exchange(request).await?;
        assert_eq!(local.error, remote.error);
        assert_eq!(local.value, remote.value);
    }
    task.abort();
    assert!(task.await.is_err_and(|error| error.is_cancelled()));
    Ok(())
}
