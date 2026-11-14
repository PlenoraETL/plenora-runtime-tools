//! Real broker probes with a declared scripted reply peer, not a domain provider.
use futures_util::StreamExt;
use plenora_runtime_chain_host::RemoteClient;
use plenora_runtime_chains::{Error, Request, Response, Transport};
use std::{error::Error as StdError, time::Duration};

#[tokio::test]
async fn invalid_observation_budget_is_rejected_before_connecting() {
    for budget in [0, 86_400_001, u64::MAX] {
        assert!(matches!(
            RemoteClient::connect_with_timeout("invalid", "valid", budget).await,
            Err(Error::Invalid)
        ));
    }
}

#[tokio::test]
#[ignore = "requires real NATS; collected independently by PostgreSQL and coverage CI campaigns"]
async fn nats_real_observation_timeout_late_reply_and_no_implicit_retry()
-> Result<(), Box<dyn StdError>> {
    let server = std::env::var("PLENORA_NATS_URL")?;
    let broker = async_nats::connect(&server).await?;
    let subject = format!("observation.{}", uuid::Uuid::new_v4().simple());
    let mut requests = broker.subscribe(subject.clone()).await?;
    broker.flush().await?; // Subscribe confirmation is the readiness barrier.
    let (seen, received) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let reply = serde_json::to_vec(&Response {
            value: serde_json::json!("scripted"),
            error: None,
        })?;
        let first = requests.next().await.ok_or("request")?;
        seen.send(()).map_err(|()| "readiness")?;
        released.await?; // Hold the first reply until the client has observed timeout.
        broker
            .publish(first.reply.ok_or("inbox")?, reply.clone().into())
            .await?;
        broker.flush().await?;
        let second = requests.next().await.ok_or("request")?;
        // Explicitly cross the old hidden 10s driver timeout, within the chosen 20s wait.
        tokio::time::sleep(Duration::from_secs(11)).await;
        broker
            .publish(second.reply.ok_or("inbox")?, reply.into())
            .await?;
        broker.flush().await?;
        Ok::<_, Box<dyn StdError + Send + Sync>>(())
    });
    let short = RemoteClient::connect_with_timeout(&server, &subject, 1_000).await?;
    let pending = tokio::spawn(async move { short.exchange(Request::Capabilities).await });
    tokio::time::timeout(Duration::from_secs(10), received).await??;
    assert!(matches!(pending.await?, Err(Error::Timeout)));
    release.send(()).map_err(|()| "release")?;
    let long = RemoteClient::connect_with_timeout(&server, &subject, 20_000).await?;
    let response = long.exchange(Request::Capabilities).await?;
    assert_eq!(response.value, "scripted");
    assert_eq!(response.error, None);
    peer.await?.map_err(|_| "scripted peer failure")?;
    // No subscriber remains: distinguish absent service from a service that is still working.
    let absent = RemoteClient::connect(
        &server,
        &format!("absent.{}", uuid::Uuid::new_v4().simple()),
    )
    .await?;
    assert!(matches!(
        absent.exchange(Request::Capabilities).await,
        Err(Error::Storage)
    ));
    assert!(matches!(
        long.call(vec![0; 1024 * 1024 + 1]).await,
        Err(Error::ResourceLimit)
    ));
    assert!(matches!(
        long.call(b"invalid".to_vec()).await,
        Err(Error::Invalid)
    ));
    Ok(())
}
