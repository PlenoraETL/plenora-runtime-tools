//! Protected host operations and lossless external transport carrier.
use async_trait::async_trait;
use plenora_runtime_boundary::{
    AdmissionPolicy, BoundaryResultSink, CapabilityDispatcher, CapabilityDispatcherConfig,
    CapabilityRegistryBuilder, ComponentOutcome, DeliveryPlan, DurableExecutionStore,
    DurableRuntime, EvidenceComponent, OutcomeBudget, PrimaryResultSink, RouteTable,
    RuntimeBoundary, StoredMessage,
};
use plenora_runtime_capabilities::{
    CapabilityDiscovery, CapabilityId, CapabilityRequest, CapabilityResponse, ContractId,
    IDEMPOTENCY_KEY_METADATA_KEY as KEY, OperationName, OperationVersion, PlenoraError,
    PlenoraErrorCategory as Category, PlenoraErrorPhase as Phase,
    PlenoraErrorRemoteEffect as Effect, PlenoraErrorRetry as Retry,
};
use plenora_runtime_chains::{ChainEngine, Error, FUNCTIONS, Request, Response, Store};
use plenora_runtime_core::{Clock, RuntimeHandle, ServiceMetadata};
use plenora_runtime_messaging::{CorrelationId, MessageId, MessageMetadata, SerializedMessage};
use plenora_runtime_worker::WorkerContext;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;
const INPUT: &str = "plenora-chain-call-v2";
const OUTPUT: &str = "plenora-chain-response-v2";
const CAPABILITY: &str = "plenora.runtime-tools";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Wire {
    content_type: String,
    bytes: Vec<u8>,
    headers: Vec<(String, Vec<u8>)>,
}
impl Wire {
    pub(crate) fn capture(message: &SerializedMessage) -> Self {
        Self {
            content_type: message.content_type.to_string(),
            bytes: message.bytes.to_vec(),
            headers: message
                .headers
                .iter()
                .map(|(k, v)| (k.into(), v.to_vec()))
                .collect(),
        }
    }
    pub(crate) fn restore(self) -> SerializedMessage {
        SerializedMessage::new(self.content_type, self.bytes).with_raw_metadata(
            self.headers
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
        )
    }
}
struct Policy {
    registry: plenora_runtime_chains::contracts::Registry,
}
fn decode(request: &CapabilityRequest) -> Result<Request, Category> {
    let call: Request = serde_json::from_slice(&request.input().bytes)
        .map_err(|_| Category::InvalidConfiguration)?;
    let value = serde_json::to_value(&call).map_err(|_| Category::Internal)?;
    if request.operation().as_str()
        != format!(
            "chain.{}",
            value["function"]
                .as_str()
                .ok_or(Category::InvalidConfiguration)?
        )
    {
        return Err(Category::InvalidConfiguration);
    }
    if let Request::Submit { options, .. } | Request::Run { options, .. } = &call
        && (options.idempotency_key.is_some()
            || !uuid::Uuid::parse_str(&options.run_id)
                .is_ok_and(|id| id.to_string() == options.run_id))
    {
        return Err(Category::InvalidConfiguration);
    }
    Ok(call)
}
impl AdmissionPolicy for Policy {
    fn idempotency_key_limit(
        &self,
        capability: &str,
        operation: &str,
        version: &str,
    ) -> Option<usize> {
        (capability == CAPABILITY
            && matches!(operation, "chain.submit" | "chain.run")
            && version == "2")
            .then_some(256)
    }
    fn validate_payload(&self, request: &CapabilityRequest) -> Result<(), Category> {
        let call = decode(request)?;
        if request
            .input()
            .headers
            .get_text(KEY)
            .map_err(|_| Category::Protocol)?
            .is_some_and(|key| {
                key.is_empty() || key.len() > 256 || !key.bytes().all(|b| b.is_ascii_graphic())
            })
        {
            return Err(Category::Protocol);
        }
        if let Request::Submit { plan, .. } | Request::Run { plan, .. } = call {
            let plan = plenora_runtime_chains::contracts::parse_plan(plan.as_bytes())
                .map_err(|_| Category::InvalidConfiguration)?;
            plenora_runtime_chains::contracts::validate_plan(&plan, &self.registry)
                .map_err(|_| Category::InvalidConfiguration)?;
        }
        Ok(())
    }
    fn idempotency_payload(&self, request: &CapabilityRequest) -> Result<Vec<u8>, Category> {
        let call = decode(request)?;
        let (Request::Submit { plan, options } | Request::Run { plan, options }) = call else {
            return Err(Category::Unsupported);
        };
        // Only the candidate receipt identity is nonbinding. Plan, consent, destinations and deadline remain exact.
        serde_json::to_vec(&(plan, options.deadline_ms)).map_err(|_| Category::Internal)
    }
}
struct Component<S: Store + DurableExecutionStore> {
    engine: Arc<ChainEngine<S>>,
    failure: PlenoraError,
}
#[async_trait]
impl<S: Store + DurableExecutionStore> EvidenceComponent for Component<S> {
    async fn invoke(
        &self,
        _: WorkerContext,
        request: CapabilityRequest,
        budget: OutcomeBudget,
    ) -> ComponentOutcome {
        match self.execute(request, budget).await {
            Ok(outcome) => outcome,
            Err(_) => budget.failure(self.failure.clone()),
        }
    }
}
impl<S: Store + DurableExecutionStore> Component<S> {
    async fn execute(
        &self,
        request: CapabilityRequest,
        budget: OutcomeBudget,
    ) -> Result<ComponentOutcome, Error> {
        let call = decode(&request).map_err(|_| Error::Invalid)?;
        let is_run = matches!(call, Request::Run { .. });
        let reply = self.engine.execute(call).await;
        let effect = if is_run {
            reply
                .value
                .get("remote_effect")
                .map_or(Ok(Effect::Unknown), |v| match v.as_str() {
                    Some("none") => Ok(Effect::None),
                    Some("committed") => Ok(Effect::Committed),
                    Some("partial") => Ok(Effect::Partial),
                    Some("unknown") => Ok(Effect::Unknown),
                    Some("rolled_back") => Ok(Effect::RolledBack),
                    _ => Err(Error::Invalid),
                })?
        } else {
            Effect::None
        };
        let response = CapabilityResponse::new(
            ContractId::new(OUTPUT).map_err(|_| Error::Invalid)?,
            SerializedMessage::new(
                "application/json",
                serde_json::to_vec(&reply).map_err(|_| Error::Invalid)?,
            ),
        );
        budget
            .success(response, Phase::Finalize, effect)
            .map_err(|_| Error::Storage)
    }
}
struct SharedClock(Arc<dyn Clock>);
impl Clock for SharedClock {
    fn now(&self) -> std::time::SystemTime {
        self.0.now()
    }
}
pub(crate) fn runtime<S: Store + DurableExecutionStore>(
    store: Arc<S>,
    engine: Arc<ChainEngine<S>>,
    clock: Arc<dyn Clock>,
    tenant: &str,
) -> Result<DurableRuntime<S>, Error> {
    let operations:Vec<_>=FUNCTIONS.iter().map(|name|json!({"id":format!("chain.{name}"),"version":2,"status":"available","surfaces":["runtime"],"input":{"contract":INPUT,"content_types":["application/json"]},"output":{"contract":OUTPUT,"content_types":["application/json"]},"side_effect":"remote","controls":{"deadline":true,"idempotency_key":matches!(*name,"submit"|"run"),"cancellation":true}})).collect();
    let discovery=CapabilityDiscovery::from_json(&serde_json::to_vec(&json!({"schema_version":2,"component":"plenora-runtime-tools","component_version":"1.0.0","interfaces":[{"kind":"runtime","contract":"plenora-runtime-binding-v2","version":2,"artifact":CAPABILITY}],"operations":operations})).map_err(|_| Error::Invalid)?).map_err(|_| Error::Invalid)?;
    let policy = Policy {
        registry: engine.capabilities().clone(),
    };
    let mut registered = CapabilityRegistryBuilder::default();
    registered
        .register_evidenced(
            discovery.clone(),
            Component {
                engine,
                failure: PlenoraError::new(
                    Category::Internal,
                    Phase::Finalize,
                    Effect::Unknown,
                    Retry::RequiresRecovery,
                    "Host result is unavailable.",
                )
                .map_err(|_| Error::Invalid)?,
            },
        )
        .map_err(|_| Error::Invalid)?;
    let sink: Arc<dyn BoundaryResultSink> =
        Arc::new(PrimaryResultSink::new(store.clone(), tenant).map_err(|_| Error::Invalid)?);
    let boundary = RuntimeBoundary::new(
        CapabilityDispatcher::new(
            registered.build(),
            CapabilityDispatcherConfig::new(1024 * 1024).map_err(|_| Error::Invalid)?,
        )
        .map_err(|_| Error::Invalid)?,
        Arc::new(SharedClock(clock)),
        Arc::new(policy),
        sink.clone(),
    );
    let route = "chain.primary.control-results";
    DurableRuntime::new(
        boundary,
        store,
        tenant,
        DeliveryPlan::new(route, 1, 0, 1024 * 1024, 1024 * 1024).map_err(|_| Error::Invalid)?,
        RouteTable::new(vec![(route.into(), discovery, sink)]).map_err(|_| Error::Invalid)?,
    )
    .map_err(|_| Error::Invalid)
}
pub(crate) fn worker() -> WorkerContext {
    let runtime = RuntimeHandle::new(ServiceMetadata::new("chain-host", "1", "host"));
    WorkerContext::new(
        MessageId::random(),
        CorrelationId::random(),
        None,
        1,
        MessageMetadata::new(),
        runtime.shutdown_signal(),
    )
}
pub(crate) fn message(mut call: Request) -> Result<SerializedMessage, Error> {
    let key = match &mut call {
        Request::Submit { options, .. } | Request::Run { options, .. } => {
            options.idempotency_key.take()
        }
        _ => None,
    };
    let value = serde_json::to_value(&call).map_err(|_| Error::Invalid)?;
    let name = value["function"].as_str().ok_or(Error::Invalid)?;
    let request = CapabilityRequest::new(
        CapabilityId::new(CAPABILITY, 1).map_err(|_| Error::Invalid)?,
        OperationName::new(format!("chain.{name}")).map_err(|_| Error::Invalid)?,
        OperationVersion::new(2).map_err(|_| Error::Invalid)?,
        ContractId::new(INPUT).map_err(|_| Error::Invalid)?,
        SerializedMessage::new(
            "application/json",
            serde_json::to_vec(&call).map_err(|_| Error::Invalid)?,
        ),
    );
    let mut message = plenora_runtime_capabilities::CapabilityMessageCodec
        .encode(&request)
        .map_err(|_| Error::Invalid)?;
    for (k, v) in [
        (
            plenora_runtime_messaging::MESSAGE_ID_METADATA_KEY,
            uuid::Uuid::new_v4().to_string(),
        ),
        (
            plenora_runtime_messaging::CORRELATION_ID_METADATA_KEY,
            uuid::Uuid::new_v4().to_string(),
        ),
        (
            "plenora.negotiation.requires",
            "plenora-runtime-binding-v2".into(),
        ),
        ("plenora.negotiation.accepts_downgrade", "false".into()),
    ] {
        message
            .headers
            .insert_text(k, v)
            .map_err(|_| Error::Invalid)?;
    }
    if let Some(key) = key {
        message
            .headers
            .insert_text(KEY, key)
            .map_err(|_| Error::Invalid)?;
    }
    Ok(message)
}
pub(crate) fn error(error: Error) -> Response {
    Response {
        value: Value::Null,
        error: Some(error),
    }
}
pub(crate) fn response(terminal: &SerializedMessage, report: &Value) -> Response {
    if let Some(output) = report.get("output")
        && let Ok(stored) = serde_json::from_value::<StoredMessage>(output.clone())
        && let Ok(message) = stored.restore()
        && let Ok(reply) = serde_json::from_slice(&message.bytes)
    {
        return reply;
    }

    let value: Value = serde_json::from_slice(&terminal.bytes).unwrap_or(Value::Null);
    error(match value["category"].as_str() {
        Some("protocol" | "invalid_configuration") => Error::Invalid,
        Some("unsupported") => Error::Unsupported,
        Some("timeout") => Error::Timeout,
        Some("resource_limit") => Error::ResourceLimit,
        Some("conflict") => Error::Conflict,
        _ => Error::Recovery,
    })
}
