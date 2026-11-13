//! Sole step invocation path: registered component, durable wrapper, persisted evidence.
use crate::{
    Adapter, Error, Journal,
    contracts::{Registry, StepResult, parse_plan, parse_result, parse_step_result},
    journal::Intent,
};
use async_trait::async_trait;
use plenora_runtime_boundary::{
    AdmissionPolicy, BoundaryResultSink, CapabilityDispatcher, CapabilityDispatcherConfig,
    CapabilityRegistryBuilder, ComponentOutcome, DeliveryPlan, DeliveryRoute,
    DurableExecutionStore, DurableRuntime, EvidenceComponent, OutcomeBudget, RouteTable,
    RuntimeBoundary, StoredMessage,
};
use plenora_runtime_capabilities::{
    CapabilityDiscovery, CapabilityId, CapabilityRequest, CapabilityResponse, ContractId,
    OperationName, OperationVersion, PlenoraError, PlenoraErrorCategory as Category,
    PlenoraErrorPhase as Phase, PlenoraErrorRemoteEffect as Effect, PlenoraErrorRetry as Retry,
};
use plenora_runtime_core::{Clock, RuntimeHandle, ServiceMetadata};
use plenora_runtime_messaging::{CorrelationId, MessageId, MessageMetadata, SerializedMessage};
use plenora_runtime_worker::WorkerContext;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Wire {
    tenant: String,
    run_id: String,
    plan: Vec<u8>,
    intent: Intent,
    mode: Mode,
}
#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Mode {
    Invoke,
    Recover,
    Cancel,
}
impl Mode {
    fn suffix(self) -> &'static str {
        match self {
            Self::Invoke => "",
            Self::Recover => ".recover",
            Self::Cancel => ".cancel",
        }
    }
}
struct Policy;
impl AdmissionPolicy for Policy {
    fn idempotency_key_limit(&self, _: &str, _: &str, _: &str) -> Option<usize> {
        None
    }
    fn validate_payload(&self, request: &CapabilityRequest) -> Result<(), Category> {
        serde_json::from_slice::<Wire>(&request.input().bytes)
            .map(|_| ())
            .map_err(|_| Category::InvalidConfiguration)
    }
}
struct Component<A> {
    adapter: Arc<A>,
    tenant: String,
    failure: PlenoraError,
}
#[async_trait]
impl<A: Adapter> EvidenceComponent for Component<A> {
    async fn invoke(
        &self,
        _: WorkerContext,
        request: CapabilityRequest,
        budget: OutcomeBudget,
    ) -> ComponentOutcome {
        match self.execute(&request, budget).await {
            Ok(outcome) => outcome,
            Err(_) => budget.failure(self.failure.clone()),
        }
    }
}
impl<A: Adapter> Component<A> {
    async fn execute(
        &self,
        request: &CapabilityRequest,
        budget: OutcomeBudget,
    ) -> Result<ComponentOutcome, Error> {
        let wire: Wire =
            serde_json::from_slice(&request.input().bytes).map_err(|_| Error::Invalid)?;
        if wire.tenant != self.tenant {
            return Err(Error::Invalid);
        }
        let invocation = crate::engine::resolve_invocation(
            &wire.tenant,
            &wire.run_id,
            &parse_plan(&wire.plan)?,
            &parse_result(&wire.intent.prior)?,
            &wire.intent,
        )?;
        if request.capability().name() != capability(&invocation.step.component)
            || request.operation().as_str()
                != format!("{}{}", invocation.step.operation, wire.mode.suffix())
            || u64::from(request.operation_version().get()) != invocation.step.operation_version
            || request.input_contract().as_str() != invocation.step.input_contract
        {
            return Err(Error::Invalid);
        }
        // Read-only admission failures prove that the domain was not invoked. They
        // must not mint Unknown evidence that could later authorize reconciliation.
        if matches!(wire.mode, Mode::Invoke | Mode::Recover)
            && self
                .adapter
                .authorize(&wire.tenant, &parse_plan(&wire.plan)?)
                .await
                .is_err()
        {
            return Ok(budget.failure(preflight_failure(Category::Authorization)?));
        }
        let evidence = match wire.mode {
            Mode::Invoke => {
                if let Err(error) = self.adapter.prepare(&invocation).await {
                    let category = if error == Error::ResourceLimit {
                        Category::ResourceLimit
                    } else {
                        Category::InvalidConfiguration
                    };
                    return Ok(budget.failure(preflight_failure(category)?));
                }
                Some(self.adapter.invoke(&invocation).await?)
            }
            Mode::Recover => self.adapter.recover(&invocation).await?,
            Mode::Cancel => {
                self.adapter.request_cancel(&invocation).await?;
                None
            }
        };
        let effect =
            evidence
                .as_ref()
                .map_or(Ok(Effect::None), |s| match s.remote_effect.as_str() {
                    "none" => Ok(Effect::None),
                    "committed" => Ok(Effect::Committed),
                    "partial" => Ok(Effect::Partial),
                    "unknown" => Ok(Effect::Unknown),
                    "rolled_back" => Ok(Effect::RolledBack),
                    _ => Err(Error::Invalid),
                })?;
        let output = serde_json::to_vec(&evidence).map_err(|_| Error::Invalid)?;
        budget
            .success(
                CapabilityResponse::new(
                    ContractId::new(&invocation.step.output_contract)
                        .map_err(|_| Error::Invalid)?,
                    SerializedMessage::new("application/json", output),
                ),
                Phase::Finalize,
                effect,
            )
            .map_err(|_| Error::Storage)
    }
}
fn preflight_failure(category: Category) -> Result<PlenoraError, Error> {
    PlenoraError::new(
        category,
        Phase::Validate,
        Effect::None,
        Retry::Never,
        "Step admission rejected.",
    )
    .map_err(|_| Error::Invalid)
}
struct SharedClock(Arc<dyn Clock>);
impl Clock for SharedClock {
    fn now(&self) -> std::time::SystemTime {
        self.0.now()
    }
}
fn capability(component: &str) -> String {
    format!("plenora.{component}-tools")
}
fn worker() -> WorkerContext {
    let runtime = RuntimeHandle::new(ServiceMetadata::new("chain", "1", "host"));
    WorkerContext::new(
        MessageId::random(),
        CorrelationId::random(),
        None,
        1,
        MessageMetadata::new(),
        runtime.shutdown_signal(),
    )
}
// The coordinator receives only this restricted view, never an adapter invocation capability.
#[async_trait]
trait Preparation: Send + Sync {
    async fn authorize(&self, tenant: &str, plan: &crate::contracts::Plan) -> Result<(), Error>;
    async fn prepare(&self, invocation: &crate::Invocation) -> Result<(), Error>;
    async fn artifact(&self, tenant: &str, reference: &str, limit: usize)
    -> Result<Vec<u8>, Error>;
}
struct Resources<A>(Arc<A>);
#[async_trait]
impl<A: Adapter> Preparation for Resources<A> {
    async fn authorize(&self, tenant: &str, plan: &crate::contracts::Plan) -> Result<(), Error> {
        self.0.authorize(tenant, plan).await
    }
    async fn prepare(&self, invocation: &crate::Invocation) -> Result<(), Error> {
        self.0.prepare(invocation).await
    }
    async fn artifact(
        &self,
        tenant: &str,
        reference: &str,
        limit: usize,
    ) -> Result<Vec<u8>, Error> {
        self.0.artifact(tenant, reference, limit).await
    }
}
pub(crate) struct StepExecutor<S: DurableExecutionStore> {
    runtime: DurableRuntime<S>,
    resources: Arc<dyn Preparation>,
}
impl<S: DurableExecutionStore> StepExecutor<S> {
    pub(crate) fn new<A: Adapter>(
        store: Arc<S>,
        adapter: Arc<A>,
        registry: &Registry,
        clock: Arc<dyn Clock>,
        tenant: &str,
    ) -> Result<Self, Error> {
        let mut components: BTreeMap<&str, Vec<Value>> = BTreeMap::new();
        for op in &registry.operations {
            for suffix in ["", ".recover", ".cancel"] {
                components.entry(&op.component).or_default().push(json!({"id": format!("{}{}", op.operation, suffix), "version": op.version, "status":"available", "surfaces":["runtime"],
                    "input":{"contract":op.input_contract,"content_types":["application/json"]},
                    "output":{"contract":op.output_contract,"content_types":["application/json"]},
                    "side_effect":"remote","controls":{"deadline":true,"idempotency_key":false,"cancellation":true}}));
            }
        }
        let failure = PlenoraError::new(
            Category::Internal,
            Phase::Finalize,
            Effect::Unknown,
            Retry::RequiresRecovery,
            "Step evidence is unavailable.",
        )
        .map_err(|_| Error::Invalid)?;
        let mut registered = CapabilityRegistryBuilder::default();
        let mut route_discovery = None;
        for (component, operations) in components {
            let discovery = CapabilityDiscovery::from_json(&serde_json::to_vec(&json!({"schema_version":2,"component":format!("plenora-{component}-tools"),"component_version":"1.0.0",
                "interfaces":[{"kind":"runtime","contract":"plenora-runtime-binding-v2","version":2,"artifact":capability(component)}],"operations":operations})).map_err(|_| Error::Invalid)?).map_err(|_| Error::Invalid)?;
            route_discovery = Some(discovery.clone());
            registered
                .register_evidenced(
                    discovery,
                    Component {
                        adapter: adapter.clone(),
                        tenant: tenant.into(),
                        failure: failure.clone(),
                    },
                )
                .map_err(|_| Error::Invalid)?;
        }
        let scope = tenant.to_owned();
        let sink: Arc<dyn BoundaryResultSink> = Arc::new(
            plenora_runtime_boundary::PrimaryResultSink::new(store.clone(), &scope)
                .map_err(|_| Error::Invalid)?,
        );
        let route = "chain.primary.step-results";
        let boundary = RuntimeBoundary::new(
            CapabilityDispatcher::new(
                registered.build(),
                CapabilityDispatcherConfig::new(1024 * 1024).map_err(|_| Error::Invalid)?,
            )
            .map_err(|_| Error::Invalid)?,
            Arc::new(SharedClock(clock)),
            Arc::new(Policy),
            sink.clone(),
        );
        let runtime = DurableRuntime::new(
            boundary,
            store,
            &scope,
            DeliveryPlan::new(route, 1, 0, 1024 * 1024, 1024 * 1024).map_err(|_| Error::Invalid)?,
            RouteTable::new(vec![(
                route.into(),
                route_discovery.ok_or(Error::Invalid)?,
                sink,
            )])
            .map_err(|_| Error::Invalid)?,
        )
        .map_err(|_| Error::Invalid)?;
        Ok(Self {
            runtime,
            resources: Arc::new(Resources(adapter)),
        })
    }
    pub(crate) async fn authorize(
        &self,
        tenant: &str,
        plan: &crate::contracts::Plan,
    ) -> Result<(), Error> {
        self.resources.authorize(tenant, plan).await
    }
    pub(crate) async fn prepare(&self, invocation: &crate::Invocation) -> Result<(), Error> {
        self.resources.prepare(invocation).await
    }
    pub(crate) async fn artifact(
        &self,
        tenant: &str,
        reference: &str,
        limit: usize,
    ) -> Result<Vec<u8>, Error> {
        self.resources.artifact(tenant, reference, limit).await
    }
    fn message(j: &Journal, mode: Mode) -> Result<SerializedMessage, Error> {
        let intent = j.active.as_ref().ok_or(Error::Recovery)?;
        let plan = parse_plan(&j.plan)?;
        let step = plan.steps.get(intent.index).ok_or(Error::Invalid)?;
        let wire = Wire {
            tenant: j.tenant.clone(),
            run_id: j.run_id().into(),
            plan: j.plan.clone(),
            intent: intent.clone(),
            mode,
        };
        let request = CapabilityRequest::new(
            CapabilityId::new(capability(&step.component), 1).map_err(|_| Error::Invalid)?,
            OperationName::new(format!("{}{}", step.operation, mode.suffix()))
                .map_err(|_| Error::Invalid)?,
            OperationVersion::new(
                u16::try_from(step.operation_version).map_err(|_| Error::Invalid)?,
            )
            .map_err(|_| Error::Invalid)?,
            ContractId::new(&step.input_contract).map_err(|_| Error::Invalid)?,
            SerializedMessage::new(
                "application/json",
                serde_json::to_vec(&wire).map_err(|_| Error::Invalid)?,
            ),
        );
        let mut message = plenora_runtime_capabilities::CapabilityMessageCodec
            .encode(&request)
            .map_err(|_| Error::Invalid)?;
        let identity = if matches!(mode, Mode::Invoke) {
            intent.message_id.clone()
        } else {
            uuid::Uuid::new_v4().to_string()
        };
        for (key, value) in [
            (plenora_runtime_messaging::MESSAGE_ID_METADATA_KEY, identity),
            (
                plenora_runtime_messaging::CORRELATION_ID_METADATA_KEY,
                j.run_id().into(),
            ),
            (
                "plenora.negotiation.requires",
                "plenora-runtime-binding-v2".into(),
            ),
            ("plenora.negotiation.accepts_downgrade", "false".into()),
        ] {
            message
                .headers
                .insert_text(key, value)
                .map_err(|_| Error::Invalid)?;
        }
        if let Some(causation) = &intent.causation {
            message
                .headers
                .insert_text(
                    plenora_runtime_messaging::CAUSATION_ID_METADATA_KEY,
                    causation,
                )
                .map_err(|_| Error::Invalid)?;
        }
        if matches!(mode, Mode::Invoke) {
            message
                .headers
                .insert_text(
                    plenora_runtime_capabilities::EXECUTION_DEADLINE_METADATA_KEY,
                    crate::timestamp(intent.deadline)?,
                )
                .map_err(|_| Error::Invalid)?;
        }
        Ok(message)
    }
    async fn execute(&self, message: &SerializedMessage) -> Result<Option<StepResult>, Error> {
        let prepared = self
            .runtime
            .process_confirmed(message, DeliveryRoute::Ingress, worker())
            .await
            .map_err(|_| Error::Recovery)?;
        let output = prepared
            .report()
            .get("output")
            .ok_or(Error::Recovery)?
            .clone();
        let message: StoredMessage = serde_json::from_value(output).map_err(|_| Error::Recovery)?;
        let message = message.restore().map_err(|_| Error::Recovery)?;
        if message.bytes.as_ref() == b"null" {
            return Ok(None);
        }
        parse_step_result(&message.bytes)
            .map(Some)
            .map_err(|_| Error::Recovery)
    }
    pub(crate) async fn confirmed_unknown(&self, j: &Journal) -> Result<bool, Error> {
        let prepared = self
            .runtime
            .evidence(&Self::message(j, Mode::Invoke)?)
            .await
            .map_err(|_| Error::Recovery)?;
        Ok(prepared.report()["remote_effect"] == "unknown")
    }
    pub(crate) async fn invoke(&self, j: &Journal) -> Result<StepResult, Error> {
        self.execute(&Self::message(j, Mode::Invoke)?)
            .await?
            .ok_or(Error::Recovery)
    }
    pub(crate) async fn recover(&self, j: &Journal) -> Result<Option<StepResult>, Error> {
        let original = Self::message(j, Mode::Invoke)?;
        let (stage, attempt) = self
            .runtime
            .receipt_state(&original)
            .await
            .map_err(|_| Error::Recovery)?;
        if stage == plenora_runtime_boundary::DeliveryStage::Ready || attempt == 0 {
            return Err(Error::Recovery);
        }
        if matches!(
            stage,
            plenora_runtime_boundary::DeliveryStage::Evidence
                | plenora_runtime_boundary::DeliveryStage::Finished
        ) {
            // Finalize the receipt before consuming proof; rejection is never reconciliation authority.
            let prepared = self
                .runtime
                .process_confirmed(&original, DeliveryRoute::Ingress, worker())
                .await
                .map_err(|_| Error::Recovery)?;
            if let Some(output) = prepared.report().get("output") {
                let stored: StoredMessage =
                    serde_json::from_value(output.clone()).map_err(|_| Error::Recovery)?;
                let message = stored.restore().map_err(|_| Error::Recovery)?;
                let step = parse_step_result(&message.bytes).map_err(|_| Error::Recovery)?;
                if step.remote_effect != "unknown" {
                    return Ok(Some(step));
                }
            } else if prepared.report()["remote_effect"] != "unknown" {
                return Err(Error::Recovery);
            }
        }
        // Reconciliation is a separate protected, read-only receipt; never another Invoke.
        self.execute(&Self::message(j, Mode::Recover)?).await
    }
    pub(crate) async fn cancel(&self, j: &Journal) -> Result<(), Error> {
        self.execute(&Self::message(j, Mode::Cancel)?)
            .await
            .map(|_| ())
    }
}
