//! Dependency-direction and production-source safety checks.

use std::{
    error::Error,
    ffi::OsStr,
    fmt::{self, Display, Formatter},
    fs, io,
    ops::Range,
    path::{Path, PathBuf},
};

#[derive(Debug)]
struct ArchitectureViolation(String);

impl Display for ArchitectureViolation {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for ArchitectureViolation {}

#[derive(Clone, Copy)]
enum LiteralPolicy {
    Mask,
    Preserve,
}

#[test]
fn runtime_core_has_no_adapter_or_engine_dependency() -> Result<(), Box<dyn Error>> {
    assert_terms_absent(
        &workspace_root()?,
        "crates/plenora-runtime-core",
        &[
            "async-nats",
            "async_nats",
            "apalis",
            "plenora-runtime-nats",
            "plenora_runtime_nats",
            "plenora-runtime-apalis",
            "plenora_runtime_apalis",
        ],
    )
}

#[test]
fn runtime_messaging_has_no_nats_dependency() -> Result<(), Box<dyn Error>> {
    assert_terms_absent(
        &workspace_root()?,
        "crates/plenora-runtime-messaging",
        &[
            "async-nats",
            "async_nats",
            "plenora-runtime-nats",
            "plenora_runtime_nats",
        ],
    )
}

#[test]
fn runtime_worker_has_no_apalis_dependency_or_type_leakage() -> Result<(), Box<dyn Error>> {
    assert_terms_absent(
        &workspace_root()?,
        "crates/plenora-runtime-worker",
        &["apalis", "plenora-runtime-apalis", "plenora_runtime_apalis"],
    )
}

#[test]
fn runtime_capabilities_has_no_concrete_library_or_adapter_dependency() -> Result<(), Box<dyn Error>>
{
    assert_terms_absent(
        &workspace_root()?,
        "crates/plenora-runtime-capabilities",
        &[
            "async-nats",
            "async_nats",
            "apalis",
            "axum",
            "tower-http",
            "tower_http",
            "opentelemetry",
            "sqlx",
            "diesel",
            "data-tools",
            "data_tools",
            "database-tools",
            "database_tools",
            "io-tools",
            "io_tools",
            "reqwest",
            "hyper",
            "ureq",
            "oauth2",
            "openmeteo",
            "sister",
        ],
    )
}

#[test]
fn runtime_outbox_has_no_database_driver_dependency() -> Result<(), Box<dyn Error>> {
    assert_terms_absent(
        &workspace_root()?,
        "crates/plenora-runtime-outbox",
        &[
            "postgres",
            "tokio-postgres",
            "tokio_postgres",
            "sqlx",
            "diesel",
            "sea-orm",
            "sea_orm",
            "rusqlite",
        ],
    )
}

#[test]
fn runtime_scheduler_has_no_worker_broker_or_database_dependency() -> Result<(), Box<dyn Error>> {
    assert_terms_absent(
        &workspace_root()?,
        "crates/plenora-runtime-scheduler",
        &[
            "async-nats",
            "async_nats",
            "apalis",
            "plenora-runtime-nats",
            "plenora_runtime_nats",
            "plenora-runtime-apalis",
            "plenora_runtime_apalis",
            "plenora-runtime-worker",
            "plenora_runtime_worker",
            "postgres",
            "sqlx",
            "diesel",
        ],
    )
}

#[test]
fn runtime_subprocess_has_no_application_worker_broker_or_database_dependency()
-> Result<(), Box<dyn Error>> {
    assert_terms_absent(
        &workspace_root()?,
        "crates/plenora-runtime-subprocess",
        &[
            "async-nats",
            "async_nats",
            "apalis",
            "plenora-runtime-worker",
            "plenora_runtime_worker",
            "plenora-runtime-control",
            "plenora_runtime_control",
            "data-tools",
            "data_tools",
            "database-tools",
            "database_tools",
            "io-tools",
            "io_tools",
            "postgres",
            "sqlx",
            "diesel",
        ],
    )
}

#[test]
fn runtime_control_has_no_transport_engine_database_or_application_dependency()
-> Result<(), Box<dyn Error>> {
    assert_terms_absent(
        &workspace_root()?,
        "crates/plenora-runtime-control",
        &[
            "async-nats",
            "async_nats",
            "apalis",
            "axum",
            "tower-http",
            "tower_http",
            "data-tools",
            "data_tools",
            "database-tools",
            "database_tools",
            "io-tools",
            "io_tools",
            "postgres",
            "sqlx",
            "diesel",
        ],
    )
}

#[test]
fn runtime_resources_has_no_provider_database_or_telemetry_dependency() -> Result<(), Box<dyn Error>>
{
    assert_terms_absent(
        &workspace_root()?,
        "crates/plenora-runtime-resources",
        &[
            "async-nats",
            "async_nats",
            "apalis",
            "axum",
            "tower-http",
            "tower_http",
            "opentelemetry",
            "tracing-opentelemetry",
            "tracing_opentelemetry",
            "postgres",
            "sqlx",
            "diesel",
        ],
    )
}

#[test]
fn foundational_crates_do_not_depend_on_http_or_telemetry_adapters() -> Result<(), Box<dyn Error>> {
    let root = workspace_root()?;
    for package in [
        "crates/plenora-runtime-core",
        "crates/plenora-runtime-messaging",
        "crates/plenora-runtime-outbox",
        "crates/plenora-runtime-resources",
        "crates/plenora-runtime-scheduler",
    ] {
        assert_terms_absent(
            &root,
            package,
            &[
                "axum",
                "tower-http",
                "tower_http",
                "opentelemetry",
                "tracing-opentelemetry",
                "tracing_opentelemetry",
            ],
        )?;
    }
    Ok(())
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "the dependency policy is intentionally one auditable declarative matrix"
)]
fn internal_dependency_direction_has_no_reverse_edges() -> Result<(), Box<dyn Error>> {
    let root = workspace_root()?;
    for (package, forbidden) in [
        (
            "crates/plenora-runtime-core",
            &[
                "plenora-runtime-messaging",
                "plenora-runtime-worker",
                "plenora-runtime-outbox",
                "plenora-runtime-http",
                "plenora-runtime-observability",
                "plenora-runtime-capabilities",
                "plenora-runtime-testkit",
                "plenora-runtime-resources",
                "plenora-runtime-scheduler",
                "plenora-runtime-subprocess",
                "plenora-runtime-control",
            ][..],
        ),
        (
            "crates/plenora-runtime-messaging",
            &[
                "plenora-runtime-worker",
                "plenora-runtime-outbox",
                "plenora-runtime-http",
                "plenora-runtime-observability",
                "plenora-runtime-capabilities",
                "plenora-runtime-testkit",
                "plenora-runtime-apalis",
                "plenora-runtime-nats",
                "plenora-runtime-resources",
                "plenora-runtime-scheduler",
                "plenora-runtime-subprocess",
                "plenora-runtime-control",
            ][..],
        ),
        (
            "crates/plenora-runtime-worker",
            &[
                "plenora-runtime-outbox",
                "plenora-runtime-http",
                "plenora-runtime-observability",
                "plenora-runtime-capabilities",
                "plenora-runtime-testkit",
                "plenora-runtime-apalis",
                "plenora-runtime-nats",
                "plenora-runtime-resources",
                "plenora-runtime-scheduler",
                "plenora-runtime-subprocess",
                "plenora-runtime-control",
            ][..],
        ),
        (
            "crates/plenora-runtime-outbox",
            &[
                "plenora-runtime-http",
                "plenora-runtime-observability",
                "plenora-runtime-capabilities",
                "plenora-runtime-testkit",
                "plenora-runtime-apalis",
                "plenora-runtime-nats",
                "plenora-runtime-subprocess",
                "plenora-runtime-control",
            ][..],
        ),
        (
            "crates/plenora-runtime-http",
            &[
                "plenora-runtime-worker",
                "plenora-runtime-outbox",
                "plenora-runtime-observability",
                "plenora-runtime-capabilities",
                "plenora-runtime-testkit",
                "plenora-runtime-apalis",
                "plenora-runtime-nats",
                "plenora-runtime-subprocess",
                "plenora-runtime-control",
            ][..],
        ),
        (
            "crates/plenora-runtime-observability",
            &[
                "plenora-runtime-http",
                "plenora-runtime-worker",
                "plenora-runtime-outbox",
                "plenora-runtime-capabilities",
                "plenora-runtime-testkit",
                "plenora-runtime-apalis",
                "plenora-runtime-nats",
                "plenora-runtime-subprocess",
                "plenora-runtime-control",
            ][..],
        ),
        (
            "crates/plenora-runtime-capabilities",
            &[
                "plenora-runtime-outbox",
                "plenora-runtime-http",
                "plenora-runtime-observability",
                "plenora-runtime-testkit",
                "plenora-runtime-apalis",
                "plenora-runtime-nats",
                "plenora-runtime-subprocess",
                "plenora-runtime-control",
            ][..],
        ),
    ] {
        assert_manifest_terms_absent(&root, package, forbidden)?;
    }
    Ok(())
}

#[test]
fn observability_remains_exporter_sdk_vendor_and_backend_neutral() -> Result<(), Box<dyn Error>> {
    let root = workspace_root()?;
    let package = "crates/plenora-runtime-observability";
    assert_manifest_terms_absent(
        &root,
        package,
        &[
            "opentelemetry_sdk",
            "opentelemetry-sdk",
            "opentelemetry-otlp",
            "opentelemetry-stdout",
            "opentelemetry-jaeger",
            "opentelemetry-zipkin",
            "opentelemetry-prometheus",
            "tracing-appender",
            "tracing-journald",
            "datadog",
            "dynatrace",
            "grafana",
            "honeycomb",
            "jaeger",
            "newrelic",
            "prometheus",
            "sentry",
            "tempo",
            "zipkin",
        ],
    )?;
    assert_source_identifier_fragments_absent(
        &root,
        package,
        &[
            "exporter",
            "opentelemetry_sdk",
            "otel_sdk",
            "datadog",
            "dynatrace",
            "grafana",
            "honeycomb",
            "jaeger",
            "newrelic",
            "prometheus",
            "sentry",
            "tempo",
            "zipkin",
        ],
        LiteralPolicy::Preserve,
    )
}

#[test]
fn runtime_http_has_no_auth_business_pfm_or_provider_scope() -> Result<(), Box<dyn Error>> {
    let root = workspace_root()?;
    let package = "crates/plenora-runtime-http";
    assert_manifest_terms_absent(
        &root,
        package,
        &[
            "aliri",
            "axum-auth",
            "axum-login",
            "casbin",
            "jsonwebtoken",
            "oauth2",
            "openidconnect",
            "oso",
            "plenora-auth",
            "plenora-authorization",
            "plenora-pfm",
        ],
    )?;
    assert_source_identifier_fragments_absent(
        &root,
        package,
        &[
            "authentication",
            "authenticator",
            "authorization",
            "authorizer",
            "business",
            "credential",
            "jwt",
            "oauth",
            "oidc",
            "password",
            "pfm",
            "private_key",
            "secret",
        ],
        LiteralPolicy::Preserve,
    )?;
    assert_source_identifiers_absent(
        &root,
        package,
        &["auth", "authn", "authz"],
        LiteralPolicy::Preserve,
    )
}

#[test]
fn all_dependency_versions_are_exactly_pinned_or_workspace_inherited() -> Result<(), Box<dyn Error>>
{
    let root = workspace_root()?;
    let mut violations = Vec::new();

    for manifest_path in workspace_manifests(&root)? {
        let manifest = strip_toml_comments(&fs::read_to_string(&manifest_path)?);
        let mut dependency_section = false;
        for line in manifest.lines() {
            let trimmed = line.trim();
            if trimmed.starts_with('[') {
                dependency_section = toml_section_name(trimmed)
                    .is_some_and(|section| section.contains("dependencies"));
                continue;
            }
            if !dependency_section {
                continue;
            }
            if ["git", "branch", "tag", "rev"]
                .iter()
                .any(|key| quoted_assignment_value(trimmed, key).is_some())
            {
                violations.push(format!(
                    "{} contains a non-registry dependency source",
                    relative_path(&root, &manifest_path)
                ));
            }
            if let Some(version) = quoted_assignment_value(trimmed, "version")
                .or_else(|| bare_dependency_version(trimmed))
                && !version.starts_with('=')
            {
                violations.push(format!(
                    "{} contains non-exact dependency version {version}",
                    relative_path(&root, &manifest_path)
                ));
            }
        }
    }

    finish_violations(violations)
}

#[test]
fn workspace_and_all_members_enforce_shared_safety_lints() -> Result<(), Box<dyn Error>> {
    let root = workspace_root()?;
    let root_manifest = root.join("Cargo.toml");
    let root_contents = strip_toml_comments(&fs::read_to_string(&root_manifest)?);
    let root_compact: String = root_contents
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect();
    let mut violations = Vec::new();
    for required in [
        "[workspace.lints.rust]",
        r#"missing_docs="warn""#,
        r#"unsafe_code="forbid""#,
        r#"expect_used="deny""#,
        r#"panic="deny""#,
        r#"unimplemented="deny""#,
        r#"unwrap_used="deny""#,
    ] {
        if !root_compact.contains(required) {
            violations.push(format!(
                "{} is missing shared safety lint {required}",
                relative_path(&root, &root_manifest)
            ));
        }
    }

    for manifest in workspace_manifests(&root)?
        .into_iter()
        .filter(|manifest| manifest != &root_manifest)
    {
        let contents = strip_toml_comments(&fs::read_to_string(&manifest)?);
        let compact: String = contents
            .chars()
            .filter(|character| !character.is_whitespace())
            .collect();
        if !compact.contains("[lints]workspace=true") {
            violations.push(format!(
                "{} does not inherit workspace lints",
                relative_path(&root, &manifest)
            ));
        }
    }

    finish_violations(violations)
}

#[test]
fn workspace_members_and_ci_share_msrv_and_same_sha_qualification() -> Result<(), Box<dyn Error>> {
    let root = workspace_root()?;
    let root_manifest = root.join("Cargo.toml");
    let root_contents = strip_toml_comments(&fs::read_to_string(&root_manifest)?);
    let msrv = toml_section_assignment(&root_contents, "workspace.package", "rust-version")
        .ok_or_else(|| {
            ArchitectureViolation("workspace.package is missing rust-version".to_owned())
        })?;
    let mut violations = Vec::new();

    for manifest in workspace_manifests(&root)?
        .into_iter()
        .filter(|manifest| manifest != &root_manifest)
    {
        let contents = strip_toml_comments(&fs::read_to_string(&manifest)?);
        let compact: String = contents
            .chars()
            .filter(|character| !character.is_whitespace())
            .collect();
        if !compact.contains("rust-version.workspace=true") {
            violations.push(format!(
                "{} does not inherit the workspace MSRV {msrv}",
                relative_path(&root, &manifest)
            ));
        }
    }

    let workflow_path = root.join(".github/workflows/ci.yml");
    let workflow = fs::read_to_string(&workflow_path)?;
    let workflow_compact: String = workflow
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect();
    for required in [
        "workflow_dispatch:",
        "expected_sha:",
        "required:true",
        r"ACTUAL_SHA:${{github.sha}}",
        r"EXPECTED_SHA:${{inputs.expected_sha}}",
        r#"run:test"$EXPECTED_SHA"="$ACTUAL_SHA""#,
    ] {
        if !workflow_compact.contains(required) {
            violations.push(format!(
                "{} is missing same-SHA qualification invariant {required}",
                relative_path(&root, &workflow_path)
            ));
        }
    }

    let jobs = workflow_job_blocks(&workflow);
    if !jobs.iter().any(|(name, _)| name == "qualification-sha") {
        violations.push(format!(
            "{} has no qualification-sha job",
            relative_path(&root, &workflow_path)
        ));
    }
    for (name, body) in jobs {
        if name == "qualification-sha" {
            continue;
        }
        if !body
            .lines()
            .any(|line| line.trim() == "needs: qualification-sha")
        {
            violations.push(format!(
                "{} job {name} does not depend on qualification-sha",
                relative_path(&root, &workflow_path)
            ));
        }
        if body.contains("cargo ") {
            let compact: String = body
                .chars()
                .filter(|character| !character.is_whitespace())
                .collect();
            for required in [
                format!("rustuptoolchaininstall{msrv}"),
                format!("rustupdefault{msrv}"),
            ] {
                if !compact.contains(&required) {
                    violations.push(format!(
                        "{} job {name} does not run on workspace MSRV {msrv}",
                        relative_path(&root, &workflow_path)
                    ));
                }
            }
        }
    }

    finish_violations(violations)
}

#[test]
fn qualification_docs_preserve_open_release_and_security_gates() -> Result<(), Box<dyn Error>> {
    let root = workspace_root()?;
    let qualification_path = root.join("docs/qualification.md");
    let qualification = fs::read_to_string(&qualification_path)?;
    let security_path = root.join("docs/security-review.md");
    let security = fs::read_to_string(&security_path)?;
    let workflow_path = root.join(".github/workflows/ci.yml");
    let workflow = fs::read_to_string(&workflow_path)?;
    let mut violations = Vec::new();

    for required in [
        "CI on Windows, Linux, and macOS",
        "Pending commit",
        "a real Plenora consumer",
        "a PFM microservice proof of concept",
        "transactional outbox/inbox persistence",
        "same-SHA CI and security approval",
    ] {
        if !qualification.contains(required) {
            violations.push(format!(
                "{} is missing qualification gate {required}",
                relative_path(&root, &qualification_path)
            ));
        }
    }
    for required in [
        "organizational release approval pending",
        "Runtime-tools deliberately implements no identity or role policy",
        "exact commit",
        "Security owner approval",
    ] {
        if !security.contains(required) {
            violations.push(format!(
                "{} is missing residual security requirement {required}",
                relative_path(&root, &security_path)
            ));
        }
    }
    for required in [
        "cargo check --workspace --all-targets --locked",
        "cargo clippy --workspace --all-targets --all-features --locked",
        "cargo test --workspace --all-targets --all-features --locked",
        "cargo doc --workspace --no-deps --all-features --locked",
    ] {
        if !workflow.contains(required) {
            violations.push(format!(
                "{} is missing locked qualification command {required}",
                relative_path(&root, &workflow_path)
            ));
        }
    }

    finish_violations(violations)
}

#[test]
fn ci_enforces_coverage_and_runs_every_fuzz_target() -> Result<(), Box<dyn Error>> {
    let root = workspace_root()?;
    let workflow_path = root.join(".github/workflows/ci.yml");
    let workflow = fs::read_to_string(&workflow_path)?;
    let fuzz_manifest_path = root.join("fuzz/Cargo.toml");
    let fuzz_manifest = fs::read_to_string(&fuzz_manifest_path)?;
    let mut violations = Vec::new();

    for required in [
        "cargo-llvm-cov --version 0.8.7",
        "cargo llvm-cov report --summary-only --fail-under-lines 90",
        "cargo-fuzz --version 0.13.2",
        "nightly-2026-08-01",
        "-runs=2000",
    ] {
        if !workflow.contains(required) {
            violations.push(format!(
                "{} is missing coverage/fuzz gate {required}",
                relative_path(&root, &workflow_path)
            ));
        }
    }
    for target in [
        "message_metadata",
        "capability_codec",
        "retry_policy",
        "nats_config",
        "propagation_carrier",
        "runtime_inputs",
    ] {
        if !workflow.contains(target) {
            violations.push(format!(
                "{} does not run fuzz target {target}",
                relative_path(&root, &workflow_path)
            ));
        }
        if !fuzz_manifest.contains(&format!("name = \"{target}\"")) {
            violations.push(format!(
                "{} does not declare fuzz target {target}",
                relative_path(&root, &fuzz_manifest_path)
            ));
        }
    }

    finish_violations(violations)
}

#[test]
fn all_production_crates_forbid_unsafe_and_sources_use_no_unsafe_blocks()
-> Result<(), Box<dyn Error>> {
    let root = workspace_root()?;
    let mut violations = Vec::new();

    for package in production_packages(&root)? {
        let library = package.join("src/lib.rs");
        let contents = fs::read_to_string(&library)?;
        let compact: String = sanitize_rust(&contents, LiteralPolicy::Mask)
            .chars()
            .filter(|character| !character.is_whitespace())
            .collect();
        if !compact.contains("#![forbid(unsafe_code)]") {
            violations.push(format!(
                "{} does not forbid unsafe code at the crate root",
                relative_path(&root, &library)
            ));
        }
    }

    for file in production_rust_files(&root)? {
        let contents = fs::read_to_string(&file)?;
        let code = sanitize_production_rust(&contents, LiteralPolicy::Mask);
        if identifiers(&code).contains(&"unsafe") {
            violations.push(format!(
                "{} contains an unsafe production construct",
                relative_path(&root, &file)
            ));
        }
    }

    finish_violations(violations)
}

#[test]
fn production_sources_use_no_known_unbounded_or_zero_capacity_channels()
-> Result<(), Box<dyn Error>> {
    let root = workspace_root()?;
    let mut violations = Vec::new();

    for file in production_rust_files(&root)? {
        let contents = fs::read_to_string(&file)?;
        let code = sanitize_production_rust(&contents, LiteralPolicy::Mask);
        let compact: String = code
            .chars()
            .filter(|character| !character.is_whitespace())
            .collect();
        for forbidden in [
            "unbounded_channel(",
            "::unbounded(",
            ".unbounded_send(",
            "std::sync::mpsc::channel(",
            "Semaphore::new(0)",
            "broadcast::channel(0)",
            "mpsc::channel(0)",
            "sync_channel(0)",
        ] {
            if compact.contains(forbidden) {
                violations.push(format!(
                    "{} contains forbidden queue/channel construct {forbidden}",
                    relative_path(&root, &file)
                ));
            }
        }
    }

    finish_violations(violations)
}

#[test]
fn runtime_capacity_concurrency_payload_and_security_defaults_are_explicit()
-> Result<(), Box<dyn Error>> {
    let root = workspace_root()?;
    let mut violations = Vec::new();
    require_compact_code(
        &root,
        "adapters/plenora-runtime-nats/src/config.rs",
        &[
            "#[default]Required",
            "mode:TlsMode::Required",
            r#"Arc::from("tls://127.0.0.1:4222")"#,
            "#[default]BindExisting",
            r#"nonzero("client_capacity",self.client_capacity)?"#,
            r#"nonzero("subscription_capacity",self.subscription_capacity)?"#,
            r#"nonzero("max_payload_bytes",self.max_payload_bytes)?"#,
        ],
        &mut violations,
    )?;
    require_compact_code(
        &root,
        "adapters/plenora-runtime-nats/src/connection.rs",
        &[
            ".require_tls(config.tls.mode==TlsMode::Required)",
            "pubasyncfnreplay_consumer(",
            "request:ReplayRequest",
        ],
        &mut violations,
    )?;
    require_compact_code(
        &root,
        "crates/plenora-runtime-http/src/config.rs",
        &[
            "DEFAULT_MAX_REQUEST_BODY_BYTES",
            "ifself.max_request_body_bytes==0",
        ],
        &mut violations,
    )?;
    require_compact_code(
        &root,
        "crates/plenora-runtime-worker/src/config.rs",
        &["ifmax_in_flight==0"],
        &mut violations,
    )?;
    require_compact_code(
        &root,
        "crates/plenora-runtime-worker/src/executor.rs",
        &["Semaphore::new(config.concurrency.max_in_flight)"],
        &mut violations,
    )?;
    require_compact_code(
        &root,
        "crates/plenora-runtime-capabilities/src/registry.rs",
        &[
            "ifmax_capabilities==0",
            "ifself.registrations.len()>=self.config.max_capabilities",
        ],
        &mut violations,
    )?;
    require_compact_code(
        &root,
        "crates/plenora-runtime-capabilities/src/dispatcher.rs",
        &[
            "ifmax_payload_bytes==0",
            "ifrequest.input().len()>self.config.max_payload_bytes",
        ],
        &mut violations,
    )?;
    finish_violations(violations)
}

#[test]
fn runtime_and_testkit_memory_bounds_are_enforced() -> Result<(), Box<dyn Error>> {
    let root = workspace_root()?;
    let mut violations = Vec::new();
    require_compact_code(
        &root,
        "crates/plenora-runtime-core/src/runtime.rs",
        &[
            "pubmax_concurrent_tasks:usize",
            "pubtask_report_capacity:usize",
            "iflifecycle.active_tasks>=self.config.max_concurrent_tasks",
            "SpawnError::TaskCapacityExceeded{limit:self.config.max_concurrent_tasks",
            "ifself.config.task_report_capacity>0",
            "ifreports.len()==self.config.task_report_capacity",
            "reports.pop_front()",
            "fnabort_active_tasks(&self)",
            "handle.abort()",
            "self.inner.abort_active_tasks()",
        ],
        &mut violations,
    )?;
    require_compact_code(
        &root,
        "crates/plenora-runtime-testkit/src/broker.rs",
        &[
            "pubstructFakeBrokerLimits",
            "pubmax_pending_deliveries:usize",
            "pubmax_catalog_entries:usize",
            "pubmax_published_history:usize",
            "pubmax_acknowledgement_history:usize",
            "pubmax_terminal_history:usize",
            "pubmax_scripted_faults:usize",
            "pubmax_message_bytes:usize",
            "letcapacity=state.limits.max_scripted_faults",
            "iftarget_len>=capacity",
            "ifmessage.len()>state.limits.max_message_bytes",
            "state.queue.len().saturating_add(state.in_flight)>=state.limits.max_pending_deliveries",
            "ifstate.catalog.len()>=state.limits.max_catalog_entries",
            "state.limits.max_published_history",
            "state.limits.max_acknowledgement_history",
            "state.limits.max_terminal_history",
            "fnpush_bounded<T>(entries:&mutVecDeque<T>,entry:T,capacity:usize)",
        ],
        &mut violations,
    )?;
    require_compact_code(
        &root,
        "crates/plenora-runtime-testkit/src/capability.rs",
        &[
            "pubinvocation_capacity:usize",
            "puboutcome_capacity:usize",
            "ifstate.invocations.len()>=self.inner.config.invocation_capacity",
            "ifstate.outcomes.len()>=self.inner.config.outcome_capacity",
        ],
        &mut violations,
    )?;
    require_compact_code(
        &root,
        "crates/plenora-runtime-testkit/src/fault.rs",
        &[
            "pubconstDEFAULT_FAULT_SEQUENCE_CAPACITY:usize",
            "pubstructFaultSequence<T>",
            "capacity:usize",
            "pubfnwith_capacity(capacity:usize)->Self",
            "ifentries.len()>=self.capacity",
            "FaultSequenceCapacityError{capacity:self.capacity,",
        ],
        &mut violations,
    )?;
    finish_violations(violations)
}

#[test]
fn message_metadata_entry_key_value_and_total_bounds_are_enforced() -> Result<(), Box<dyn Error>> {
    let root = workspace_root()?;
    let mut violations = Vec::new();
    require_compact_code(
        &root,
        "crates/plenora-runtime-messaging/src/metadata.rs",
        &[
            "pubconstMAX_METADATA_ENTRIES:usize",
            "pubconstMAX_METADATA_KEY_BYTES:usize",
            "pubconstMAX_METADATA_VALUE_BYTES:usize",
            "pubconstMAX_METADATA_TOTAL_BYTES:usize",
            "self.entries.len()>=MAX_METADATA_ENTRIES",
            "ifresulting_bytes>MAX_METADATA_TOTAL_BYTES",
            "ifkey.len()>MAX_METADATA_KEY_BYTES",
            "ifvalue.len()>MAX_METADATA_VALUE_BYTES",
            "MetadataKeyErrorKind::EntryCapacityExceeded",
            "MetadataKeyErrorKind::TotalBytesExceeded",
        ],
        &mut violations,
    )?;
    require_compact_code(
        &root,
        "crates/plenora-runtime-messaging/src/lib.rs",
        &[
            "MAX_METADATA_ENTRIES",
            "MAX_METADATA_KEY_BYTES",
            "MAX_METADATA_VALUE_BYTES",
            "MAX_METADATA_TOTAL_BYTES",
        ],
        &mut violations,
    )?;
    finish_violations(violations)
}

#[test]
fn health_endpoints_expose_only_redacted_aggregate_state() -> Result<(), Box<dyn Error>> {
    let root = workspace_root()?;
    let path = root.join("crates/plenora-runtime-http/src/health.rs");
    let contents = fs::read_to_string(&path)?;
    let source = sanitize_rust(&contents, LiteralPolicy::Preserve);
    let source_identifiers = identifiers(&source);
    let mut violations = Vec::new();

    for forbidden in [
        "bytes",
        "components",
        "config",
        "credential",
        "message",
        "metadata",
        "payload",
        "secret",
        "servers",
    ] {
        if source_identifiers
            .iter()
            .any(|identifier| identifier.to_ascii_lowercase().contains(forbidden))
        {
            violations.push(format!(
                "{} exposes forbidden health detail {forbidden}",
                relative_path(&root, &path)
            ));
        }
    }
    let compact: String = source
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect();
    for required in ["Json(StatusBody{status})", r#"(CACHE_CONTROL,"no-store")"#] {
        if !compact.contains(required) {
            violations.push(format!(
                "{} is missing redacted health response invariant {required}",
                relative_path(&root, &path)
            ));
        }
    }

    finish_violations(violations)
}

#[test]
fn credential_metadata_and_payload_debug_views_are_redacted() -> Result<(), Box<dyn Error>> {
    let root = workspace_root()?;
    let mut violations = Vec::new();
    let nats_path = root.join("adapters/plenora-runtime-nats/src/config.rs");
    let nats_source = sanitize_rust(&fs::read_to_string(&nats_path)?, LiteralPolicy::Preserve);
    for item in ["SecretString", "NatsCredentials", "ClientCertificate"] {
        match debug_impl_block(&nats_source, item) {
            Some(debug_block) if debug_block.contains("[REDACTED]") => {}
            _ => violations.push(format!(
                "{} does not provide a redacted Debug implementation for {item}",
                relative_path(&root, &nats_path)
            )),
        }
    }
    if debug_impl_block(&nats_source, "NatsConfig")
        .is_some_and(|debug_block| debug_block.contains(r#".field("servers""#))
    {
        violations.push(format!(
            "{} exposes configured server URLs through Debug",
            relative_path(&root, &nats_path)
        ));
    }

    let metadata_path = root.join("crates/plenora-runtime-messaging/src/metadata.rs");
    let metadata_source = sanitize_rust(
        &fs::read_to_string(&metadata_path)?,
        LiteralPolicy::Preserve,
    );
    match debug_impl_block(&metadata_source, "MessageMetadata") {
        Some(debug_block)
            if debug_block.contains(r#".field("keys""#)
                && !debug_block.contains(r#".field("entries""#) => {}
        _ => violations.push(format!(
            "{} must expose metadata keys but not values through Debug",
            relative_path(&root, &metadata_path)
        )),
    }

    let message_path = root.join("crates/plenora-runtime-messaging/src/message.rs");
    let message_source =
        sanitize_rust(&fs::read_to_string(&message_path)?, LiteralPolicy::Preserve);
    match debug_impl_block(&message_source, "MessageEnvelope<T>") {
        Some(debug_block) if debug_block.contains("<redacted>") => {}
        _ => violations.push(format!(
            "{} must redact envelope payloads through Debug",
            relative_path(&root, &message_path)
        )),
    }

    finish_violations(violations)
}

#[test]
fn cancellation_and_fault_injection_hooks_remain_present() -> Result<(), Box<dyn Error>> {
    let root = workspace_root()?;
    let mut violations = Vec::new();
    require_code_identifiers(
        &root,
        "crates/plenora-runtime-core/src/shutdown.rs",
        &["ShutdownSignal", "cancelled"],
        &mut violations,
    )?;
    require_code_identifiers(
        &root,
        "crates/plenora-runtime-core/src/runtime.rs",
        &["shutdown_grace_period", "Draining"],
        &mut violations,
    )?;
    require_code_identifiers(
        &root,
        "crates/plenora-runtime-testkit/src/fault.rs",
        &["FaultSequence"],
        &mut violations,
    )?;
    require_code_identifiers(
        &root,
        "crates/plenora-runtime-testkit/src/broker.rs",
        &[
            "PublishFault",
            "fail_next_ack",
            "fail_next_nack",
            "fail_next_receive",
            "inject_duplicate",
        ],
        &mut violations,
    )?;
    finish_violations(violations)
}

#[test]
fn production_sources_have_no_abortive_shortcuts() -> Result<(), Box<dyn Error>> {
    let root = workspace_root()?;
    let mut violations = Vec::new();

    for group in ["crates", "adapters"] {
        let group_path = root.join(group);
        let mut packages = read_directories(&group_path)?;
        packages.sort();

        for package in packages {
            let source = package.join("src");
            if !source.is_dir() {
                continue;
            }

            let mut files = Vec::new();
            collect_rust_files(&source, &mut files)?;
            files.sort();
            for file in files {
                let contents = fs::read_to_string(&file)?;
                let code = sanitize_production_rust(&contents, LiteralPolicy::Mask);
                let compact: String = code
                    .chars()
                    .filter(|character| !character.is_whitespace())
                    .collect();

                for forbidden in [
                    ".unwrap(",
                    ".expect(",
                    "panic!(",
                    "assert!(",
                    "assert_eq!(",
                    "assert_ne!(",
                    "unreachable!(",
                    "todo!(",
                    "unimplemented!(",
                    "dbg!(",
                ] {
                    if compact.contains(forbidden) {
                        violations.push(format!(
                            "{} contains forbidden production token {forbidden}",
                            relative_path(&root, &file)
                        ));
                    }
                }
            }
        }
    }

    violations.sort();
    if violations.is_empty() {
        Ok(())
    } else {
        Err(ArchitectureViolation(violations.join("\n")).into())
    }
}

#[test]
fn production_sources_do_not_expose_secret_values() -> Result<(), Box<dyn Error>> {
    let root = workspace_root()?;
    let mut violations = Vec::new();

    for file in production_rust_files(&root)? {
        let contents = fs::read_to_string(&file)?;
        let code = sanitize_production_rust(&contents, LiteralPolicy::Mask);

        for invocation in macro_invocations(&code, "dbg") {
            violations.push(format!(
                "{} contains dbg! in production at byte {}",
                relative_path(&root, &file),
                invocation.start
            ));
        }

        for macro_name in [
            "debug",
            "eprintln",
            "error",
            "event",
            "format",
            "format_args",
            "info",
            "println",
            "trace",
            "warn",
            "write",
            "writeln",
        ] {
            for invocation in macro_invocations(&code, macro_name) {
                let Some(masked_invocation) = code.get(invocation.clone()) else {
                    continue;
                };
                let Some(original_invocation) = contents.get(invocation.clone()) else {
                    continue;
                };
                if identifiers(masked_invocation)
                    .iter()
                    .any(|identifier| is_secret_value_identifier(identifier))
                    || contains_secret_format_capture(original_invocation)
                {
                    violations.push(format!(
                        "{} may expose a secret through {macro_name}! at byte {}",
                        relative_path(&root, &file),
                        invocation.start
                    ));
                }
            }
        }

        for item in debug_derived_secret_items(&code) {
            violations.push(format!(
                "{} derives Debug for secret-bearing item {item}",
                relative_path(&root, &file)
            ));
        }
    }

    finish_violations(violations)
}

#[test]
fn lexical_sanitizer_ignores_documentation_comments_and_literals() {
    let sample = r##"
        /// panic! and opentelemetry_sdk are documentation only.
        fn safe() {
            let text = r#"panic! and .unwrap are literal text"#;
            /* nested /* panic!() */ comment */
            operation()?;
        }

        #[cfg(test)]
        mod tests {
            fn assertions_are_allowed_in_tests() {
                assert!(true);
            }
        }
    "##;
    let masked = sanitize_rust(sample, LiteralPolicy::Mask);
    assert!(!masked.contains("panic!"));
    assert!(!masked.contains("opentelemetry_sdk"));
    assert!(!masked.contains(".unwrap"));
    assert!(masked.contains("operation()?"));
    let production = sanitize_production_rust(sample, LiteralPolicy::Mask);
    assert!(!production.contains("assert!(true)"));
    assert!(production.contains("operation()?"));
}

fn assert_manifest_terms_absent(
    root: &Path,
    package: &str,
    forbidden_terms: &[&str],
) -> Result<(), Box<dyn Error>> {
    let manifest_path = root.join(package).join("Cargo.toml");
    let manifest = strip_toml_comments(&fs::read_to_string(&manifest_path)?).to_ascii_lowercase();
    let mut violations = Vec::new();
    for forbidden in forbidden_terms {
        if contains_bounded_term(&manifest, forbidden) {
            violations.push(format!(
                "{} contains forbidden dependency term {forbidden}",
                relative_path(root, &manifest_path)
            ));
        }
    }
    finish_violations(violations)
}

fn assert_source_identifiers_absent(
    root: &Path,
    package: &str,
    forbidden_terms: &[&str],
    literal_policy: LiteralPolicy,
) -> Result<(), Box<dyn Error>> {
    assert_source_policy(root, package, forbidden_terms, literal_policy, false)
}

fn assert_source_identifier_fragments_absent(
    root: &Path,
    package: &str,
    forbidden_terms: &[&str],
    literal_policy: LiteralPolicy,
) -> Result<(), Box<dyn Error>> {
    assert_source_policy(root, package, forbidden_terms, literal_policy, true)
}

fn assert_source_policy(
    root: &Path,
    package: &str,
    forbidden_terms: &[&str],
    literal_policy: LiteralPolicy,
    match_fragments: bool,
) -> Result<(), Box<dyn Error>> {
    let mut files = Vec::new();
    collect_rust_files(&root.join(package).join("src"), &mut files)?;
    files.sort();
    let mut violations = Vec::new();

    for file in files {
        let contents = fs::read_to_string(&file)?;
        let source = sanitize_production_rust(&contents, literal_policy);
        let source_identifiers = identifiers(&source);
        for forbidden in forbidden_terms {
            let found = source_identifiers.iter().any(|identifier| {
                if *identifier == "AUTHORIZATION" {
                    false
                } else {
                    let normalized = identifier.to_ascii_lowercase();
                    if match_fragments {
                        normalized.contains(forbidden)
                    } else {
                        normalized == *forbidden
                    }
                }
            });
            if found {
                violations.push(format!(
                    "{} contains forbidden architecture identifier {forbidden}",
                    relative_path(root, &file)
                ));
            }
        }
    }

    finish_violations(violations)
}

fn require_compact_code(
    root: &Path,
    relative: &str,
    required_terms: &[&str],
    violations: &mut Vec<String>,
) -> Result<(), io::Error> {
    let path = root.join(relative);
    let contents = fs::read_to_string(&path)?;
    let compact: String = sanitize_rust(&contents, LiteralPolicy::Preserve)
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect();
    for required in required_terms {
        if !compact.contains(required) {
            violations.push(format!(
                "{} is missing required static invariant {required}",
                relative_path(root, &path)
            ));
        }
    }
    Ok(())
}

fn require_code_identifiers(
    root: &Path,
    relative: &str,
    required_identifiers: &[&str],
    violations: &mut Vec<String>,
) -> Result<(), io::Error> {
    let path = root.join(relative);
    let contents = fs::read_to_string(&path)?;
    let source = sanitize_rust(&contents, LiteralPolicy::Mask);
    let source_identifiers = identifiers(&source);
    for required in required_identifiers {
        if !source_identifiers.contains(required) {
            violations.push(format!(
                "{} is missing required cross-cutting hook {required}",
                relative_path(root, &path)
            ));
        }
    }
    Ok(())
}

fn debug_impl_block<'a>(source: &'a str, item: &str) -> Option<&'a str> {
    let signature = format!("impl Debug for {item}");
    let generic_signature = format!("impl<T> Debug for {item}");
    let (start, signature_len) = source
        .find(&signature)
        .map(|start| (start, signature.len()))
        .or_else(|| {
            source
                .find(&generic_signature)
                .map(|start| (start, generic_signature.len()))
        })?;
    let opening_offset = source.get(start + signature_len..)?.find('{')?;
    let opening = start + signature_len + opening_offset;
    let end = matching_delimiter_end(source.as_bytes(), opening)?;
    source.get(start..end)
}

fn assert_terms_absent(
    root: &Path,
    package: &str,
    forbidden_terms: &[&str],
) -> Result<(), Box<dyn Error>> {
    let package_path = root.join(package);
    let manifest_path = package_path.join("Cargo.toml");
    let manifest = strip_toml_comments(&fs::read_to_string(&manifest_path)?).to_ascii_lowercase();
    let mut violations = Vec::new();
    for forbidden in forbidden_terms {
        if contains_bounded_term(&manifest, forbidden) {
            violations.push(format!(
                "{} contains forbidden architecture term {forbidden}",
                relative_path(root, &manifest_path)
            ));
        }
    }

    let mut files = Vec::new();
    collect_rust_files(&package_path.join("src"), &mut files)?;
    files.sort();

    for file in files {
        let contents = fs::read_to_string(&file)?;
        let code = sanitize_production_rust(&contents, LiteralPolicy::Mask).to_ascii_lowercase();
        let source_identifiers = identifiers(&code);
        for forbidden in forbidden_terms {
            let source_term = forbidden.replace('-', "_");
            if source_identifiers
                .iter()
                .any(|identifier| *identifier == source_term)
            {
                violations.push(format!(
                    "{} contains forbidden architecture term {forbidden}",
                    relative_path(root, &file)
                ));
            }
        }
    }

    finish_violations(violations)
}

fn workspace_manifests(root: &Path) -> Result<Vec<PathBuf>, io::Error> {
    let mut manifests = vec![root.join("Cargo.toml")];
    for group in ["adapters", "crates", "examples", "tests"] {
        for package in read_directories(&root.join(group))? {
            let manifest = package.join("Cargo.toml");
            if manifest.is_file() {
                manifests.push(manifest);
            }
        }
    }
    manifests.sort();
    Ok(manifests)
}

fn toml_section_name(line: &str) -> Option<&str> {
    line.strip_prefix('[')?.strip_suffix(']')
}

fn toml_section_assignment<'a>(input: &'a str, section: &str, key: &str) -> Option<&'a str> {
    let mut current_section = None;
    for line in input.lines() {
        let trimmed = line.trim();
        if let Some(candidate) = toml_section_name(trimmed) {
            current_section = Some(candidate);
            continue;
        }
        if current_section == Some(section)
            && let Some(value) = quoted_assignment_value(trimmed, key)
        {
            return Some(value);
        }
    }
    None
}

fn workflow_job_blocks(input: &str) -> Vec<(String, String)> {
    let mut jobs = Vec::new();
    let mut in_jobs = false;
    let mut current_name: Option<String> = None;
    let mut current_body = String::new();

    for line in input.lines() {
        let indentation = line.bytes().take_while(|byte| *byte == b' ').count();
        let trimmed = line.trim();
        if !in_jobs {
            if indentation == 0 && trimmed == "jobs:" {
                in_jobs = true;
            }
            continue;
        }
        if indentation == 0 && !trimmed.is_empty() {
            break;
        }
        if indentation == 2
            && let Some(name) = trimmed.strip_suffix(':')
            && !name.is_empty()
        {
            if let Some(previous_name) = current_name.replace(name.to_owned()) {
                jobs.push((previous_name, std::mem::take(&mut current_body)));
            }
            continue;
        }
        if current_name.is_some() {
            current_body.push_str(line);
            current_body.push('\n');
        }
    }
    if let Some(name) = current_name {
        jobs.push((name, current_body));
    }
    jobs
}

fn quoted_assignment_value<'a>(line: &'a str, key: &str) -> Option<&'a str> {
    let key_start = line.find(key)?;
    let before = key_start
        .checked_sub(1)
        .and_then(|index| line.as_bytes().get(index));
    let after = line.as_bytes().get(key_start + key.len());
    if before.is_some_and(|byte| is_manifest_name_byte(*byte))
        || after.is_some_and(|byte| is_manifest_name_byte(*byte))
    {
        return None;
    }
    let assignment = line.get(key_start + key.len()..)?;
    let value = assignment.split_once('=')?.1.trim();
    let quoted = value.strip_prefix('"')?;
    quoted.split_once('"').map(|(value, _)| value)
}

fn bare_dependency_version(line: &str) -> Option<&str> {
    let (_, value) = line.split_once('=')?;
    let quoted = value.trim().strip_prefix('"')?;
    quoted.split_once('"').map(|(version, _)| version)
}

fn production_packages(root: &Path) -> Result<Vec<PathBuf>, io::Error> {
    let mut packages = Vec::new();
    for group in ["crates", "adapters"] {
        packages.extend(read_directories(&root.join(group))?);
    }
    packages.sort();
    Ok(packages)
}

fn production_rust_files(root: &Path) -> Result<Vec<PathBuf>, io::Error> {
    let mut files = Vec::new();
    for package in production_packages(root)? {
        collect_rust_files(&package.join("src"), &mut files)?;
    }
    files.sort();
    Ok(files)
}

fn finish_violations(mut violations: Vec<String>) -> Result<(), Box<dyn Error>> {
    violations.sort();
    violations.dedup();
    if violations.is_empty() {
        Ok(())
    } else {
        Err(ArchitectureViolation(violations.join("\n")).into())
    }
}

fn strip_toml_comments(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    for line in input.split_inclusive('\n') {
        let mut in_basic_string = false;
        let mut in_literal_string = false;
        let mut escaped = false;
        for character in line.chars() {
            if character == '#' && !in_basic_string && !in_literal_string {
                break;
            }
            output.push(character);
            if in_basic_string {
                if escaped {
                    escaped = false;
                } else if character == '\\' {
                    escaped = true;
                } else if character == '"' {
                    in_basic_string = false;
                }
            } else if in_literal_string {
                if character == '\'' {
                    in_literal_string = false;
                }
            } else if character == '"' {
                in_basic_string = true;
            } else if character == '\'' {
                in_literal_string = true;
            }
        }
        if line.ends_with('\n') && !output.ends_with('\n') {
            output.push('\n');
        }
    }
    output
}

fn contains_bounded_term(haystack: &str, needle: &str) -> bool {
    haystack.match_indices(needle).any(|(start, _)| {
        let before = start
            .checked_sub(1)
            .and_then(|index| haystack.as_bytes().get(index));
        let after = haystack.as_bytes().get(start + needle.len());
        before.is_none_or(|byte| !is_manifest_name_byte(*byte))
            && after.is_none_or(|byte| !is_manifest_name_byte(*byte))
    })
}

const fn is_manifest_name_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-')
}

fn identifiers(source: &str) -> Vec<&str> {
    let bytes = source.as_bytes();
    let mut output = Vec::new();
    let mut cursor = 0;
    while cursor < bytes.len() {
        if bytes[cursor].is_ascii_alphabetic() || bytes[cursor] == b'_' {
            let start = cursor;
            cursor += 1;
            while cursor < bytes.len()
                && (bytes[cursor].is_ascii_alphanumeric() || bytes[cursor] == b'_')
            {
                cursor += 1;
            }
            if let Some(identifier) = source.get(start..cursor) {
                output.push(identifier);
            }
        } else {
            cursor += 1;
        }
    }
    output
}

fn sanitize_production_rust(input: &str, literal_policy: LiteralPolicy) -> String {
    let sanitized = sanitize_rust(input, literal_policy);
    let bytes = sanitized.as_bytes();
    let mut output = bytes.to_vec();
    let mut cursor = 0;

    while let Some(offset) = sanitized
        .get(cursor..)
        .and_then(|remaining| remaining.find("#[cfg(test)]"))
    {
        let start = cursor + offset;
        let item_start = start + "#[cfg(test)]".len();
        let brace = bytes
            .get(item_start..)
            .and_then(|remaining| remaining.iter().position(|byte| *byte == b'{'))
            .map(|offset| item_start + offset);
        let semicolon = bytes
            .get(item_start..)
            .and_then(|remaining| remaining.iter().position(|byte| *byte == b';'))
            .map(|offset| item_start + offset + 1);
        let end = match (brace, semicolon) {
            (Some(opening), Some(statement_end)) if statement_end <= opening => statement_end,
            (Some(opening), _) => matching_delimiter_end(bytes, opening).unwrap_or(bytes.len()),
            (None, Some(statement_end)) => statement_end,
            (None, None) => bytes.len(),
        };
        mask_range(&mut output, start..end);
        cursor = end;
    }

    String::from_utf8(output).unwrap_or(sanitized)
}

fn sanitize_rust(input: &str, literal_policy: LiteralPolicy) -> String {
    let bytes = input.as_bytes();
    let mut output = bytes.to_vec();
    let mut cursor = 0;

    while cursor < bytes.len() {
        if bytes.get(cursor..cursor + 2) == Some(b"//") {
            let end = bytes[cursor..]
                .iter()
                .position(|byte| *byte == b'\n')
                .map_or(bytes.len(), |offset| cursor + offset);
            mask_range(&mut output, cursor..end);
            cursor = end;
        } else if bytes.get(cursor..cursor + 2) == Some(b"/*") {
            let end = block_comment_end(bytes, cursor);
            mask_range(&mut output, cursor..end);
            cursor = end;
        } else if let Some((content_start, hashes)) = raw_string_start(bytes, cursor) {
            let end = raw_string_end(bytes, content_start, hashes);
            if matches!(literal_policy, LiteralPolicy::Mask) {
                mask_range(&mut output, cursor..end);
            }
            cursor = end;
        } else if bytes[cursor] == b'"' {
            let end = quoted_literal_end(bytes, cursor, b'"');
            if matches!(literal_policy, LiteralPolicy::Mask) {
                mask_range(&mut output, cursor..end);
            }
            cursor = end;
        } else if bytes[cursor] == b'\'' {
            if let Some(end) = char_literal_end(input, cursor) {
                if matches!(literal_policy, LiteralPolicy::Mask) {
                    mask_range(&mut output, cursor..end);
                }
                cursor = end;
            } else {
                cursor += 1;
            }
        } else {
            cursor += 1;
        }
    }

    String::from_utf8_lossy(&output).into_owned()
}

fn block_comment_end(bytes: &[u8], start: usize) -> usize {
    let mut depth = 1_u32;
    let mut cursor = start + 2;
    while cursor < bytes.len() && depth > 0 {
        if bytes.get(cursor..cursor + 2) == Some(b"/*") {
            depth = depth.saturating_add(1);
            cursor += 2;
        } else if bytes.get(cursor..cursor + 2) == Some(b"*/") {
            depth = depth.saturating_sub(1);
            cursor += 2;
        } else {
            cursor += 1;
        }
    }
    cursor
}

fn raw_string_start(bytes: &[u8], start: usize) -> Option<(usize, usize)> {
    if bytes.get(start) != Some(&b'r') {
        return None;
    }
    let mut cursor = start + 1;
    while bytes.get(cursor) == Some(&b'#') {
        cursor += 1;
    }
    (bytes.get(cursor) == Some(&b'"')).then_some((cursor + 1, cursor - start - 1))
}

fn raw_string_end(bytes: &[u8], content_start: usize, hashes: usize) -> usize {
    let mut cursor = content_start;
    while cursor < bytes.len() {
        if bytes[cursor] == b'"'
            && (0..hashes).all(|offset| bytes.get(cursor + 1 + offset) == Some(&b'#'))
        {
            return cursor + 1 + hashes;
        }
        cursor += 1;
    }
    bytes.len()
}

fn quoted_literal_end(bytes: &[u8], start: usize, delimiter: u8) -> usize {
    let mut escaped = false;
    let mut cursor = start + 1;
    while cursor < bytes.len() {
        let byte = bytes[cursor];
        cursor += 1;
        if escaped {
            escaped = false;
        } else if byte == b'\\' {
            escaped = true;
        } else if byte == delimiter {
            break;
        }
    }
    cursor
}

fn char_literal_end(input: &str, start: usize) -> Option<usize> {
    let bytes = input.as_bytes();
    let first = *bytes.get(start + 1)?;
    if first == b'\\' {
        let end = quoted_literal_end(bytes, start, b'\'');
        return (end <= bytes.len() && bytes.get(end.saturating_sub(1)) == Some(&b'\''))
            .then_some(end);
    }
    let character = input.get(start + 1..)?.chars().next()?;
    let closing = start + 1 + character.len_utf8();
    (bytes.get(closing) == Some(&b'\'')).then_some(closing + 1)
}

fn mask_range(output: &mut [u8], range: Range<usize>) {
    if let Some(bytes) = output.get_mut(range) {
        for byte in bytes {
            if !matches!(*byte, b'\n' | b'\r') {
                *byte = b' ';
            }
        }
    }
}

fn macro_invocations(source: &str, macro_name: &str) -> Vec<Range<usize>> {
    let pattern = format!("{macro_name}!");
    let bytes = source.as_bytes();
    let mut invocations = Vec::new();
    let mut cursor = 0;

    while let Some(offset) = source.get(cursor..).and_then(|tail| tail.find(&pattern)) {
        let start = cursor + offset;
        let before = start.checked_sub(1).and_then(|index| bytes.get(index));
        let mut opening = start + pattern.len();
        while bytes.get(opening).is_some_and(u8::is_ascii_whitespace) {
            opening += 1;
        }
        if before.is_none_or(|byte| !is_rust_identifier_byte(*byte))
            && bytes
                .get(opening)
                .is_some_and(|byte| matches!(byte, b'(' | b'[' | b'{'))
            && let Some(end) = matching_delimiter_end(bytes, opening)
        {
            invocations.push(start..end);
            cursor = end;
        } else {
            cursor = start + pattern.len();
        }
    }
    invocations
}

const fn is_rust_identifier_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

fn matching_delimiter_end(bytes: &[u8], opening: usize) -> Option<usize> {
    let mut expected = Vec::new();
    let mut cursor = opening;
    while let Some(byte) = bytes.get(cursor) {
        match byte {
            b'(' => expected.push(b')'),
            b'[' => expected.push(b']'),
            b'{' => expected.push(b'}'),
            b')' | b']' | b'}' => {
                if expected.last() != Some(byte) {
                    return None;
                }
                let _popped = expected.pop();
                if expected.is_empty() {
                    return Some(cursor + 1);
                }
            }
            _ => {}
        }
        cursor += 1;
    }
    None
}

fn is_secret_value_identifier(identifier: &str) -> bool {
    let identifier = identifier.to_ascii_lowercase();
    identifier == "token"
        || [
            "access_token",
            "apikey",
            "api_key",
            "authorization",
            "bearer_token",
            "credential",
            "nkey_seed",
            "passwd",
            "password",
            "payload",
            "private_key",
            "refresh_token",
            "secret",
        ]
        .iter()
        .any(|fragment| identifier.contains(fragment))
}

fn contains_secret_format_capture(invocation: &str) -> bool {
    let lower = invocation.to_ascii_lowercase();
    [
        "access_token",
        "apikey",
        "api_key",
        "authorization",
        "bearer_token",
        "credential",
        "nkey_seed",
        "passwd",
        "password",
        "payload",
        "private_key",
        "refresh_token",
        "secret",
        "token",
    ]
    .iter()
    .any(|name| lower.contains(&format!("{{{name}}}")) || lower.contains(&format!("{{{name}:")))
}

fn debug_derived_secret_items(source: &str) -> Vec<String> {
    let tokens = identifiers(source);
    let mut output = Vec::new();
    for (index, token) in tokens.iter().enumerate() {
        if *token != "derive" {
            continue;
        }
        let mut derives_debug = false;
        for candidate in tokens.iter().skip(index + 1) {
            if *candidate == "Debug" {
                derives_debug = true;
            }
            if matches!(*candidate, "struct" | "enum" | "union") {
                break;
            }
        }
        if !derives_debug {
            continue;
        }
        let Some(item_index) =
            tokens
                .iter()
                .enumerate()
                .skip(index + 1)
                .find_map(|(candidate_index, candidate)| {
                    matches!(*candidate, "struct" | "enum" | "union").then_some(candidate_index + 1)
                })
        else {
            continue;
        };
        let Some(item) = tokens.get(item_index) else {
            continue;
        };
        let normalized = item.to_ascii_lowercase().replace('_', "");
        if [
            "apikey",
            "bearertoken",
            "credential",
            "password",
            "privatekey",
            "refreshtoken",
            "secret",
        ]
        .iter()
        .any(|fragment| normalized.contains(fragment))
        {
            output.push((*item).to_owned());
        }
    }
    output
}

fn workspace_root() -> Result<PathBuf, io::Error> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
}

fn read_directories(path: &Path) -> Result<Vec<PathBuf>, io::Error> {
    let mut directories = Vec::new();
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            directories.push(entry.path());
        }
    }
    Ok(directories)
}

fn collect_rust_files(path: &Path, files: &mut Vec<PathBuf>) -> Result<(), io::Error> {
    if !path.is_dir() {
        return Ok(());
    }

    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let entry_path = entry.path();

        if file_type.is_dir() {
            collect_rust_files(&entry_path, files)?;
        } else if file_type.is_file()
            && entry_path
                .extension()
                .is_some_and(|extension| extension == OsStr::new("rs"))
        {
            files.push(entry_path);
        }
    }
    Ok(())
}

fn relative_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root).map_or_else(
        |_| path.display().to_string(),
        |relative| relative.display().to_string(),
    )
}
