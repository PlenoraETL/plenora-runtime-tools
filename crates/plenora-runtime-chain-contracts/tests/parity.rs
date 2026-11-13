//! Complete pinned-oracle parity, including lexical and semantic rejections.
use plenora_runtime_chain_contracts::*;
use serde::Deserialize;
use std::{error::Error, fs, path::PathBuf};

#[derive(Deserialize)]
struct Case {
    file: String,
    kind: String,
    valid: bool,
    schema: Option<bool>,
    code: Option<String>,
    context: String,
    plan: Option<String>,
}
fn fixtures() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../contracts/chains-v2/fixtures")
}

#[test]
fn all_88_python_oracle_verdicts() -> Result<(), Box<dyn Error>> {
    let root = fixtures();
    let cases: Vec<Case> = serde_json::from_slice(&fs::read(root.join("manifest.json"))?)?;
    assert_eq!(cases.len(), 88);
    for case in cases {
        let raw = fs::read(root.join(&case.file))?;
        if let Some(expected) = case.schema {
            let structural = match case.kind.as_str() {
                "plan" => parse_plan(&raw).is_ok(),
                "result" => parse_result(&raw).is_ok(),
                "event" => parse_event(&raw).is_ok(),
                "error" => parse_error(&raw).is_ok(),
                _ => false,
            };
            assert_eq!(structural, expected, "schema: {}", case.file);
        }
        let registry = parse_registry(&fs::read(root.join(&case.context))?)?;
        let plan = case
            .plan
            .as_ref()
            .map(|p| fs::read(root.join(p)))
            .transpose()?
            .as_deref()
            .map(parse_plan)
            .transpose()?;
        let run = || -> Result<(), Rejection> {
            match case.kind.as_str() {
                "plan" => {
                    validate_plan(&parse_plan(&raw)?, &registry)?;
                    Ok(())
                }
                "result" => validate_result(
                    &parse_result(&raw)?,
                    plan.as_ref().ok_or(Rejection(Code::Reference))?,
                    &registry,
                ),
                "event" => validate_event(&parse_event(&raw)?),
                "error" => validate_error(&parse_error(&raw)?),
                _ => Err(Rejection(Code::Document)),
            }
        };
        let first = run();
        assert_eq!(first, run(), "determinism: {}", case.file);
        assert_eq!(
            first.is_ok(),
            case.valid,
            "verdict: {}: {first:?}",
            case.file
        );
        assert_eq!(
            first.err().map(|e| e.code().to_owned()),
            case.code,
            "code: {}",
            case.file
        );
        if let Err(error) = first {
            assert_eq!(error.phase(), "validate");
            assert_eq!(error.remote_effect(), "none");
            assert_eq!(error.retry(), "never");
            assert!(!format!("{error:?} {error}").contains("SENTINEL"));
        }
    }
    Ok(())
}

#[test]
fn canonical_roundtrip_every_structurally_valid_document() -> Result<(), Box<dyn Error>> {
    let root = fixtures();
    let cases: Vec<Case> = serde_json::from_slice(&fs::read(root.join("manifest.json"))?)?;
    for case in cases {
        let raw = fs::read(root.join(&case.file))?;
        // Structural parsing must preserve every field, including null vs absence.
        let value = match case.kind.as_str() {
            "plan" => parse_plan(&raw).map(serde_json::to_value),
            "result" => parse_result(&raw).map(serde_json::to_value),
            "event" => parse_event(&raw).map(serde_json::to_value),
            "error" => parse_error(&raw).map(serde_json::to_value),
            _ => Err(Rejection(Code::Document)),
        };
        if let Ok(value) = value {
            assert_eq!(
                value?,
                serde_json::from_slice::<serde_json::Value>(&raw)?,
                "roundtrip: {}",
                case.file
            );
        }
    }
    Ok(())
}
