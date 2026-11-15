//! Export immutable qualification inputs; no credentials and no provider calls.
#[path = "../tests/support/fixture.rs"]
mod fixture;
use sha2::{Digest, Sha256};
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (config, plan) = fixture::fixture(true, true)?;
    let (missing_config, missing_plan) =
        fixture::fixture_names(true, true, "s3_missing_source", "s3_target")?;
    let (mut denied_config, mut denied_plan) = fixture::fixture(false, true)?;
    let bytes = denied_config
        .artifacts
        .get_mut("ref:write-parameters")
        .ok_or("parameters")?;
    let mut request: serde_json::Value = serde_json::from_slice(bytes)?;
    request["accept_loss"] = serde_json::json!(true);
    *bytes = serde_json::to_vec(&request)?;
    denied_plan.steps[1].parameters.sha256 = format!("{:x}", Sha256::digest(bytes));
    denied_config.authorized_plans = vec![serde_json::to_string(&denied_plan)?];
    println!(
        "{}",
        serde_json::to_string(
            &serde_json::json!({"adapter":config,"plan":plan,"missing":{"adapter":missing_config,"plan":missing_plan},"denied":{"adapter":denied_config,"plan":denied_plan}})
        )?
    );
    Ok(())
}
