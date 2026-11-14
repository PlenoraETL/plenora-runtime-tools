//! Export real immutable IO inputs for installed wrapper and PostgreSQL tests.
#[path = "../tests/support/data.rs"]
mod data;
#[path = "../tests/support/fixture.rs"]
mod fixture;
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::env::current_dir()?.join("target/s3-io-fixture");
    std::fs::create_dir_all(&root)?;
    let (adapter, plan) =
        fixture::fixture(&root, data::ipc(&data::batch(Some("91"), 2)?)?, true, "ipc")?;
    println!(
        "{}",
        serde_json::to_string(&serde_json::json!({"adapter":adapter,"plan":plan}))?
    );
    Ok(())
}
