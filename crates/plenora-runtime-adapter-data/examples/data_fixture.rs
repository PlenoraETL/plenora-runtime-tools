//! Generate the real Rust DATA fixture for installed wrappers and PostgreSQL CI.
#[path = "../tests/support/fixture.rs"]
mod fixture;
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (adapter, plan) =
        fixture::fixture(&fixture::native(), fixture::ipc(&fixture::batch()?)?, true)?;
    println!(
        "{}",
        serde_json::to_string(&serde_json::json!({"adapter":adapter,"plan":plan}))?
    );
    Ok(())
}
