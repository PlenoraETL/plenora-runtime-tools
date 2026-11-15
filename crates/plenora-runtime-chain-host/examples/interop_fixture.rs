//! Generate a complete real IPC -> DATA sort -> IPC profile from an explicit staging root.
#[path = "../tests/support/interop_fixture.rs"]
mod fixture;
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::path::PathBuf::from(
        std::env::args()
            .nth(1)
            .ok_or("absolute staging directory required")?,
    );
    if !root.is_absolute() {
        return Err("absolute staging directory required".into());
    }
    std::fs::create_dir_all(&root)?;
    let (adapter, plan) = fixture::fixture(&root, "ipc", false)?;
    println!(
        "{}",
        serde_json::to_string(&serde_json::json!({"adapter":adapter,"plan":plan}))?
    );
    Ok(())
}
