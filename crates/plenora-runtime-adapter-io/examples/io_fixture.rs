//! Export real immutable IO inputs for installed wrapper and PostgreSQL tests.
#[path = "../tests/support/data.rs"]
mod data;
#[path = "../tests/support/fixture.rs"]
mod fixture;
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = fixture_root(std::env::args_os().nth(1))?;
    std::fs::create_dir_all(&root)?;
    let (adapter, plan) =
        fixture::fixture(&root, data::ipc(&data::batch(Some("91"), 2)?)?, true, "ipc")?;
    println!(
        "{}",
        serde_json::to_string(&serde_json::json!({"adapter":adapter,"plan":plan}))?
    );
    Ok(())
}

fn fixture_root(argument: Option<std::ffi::OsString>) -> Result<std::path::PathBuf, &'static str> {
    let root =
        std::path::PathBuf::from(argument.ok_or("absolute fixture directory argument required")?);
    if !root.is_absolute() {
        return Err("fixture directory must be absolute");
    }
    root.to_str().ok_or("fixture directory must be UTF-8")?;
    Ok(root)
}

#[cfg(test)]
mod tests {
    use super::fixture_root;

    #[test]
    fn fixture_root_never_falls_back_to_working_directory() -> Result<(), Box<dyn std::error::Error>>
    {
        assert!(fixture_root(None).is_err());
        assert!(fixture_root(Some("relative/target".into())).is_err());
        let directory = tempfile::tempdir()?;
        let absolute = directory.path().join("not-created-yet");
        assert_eq!(
            fixture_root(Some(absolute.clone().into_os_string()))?,
            absolute
        );
        assert!(!absolute.exists());
        Ok(())
    }
}
