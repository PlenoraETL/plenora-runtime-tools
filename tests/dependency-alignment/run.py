"""Compare dependency behavior offline using isolated, exactly pinned probes."""

import os
from pathlib import Path
import subprocess
import tomllib

ROOT = Path(__file__).resolve().parents[2]
OUTPUT = ROOT / "target" / "deps-alignment"


def main():
    """Retain commands, compiler output and observations below target/."""
    OUTPUT.mkdir(parents=True, exist_ok=True)
    cargo_home = Path(os.environ.get("CARGO_HOME", Path.home() / ".cargo"))
    fixtures = list(cargo_home.glob("registry/src/*/tokio-rustls-0.26.4/tests/certs"))
    if len(fixtures) != 1:
        raise RuntimeError("Expected exactly one cached baseline TLS test fixture directory")
    env = dict(os.environ, CARGO_BUILD_JOBS="3", CARGO_NET_OFFLINE="true",
               CARGO_TARGET_DIR=str(OUTPUT / "probe-build"),
               TLS_PROBE_FIXTURES=str(fixtures[0]))
    versions = [("before", "1.0.151", "1.11.0", "0.26.4"),
                ("after", "1.0.154", "1.11.1", "0.26.5")]
    observations = {}
    for label, serde, hyper, tls in versions:
        project = OUTPUT / ("probe-" + label)
        (project / "src").mkdir(parents=True, exist_ok=True)
        (project / "src/main.rs").write_bytes(Path(__file__).with_name("probe.rs").read_bytes())
        manifest = f'''[package]
name = "alignment-probe-{label}"
version = "0.0.0"
edition = "2024"
publish = false
[workspace]
[dependencies]
# Exact baseline/candidate pins for isolated differential evidence only.
serde_json = {{ version = "={serde}", default-features = false, features = ["std", "raw_value"] }}
hyper = {{ version = "={hyper}", default-features = false, features = ["http1", "server"] }}
tokio-rustls = {{ version = "={tls}", default-features = false, features = ["ring"] }}
# Existing workspace dependencies supply the in-memory transport and body.
tokio = {{ version = "=1.53.3", features = ["rt", "io-util", "macros"] }}
axum = {{ version = "=0.8.9", default-features = false }}
'''
        (project / "Cargo.toml").write_text(manifest, encoding="utf-8")
        # Seed both probes from the candidate lock, changing only the tested versions.
        (project / "Cargo.lock").write_bytes((ROOT / "Cargo.lock").read_bytes())
        command = ["cargo", "run", "--offline", "--manifest-path", str(project / "Cargo.toml")]
        result = subprocess.run(command, cwd=ROOT, env=env, capture_output=True, timeout=600)
        (OUTPUT / ("probe-" + label + ".log")).write_bytes(result.stderr + result.stdout)
        if result.returncode:
            raise RuntimeError(f"Probe {label} failed; inspect its target/ log")
        observations[label] = result.stdout.decode("utf-8")
        print(label + ":\n" + observations[label])
    before, after = observations["before"], observations["after"]
    old_flush = next(line for line in before.splitlines() if line.startswith("http flush_first_poll:"))
    new_flush = next(line for line in after.splitlines() if line.startswith("http flush_first_poll:"))
    assert not old_flush.endswith('0\\r\\n\\r\\n"')
    assert new_flush.endswith('0\\r\\n\\r\\n"')
    for side in ["client", "server"]:
        assert f"tls {side} first_read=4 total=8" in before
        assert f"tls {side} first_read=8 total=8" in after
    assert 'http mixed_fragmented: ""' in before
    assert 'http mixed_fragmented: "HTTP/1.1 200 OK' in after
    assert "x-proof: yes" not in before and after.count("x-proof: yes") == 2
    for prefix in ["raw_slice", "raw_reader"]:
        assert f"{prefix} category=Syntax line=1 column=6" in before
        assert f"{prefix} category=Syntax line=1 column=2" in after
    assert "utf8 category=Syntax line=1 column=6" in before
    assert "utf8 category=Syntax line=1 column=2" in after
    for prefix in ["json ", "f32 ", "http canonical:"]:
        assert [line for line in before.splitlines() if line.startswith(prefix)] == [
            line for line in after.splitlines() if line.startswith(prefix)]
    old = tomllib.loads((OUTPUT / "probe-before/Cargo.lock").read_text())
    new = tomllib.loads((OUTPUT / "probe-after/Cargo.lock").read_text())
    old_versions = {p["name"]: p["version"] for p in old["package"]}
    new_versions = {p["name"]: p["version"] for p in new["package"]}
    changed = {name for name in old_versions.keys() & new_versions.keys()
               if old_versions[name] != new_versions[name]}
    assert changed == {"serde_json", "hyper", "tokio-rustls"}, changed
    print("Differential assertions passed; unrelated dependency versions are identical.")


if __name__ == "__main__":
    main()
