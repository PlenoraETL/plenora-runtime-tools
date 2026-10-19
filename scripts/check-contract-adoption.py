"""Offline adoption inventory gate. Diagnostics deliberately contain no input data."""
import argparse
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
REVISION = "3aec9f724f5a0ad21366f293895b4d0dbcd0aad1"
# Changes to the declared deviations require an explicit gate update as well.
EXPECTED_DEVIATIONS = {
    "RT-007", "RT-010", "RT-012", "RT-013", "RT-016", "RT-017",
    "RT-018", "RT-019", "RT-020", "RT-021", "RT-022", "RT-023",
    "RT-024", "RT-026", "RT-027", "ADOPTION-v4", "CAP-004",
}


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate member")
        result[key] = value
    return result


def read(path):
    def invalid_constant(_value):
        raise ValueError("non-JSON constant")
    return json.loads(path.read_text(encoding="utf-8"),
                      object_pairs_hook=unique_object, parse_constant=invalid_constant)


def require(condition):
    if not condition:
        raise ValueError("adoption invariant")


def child(root, relative):
    path = (root / relative).resolve()
    require(path.is_relative_to(root.resolve()))
    return path


def check(upstream=None):
    root = ROOT / "contracts"
    pin = read(root / "source.json")
    adoption = read(root / "adoption-source.json")
    require(pin["revision"] == REVISION)
    require(adoption["contracts_source"] == {
        "repository": pin["repository"], "revision": REVISION})
    require(adoption["profile"] == "plenora-runtime-tools-profile-v1")
    require(adoption["catalog"] is None)
    require(type(adoption["schema_version"]) is int and adoption["schema_version"] == 1)
    require(type(adoption["runtime"]["wire_version"]) is int
            and adoption["runtime"]["wire_version"] == 1)
    selected = [item["id"] for item in adoption["contracts"]]
    require(len(selected) == len(set(selected)) and set(selected) == set(pin["contracts"]))
    require({"plenora-runtime-binding-v2", "plenora-public-catalog-v2"} <= set(selected))
    require(all(item["status"] in {"selected", "adopted_with_deviations"}
                for item in adoption["contracts"]))
    actual = {p.relative_to(root).as_posix() for group in
              ["upstream", "capabilities-v2", "runtime-v1", "runtime-v2",
               "runtime-negotiation-v1", "runtime-probes-v1"]
              for p in (root / group).rglob("*") if p.is_file()}
    require(actual == set(pin["files"]))
    require(set(pin["vectors"]) == {p for p in actual if not p.startswith("upstream/")})
    for relative, record in pin["files"].items():
        content = child(root, relative).read_bytes()
        require(hashlib.sha256(content).hexdigest() == record["sha256"])
        if upstream is not None:
            require(content == child(upstream, record["source"]).read_bytes())
        if relative.endswith(".json"):
            read(child(root, relative))
    requirements = adoption["requirements"]
    rules = [item["rule"] for item in requirements]
    require(len(rules) == len(set(rules)))
    require({item["rule"] for item in requirements if item["status"] == "deviation"}
            == EXPECTED_DEVIATIONS)
    require({f"RT-{n:03}" for n in range(1, 28)} <= set(rules))
    require({"CAT-001", "CAT-002", "CAT-003", "SB-001", "ERR-015", "ERR-016",
             "ADOPTION-v4", "contracts#39", "contracts#40", "INTEROP-001..005"} <= set(rules))
    alignment = (ROOT / "docs/contract-alignment.md").read_text(encoding="utf-8")
    risks = (ROOT / "docs/security-review.md").read_text(encoding="utf-8")
    gates = (ROOT / "docs/qualification.md").read_text(encoding="utf-8")
    for item in requirements:
        require(item["status"] in {"implemented", "deviation", "not_applicable"})
        require(bool(item["scope"]) and bool(item["evidence"]))
        require(item["rule"] in alignment)
        if item["status"] == "deviation":
            require(bool(item["hazard"]) and bool(item["closure"]))
            require(item["rule"] in risks and item["rule"] in gates)
    # This source register must never masquerade as released v4 evidence.
    require(any(r["rule"] == "ADOPTION-v4" and r["status"] == "deviation"
                for r in requirements))


def main():
    class DataFreeParser(argparse.ArgumentParser):
        def error(self, message):
            self.exit(2, "FAIL: invalid gate arguments\n")

    parser = DataFreeParser(description=__doc__)
    parser.add_argument("--upstream", type=Path)
    args = parser.parse_args()
    try:
        check(args.upstream)
    except Exception:
        print("FAIL: contract adoption, inventory or deviation registry invalid", file=sys.stderr)
        return 1
    print("PASS: contract pin, vendored hashes, adoption and deviation registries")
    return 0


if __name__ == "__main__":
    sys.exit(main())
