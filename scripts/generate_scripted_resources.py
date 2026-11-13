"""Generate declared JSON domain doubles, separate from the pinned contract corpus.

These bytes are not Arrow IPC and do not attest any Plenora library implementation.
"""
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "contracts/chains-v2/fixtures"
DEST = ROOT / "crates/plenora-runtime-chain-host/tests/fixtures"


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def sha(value):
    return hashlib.sha256(value).hexdigest()


def generate():
    artifacts = {}
    schema = {"fields": [{"id": "geometry-id", "name": "geometry", "data_type": "binary", "nullable": True,
                          "geometry": True, "metadata": {"unknown": "preserved", "crs": "fixture-crs"}}],
              "metadata": {"plenora.primary_geometry": "geometry-id", "unknown": "preserved"}}
    schema_digest = sha(encoded(schema))
    arrow = encoded({"fixture": "scripted-arrow-v1", "schema": schema, "rows": []})
    for name in ("read", "transform"):
        artifacts[f"ref:{name}"] = list(arrow)

    def document(contract, body, name):
        reference = f"ref:{name}"
        raw = encoded({"fixture": "scripted-v1", "contract": contract, "body": body})
        artifacts[reference] = list(raw)
        return {"contract": contract, "artifact_ref": reference, "sha256": sha(raw)}

    def visit(value):
        if isinstance(value, dict):
            if "schema_digest" in value:
                value["schema_digest"] = schema_digest
            if "artifact_ref" in value and "port" in value:
                raw = bytes(artifacts[value["artifact_ref"]])
                value.update(bytes=len(raw), sha256=sha(raw))
                if "columns" in value:
                    value["columns"] = 1
            for item in value.values():
                visit(item)
        elif isinstance(value, list):
            for item in value:
                visit(item)

    plan = json.loads((SOURCE / "plan-valid.json").read_text(encoding="utf-8"))
    registry = json.loads((SOURCE / "context.json").read_text(encoding="utf-8"))
    for component in plan["components"].values():
        component["catalog"] = document(component["catalog"]["contract"], {"kind": "catalog"}, "catalog")
    registry["components"] = plan["components"]
    for step in plan["steps"]:
        step["parameters"] = document(step["input_contract"], {"kind": "parameters", "operation": step["operation"],
                                                              "version": step["operation_version"]}, f'parameters-{step["id"]}')
    visit(plan)
    visit(registry)
    DEST.mkdir(parents=True, exist_ok=True)
    documents = {"plan-valid.json": plan, "context.json": registry}
    for filename in ("result-valid.json", "result-recovery-valid.json"):
        outcome = json.loads((SOURCE / filename).read_text(encoding="utf-8"))
        visit(outcome)
        for result in outcome["steps"]:
            step = next(s for s in plan["steps"] if s["id"] == result["step_id"])
            if result["upstream_result"]:
                result["upstream_result"] = document(step["output_contract"], {"kind": "upstream"}, f'upstream-{step["id"]}')
            proof = {"kind": "fidelity", "step": step["id"], "operation": step["operation"], "version": step["operation_version"],
                     "parameters_sha256": step["parameters"]["sha256"], "destination": step.get("destination_ref"),
                     "inputs": {k: [v["artifact_ref"], v["sha256"]] for k, v in result["input_manifests"].items()},
                     "outputs": {k: [v["artifact_ref"], v["sha256"]] for k, v in result["outputs"].items()}, "verdict": "exact"}
            result["reports"] = [{"kind": "fidelity", "availability": "available",
                                  "document": document("plenora-fixture-fidelity-v1", proof, f'fidelity-{step["id"]}')}]
            result["counters"]["request_bytes"] = len(artifacts[step["parameters"]["artifact_ref"]])
        outcome["plan_digest"] = sha(encoded(plan))
        outcome["counters"]["request_bytes"] = sum(s["counters"]["request_bytes"] for s in outcome["steps"])
        documents[filename] = outcome
    documents["artifacts.json"] = artifacts
    for name, value in documents.items():
        (DEST / name).write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


if __name__ == "__main__":
    generate()
