#!/usr/bin/env python3
"""Validazione di riferimento delle bozze catene, senza resolver o invocazioni."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import datetime
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker
from referencing import Registry, Resource

ROOT = Path(__file__).resolve().parents[1]
SCHEMAS = ROOT / "contracts/chains-v2/schemas"
MAX_BYTES = 1048576
MAX_DEPTH = 64
MAX_INTEGER = 9007199254740991
FORMATS = FormatChecker()


@FORMATS.checks("date-time", raises=(ValueError, OverflowError))
def valid_timestamp(value):
    if not isinstance(value, str):
        return True
    datetime.fromisoformat(value.replace("t", "T").replace("z", "+00:00").replace("Z", "+00:00"))
    return True


class Rejection(ValueError):
    """Contiene soltanto un codice fisso, mai valori o percorsi dal documento."""

    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


def reject(code: str) -> None:
    raise Rejection(code)


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            reject("CHAIN_DUPLICATE")
        result[key] = value
    return result


def check_tree(value, depth=1):
    if depth > MAX_DEPTH:
        reject("CHAIN_LIMIT")
    if isinstance(value, float) or (type(value) is int and abs(value) > MAX_INTEGER):
        reject("CHAIN_DOCUMENT")
    if isinstance(value, str):
        if any(0xD800 <= ord(char) <= 0xDFFF for char in value):
            reject("CHAIN_DOCUMENT")
    if isinstance(value, dict):
        for key, child in value.items():
            check_tree(key, depth + 1)
            check_tree(child, depth + 1)
    elif isinstance(value, list):
        for child in value:
            check_tree(child, depth + 1)


def parse(raw: bytes):
    if len(raw) > MAX_BYTES:
        reject("CHAIN_LIMIT")
    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=unique_object,
                           parse_int=exact_integer,
                           parse_constant=lambda _: reject("CHAIN_DOCUMENT"))
        check_tree(value)
        return value
    except Rejection:
        raise
    except (ValueError, UnicodeError, RecursionError):
        reject("CHAIN_DOCUMENT")


def exact_integer(text):
    if text == "-0" or len(text.lstrip("-")) > 16:
        reject("CHAIN_DOCUMENT")
    return int(text)


def canonical(value) -> bytes:
    """CHAIN-C14N-1: oggetti ASCII ordinati, UTF-8, interi esatti, nessuno spazio."""
    check_tree(value)
    return json.dumps(value, sort_keys=True, ensure_ascii=False,
                      separators=(",", ":"), allow_nan=False).encode("utf-8")


def digest(value) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


def schema_validator(kind: str):
    name = "reference-context.schema.json" if kind == "context" else f"chain-{kind}-v2.schema.json"
    # Registro locale esplicito: nessun recupero di schemi dalla rete o dal payload.
    registry = Registry()
    for directory in (SCHEMAS, ROOT / "contracts/upstream/schemas"):
        for path in directory.glob("*.schema.json"):
            schema = json.loads(path.read_text(encoding="utf-8"))
            registry = registry.with_resource(path.as_uri(), Resource.from_contents(schema))
    path = SCHEMAS / name
    return Draft202012Validator({"$ref": path.as_uri()}, registry=registry,
                                format_checker=FORMATS)


def structure(value, kind):
    check_tree(value)
    if next(schema_validator(kind).iter_errors(value), None) is not None:
        reject("CHAIN_SCHEMA")


def _unique(values):
    if len(values) != len(set(values)):
        reject("CHAIN_DUPLICATE")


def input_digest(step, plan):
    """Vincola sorgenti nominate e manifest iniziali; il piano vincola il grafo intero."""
    return digest({"bindings": step["inputs"], "plan_inputs": plan["inputs"]})


def validate_plan(plan, context):
    structure(context, "context")
    if plan["components"] != context["components"]:
        reject("CHAIN_VERSION")
    claims = {claim for component in context["components"].values() for claim in component["claims"]}
    if not set(plan["requirements"]["contracts"]) <= claims:
        reject("CHAIN_VERSION")
    if plan["budgets"]["max_attempts"] < len(plan["steps"]):
        reject("CHAIN_BUDGET")
    operations = {(x["component"], x["operation"], x["version"]): x for x in context["operations"]}
    _unique([(x["component"], x["operation"], x["version"]) for x in context["operations"]])
    _unique([x["id"] for x in context["boundaries"]])
    boundaries = {x["id"]: x for x in context["boundaries"]}
    _unique([x["id"] for x in plan["steps"]])
    steps = {s["id"]: s for s in plan["steps"]}
    dependencies = {s: set() for s in steps}
    deferred = []
    for step in plan["steps"]:
        if step["component"] not in plan["components"]:
            reject("CHAIN_REFERENCE")
        descriptor = operations.get((step["component"], step["operation"], step["operation_version"]))
        if descriptor is None:
            reject("CHAIN_VERSION")
        for key in ("input_contract", "output_contract", "outputs"):
            if step[key] != descriptor[key]:
                reject("CHAIN_BOUNDARY")
        if set(step["inputs"]) != set(descriptor["inputs"]):
            reject("CHAIN_REFERENCE")
        if step["parameters"]["contract"] != step["input_contract"]:
            reject("CHAIN_BOUNDARY")
        if len(step["inputs"]) + len(step["outputs"]) > 64:
            reject("CHAIN_LIMIT")
        for key, amount in step["budgets"].items():
            if amount > plan["budgets"][key]:
                reject("CHAIN_BUDGET")
        if (step["retry_policy"]["max_attempts"] > step["budgets"]["max_attempts"]
                or step["retry_policy"]["backoff_ms"] > step["budgets"]["elapsed_ms"]):
            reject("CHAIN_BUDGET")
        for dep in step["depends_on"]:
            if dep not in steps:
                reject("CHAIN_REFERENCE")
            dependencies[step["id"]].add(dep)
        for port, binding in step["inputs"].items():
            source = binding["source"]
            expected = binding["expected"]
            if expected != descriptor["inputs"][port]:
                reject("CHAIN_BOUNDARY")
            if source["kind"] == "input":
                if source["input"] not in plan["inputs"]:
                    reject("CHAIN_REFERENCE")
                actual = plan["inputs"][source["input"]]["port"]
                if binding["boundary"] != "plan-input" or actual != expected:
                    reject("CHAIN_BOUNDARY")
            else:
                producer = steps.get(source["step"])
                if producer is None or source["port"] not in producer["outputs"]:
                    reject("CHAIN_REFERENCE")
                dependencies[step["id"]].add(source["step"])
                actual = producer["outputs"][source["port"]]
                edge = boundaries.get(binding["boundary"])
                if edge is None:
                    reject("CHAIN_BOUNDARY")
                pairs = [("source_operation", producer["operation"]),
                         ("source_version", producer["operation_version"]),
                         ("target_operation", step["operation"]),
                         ("target_version", step["operation_version"]),
                         ("source_contract", actual["contract"]),
                         ("target_contract", expected["contract"]),
                         ("content_type", actual["content_type"])]
                if any(edge[key] != value for key, value in pairs):
                    reject("CHAIN_BOUNDARY")
                if edge["mode"] == "provisional":
                    reject("CHAIN_BOUNDARY")
                if edge["mode"] == "adapter_required" and edge["adapter"] not in context["adapters"]:
                    reject("CHAIN_BOUNDARY")
                if edge["mode"] == "direct" and actual != expected:
                    reject("CHAIN_BOUNDARY")
                if actual["primary_geometry"] and not expected["primary_geometry"]:
                    reject("CHAIN_BOUNDARY")
                if actual["primary_geometry"]:
                    claim = "plenora-arrow-primary-geometry-v1"
                    if claim not in plan["components"][producer["component"]]["claims"]:
                        reject("CHAIN_BOUNDARY")
            if expected["primary_geometry"]:
                claim = "plenora-arrow-primary-geometry-v1"
                if (claim not in plan["requirements"]["contracts"] or
                        claim not in plan["components"][step["component"]]["claims"]):
                    reject("CHAIN_BOUNDARY")
            if actual["schema_digest"] is None:
                if not binding["schema_barrier"]:
                    reject("CHAIN_BOUNDARY")
                deferred.append({"step": step["id"], "port": port, "check": "schema"})
    remaining = {k: set(v) for k, v in dependencies.items()}
    order = []
    while remaining:
        ready = sorted(k for k, v in remaining.items() if not v)
        if not ready:
            reject("CHAIN_CYCLE")
        selected = ready[0]
        order.append(selected)
        del remaining[selected]
        for deps in remaining.values():
            deps.discard(selected)
    for output in plan["outputs"].values():
        source = output["source"]
        if (source["kind"] != "step" or source["step"] not in steps or
                source["port"] not in steps[source["step"]]["outputs"]):
            reject("CHAIN_REFERENCE")
        if steps[source["step"]]["outputs"][source["port"]] != output["expected"]:
            reject("CHAIN_BOUNDARY")
    grants = plan["loss_policy"]["grants"]
    _unique([g["step"] for g in grants])
    by_step = {g["step"]: g for g in grants}
    if any(g["step"] not in steps for g in grants):
        reject("CHAIN_CONSENT")
    for step in steps.values():
        grant = by_step.get(step["id"])
        if step.get("loss_consent", False) != (grant is not None):
            reject("CHAIN_CONSENT")
        if "loss_consent" in step and (step["operation"], step["operation_version"]) != ("database.write", 2):
            reject("CHAIN_CONSENT")
        if grant is not None:
            if (grant["operation"] != step["operation"] or grant["operation_version"] != step["operation_version"]
                    or grant["destination_ref"] != step.get("destination_ref")
                    or grant["input_digest"] != input_digest(step, plan)
                    or grant["parameters_digest"] != step["parameters"]["sha256"]):
                reject("CHAIN_CONSENT")
    # Descrittori fixture non provano payload, provider, autorizzazioni o risorse reali.
    return {"order": order, "deferred": sorted(deferred, key=lambda x: (x["step"], x["port"])),
            "admission": "reference_only", "plan_digest": digest(plan)}


def validate_axes(value):
    effect, retry = value["remote_effect"], value["retry"]["kind"]
    if effect in ("committed", "partial", "unknown") and retry not in ("never", "quarantine", "requires_recovery"):
        reject("CHAIN_RESULT")
    if value.get("category") == "resource_limit" and effect == "none" and retry != "never":
        reject("CHAIN_RESULT")


def validate_reports(reports):
    for report in reports:
        if (report["availability"] == "available") != (report["document"] is not None):
            reject("CHAIN_RESULT")


def validate_error(value):
    validate_axes(value)
    validate_reports(value["reports"])


def validate_result(result, plan, context):
    if plan is None:
        reject("CHAIN_REFERENCE")
    structure(plan, "plan")
    if context is None:
        reject("CHAIN_REFERENCE")
    validated_plan = validate_plan(plan, context)
    if (result["plan_id"] != plan["plan_id"] or result["plan_revision"] != plan["revision"]
            or result["plan_digest"] != digest(plan)):
        reject("CHAIN_CONFLICT")
    _unique([s["step_id"] for s in result["steps"]])
    expected = {s["id"]: s for s in plan["steps"]}
    if set(expected) != {s["step_id"] for s in result["steps"]}:
        reject("CHAIN_RESULT")
    validate_axes(result)
    check_times(result)
    all_reports = list(result["reports"])
    by_id = {s["step_id"]: s for s in result["steps"]}
    terminal_seen = False
    for step_id in validated_plan["order"]:
        current = by_id[step_id]
        if terminal_seen and current["attempt"]:
            reject("CHAIN_RESULT")
        if current["state"] != "succeeded":
            terminal_seen = True
    for step in result["steps"]:
        definition = expected[step["step_id"]]
        if (step["operation"], step["operation_version"]) != (definition["operation"], definition["operation_version"]):
            reject("CHAIN_VERSION")
        validate_axes(step)
        reconcile_error(result, step)
        check_times(step)
        if step["counters"]["max_attempts"] != step["attempt"]:
            reject("CHAIN_RESULT")
        if step["attempt"] > definition["retry_policy"]["max_attempts"]:
            reject("CHAIN_RESULT")
        for key, amount in step["counters"].items():
            if amount > definition["budgets"][key]:
                reject("CHAIN_RESULT")
        all_reports.extend(step["reports"])
        if step["state"] in ("blocked", "skipped", "rejected") and (step["attempt"] != 0 or step["remote_effect"] != "none" or step["outputs"]):
            reject("CHAIN_RESULT")
        if step["attempt"]:
            if set(step["input_manifests"]) != set(definition["inputs"]):
                reject("CHAIN_RESULT")
            for name, binding in definition["inputs"].items():
                source = binding["source"]
                if source["kind"] == "input":
                    manifest = plan["inputs"][source["input"]]
                else:
                    producer = by_id[source["step"]]
                    if producer["state"] != "succeeded" or source["port"] not in producer["outputs"]:
                        reject("CHAIN_RESULT")
                    artifact = producer["outputs"][source["port"]]
                    manifest = {key: artifact[key] for key in ("artifact_ref", "port", "bytes", "sha256")}
                if step["input_manifests"][name] != manifest:
                    reject("CHAIN_RESULT")
        if step["state"] == "cancelled" and not step["stop_confirmed"]:
            reject("CHAIN_RESULT")
        if step["state"] in ("failed", "partial", "requires_recovery", "rejected") and step["error"] is None:
            reject("CHAIN_RESULT")
        if step["remote_effect"] == "unknown" and step["state"] != "requires_recovery":
            reject("CHAIN_RESULT")
        if step["state"] == "succeeded":
            if (step["error"] is not None or step["other_errors"] or step["remote_effect"] in ("partial", "unknown", "rolled_back")
                    or set(step["outputs"]) != set(definition["outputs"])):
                reject("CHAIN_RESULT")
            if step["attempt"] == 0 or step["upstream_result"] is None:
                reject("CHAIN_RESULT")
        for port, artifact in step["outputs"].items():
            if (port not in definition["outputs"] or artifact["port"] != definition["outputs"][port]
                    or artifact["producer_step"] != step["step_id"] or artifact["attempt"] != step["attempt"]
                    or (step["state"] == "succeeded" and artifact["publication"] != "confirmed")):
                reject("CHAIN_RESULT")
            all_reports.extend(artifact["reports"])
        for error in [step["error"], *step["other_errors"]]:
            if error is not None:
                validate_error(error)
                reconcile_error(step, error)
    for error in [result["error"], *result["other_errors"]]:
        if error is not None:
            validate_error(error)
            reconcile_error(result, error)
    effects = [s["remote_effect"] for s in result["steps"]]
    states = [s["state"] for s in result["steps"]]
    if result["state"] in ("rejected", "failed", "partial", "requires_recovery") and result["error"] is None:
        reject("CHAIN_RESULT")
    if result["state"] == "rejected" and any(s["attempt"] or s["remote_effect"] != "none" for s in result["steps"]):
        reject("CHAIN_RESULT")
    if result["state"] == "failed" and not any(s in ("failed", "rejected") for s in states):
        reject("CHAIN_RESULT")
    if result["state"] == "partial" and not ("partial" in states or ("succeeded" in states and any(s != "succeeded" for s in states))):
        reject("CHAIN_RESULT")
    if result["state"] == "cancelled" and "cancelled" not in states:
        reject("CHAIN_RESULT")
    for key, amount in result["counters"].items():
        if amount > plan["budgets"][key]:
            reject("CHAIN_RESULT")
        if key not in ("memory_bytes", "elapsed_ms", "spool_bytes"):
            if amount != sum(s["counters"][key] for s in result["steps"]):
                reject("CHAIN_RESULT")
    aggregate = ("unknown" if "unknown" in effects else "partial" if "partial" in effects
                 or ("committed" in effects and result["state"] != "succeeded")
                 else "committed" if "committed" in effects else "none")
    if result["remote_effect"] != aggregate:
        reject("CHAIN_RESULT")
    if ("unknown" in effects) != result["recovery"]["required"]:
        reject("CHAIN_RESULT")
    if "unknown" in effects and result["state"] != "requires_recovery":
        reject("CHAIN_RESULT")
    if not set(result["recovery"]["resumable_steps"]) <= set(expected):
        reject("CHAIN_REFERENCE")
    for name, artifact in result["outputs"].items():
        if name not in plan["outputs"]:
            reject("CHAIN_RESULT")
        source = plan["outputs"][name]["source"]
        if artifact != by_id[source["step"]]["outputs"].get(source["port"]):
            reject("CHAIN_RESULT")
    validate_reports(all_reports)
    if result["reports_complete"] and any(r["availability"] == "unavailable" for r in all_reports):
        reject("CHAIN_RESULT")
    if not result["reports_complete"] and result["losses"] == "none":
        reject("CHAIN_RESULT")
    if result["state"] == "succeeded":
        if (any(s["state"] != "succeeded" for s in result["steps"]) or result["error"] is not None
                or result["other_errors"] or not result["reports_complete"] or result["losses"] == "unavailable"
                or set(result["outputs"]) != set(plan["outputs"])):
            reject("CHAIN_RESULT")
    if result["state"] == "cancelled" and any(not s["stop_confirmed"] for s in result["steps"] if s["attempt"]):
        reject("CHAIN_RESULT")


def check_times(value):
    start, end = value["started_at"], value["finished_at"]
    if (start is None) != (end is None):
        reject("CHAIN_RESULT")
    if start is not None:
        def instant(text):
            # La forma UTC validata rende confrontabili secondi e frazione esatta.
            second = text[:19].upper()
            fraction = re.search(r"[.]([0-9]+)", text)
            return second, (fraction.group(1) if fraction else "").ljust(9, "0")
        if instant(end) < instant(start):
            reject("CHAIN_RESULT")


def reconcile_error(outcome, error):
    effect = error["remote_effect"]
    allowed = {"unknown": {"unknown"}, "partial": {"partial", "unknown"},
               "committed": {"committed", "partial", "unknown"}}
    if effect in allowed and outcome["remote_effect"] not in allowed[effect]:
        reject("CHAIN_RESULT")
    restrictive = ("never", "quarantine", "requires_recovery")
    original, summary = error["retry"], outcome["retry"]
    if summary["kind"] in restrictive:
        return
    if original["kind"] in restrictive:
        reject("CHAIN_RESULT")
    if original["kind"] == "requires_idempotency_key" and summary["kind"] != original["kind"]:
        reject("CHAIN_RESULT")
    if original["kind"] == "after":
        if summary["kind"] != "after" or summary["delay_ms"] < original["delay_ms"]:
            reject("CHAIN_RESULT")


def validate_event(event):
    kind = event["kind"]
    if (kind == "step_state") != (event["step_id"] is not None):
        reject("CHAIN_EVENT")
    states = {"accepted": "pending", "cancel_requested": "cancel_requested",
              "cancel_confirmed": "cancelled", "recovery_required": "requires_recovery"}
    if kind in states and event["state"] != states[kind]:
        reject("CHAIN_EVENT")
    if (kind in ("result_persisted", "delivery_confirmed")) != (event["result_ref"] is not None):
        reject("CHAIN_EVENT")
    if kind != "step_state" and event["attempt"] != 0:
        reject("CHAIN_EVENT")
    if event["error"] is not None:
        validate_error(event["error"])


def validate(raw, kind, *, context=None, plan=None):
    value = parse(raw)
    structure(value, kind)
    if kind == "plan":
        if context is None:
            reject("CHAIN_REFERENCE")
        return validate_plan(value, context)
    if kind == "result":
        validate_result(value, plan, context)
    elif kind == "error":
        validate_error(value)
    elif kind == "event":
        validate_event(value)
    return {"valid": True, "admission": "reference_only"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("kind", choices=("plan", "result", "event", "error"))
    parser.add_argument("document", type=Path)
    parser.add_argument("--context", type=Path)
    parser.add_argument("--plan", type=Path)
    args = parser.parse_args()
    try:
        result = validate(args.document.read_bytes(), args.kind,
                          context=parse(args.context.read_bytes()) if args.context else None,
                          plan=parse(args.plan.read_bytes()) if args.plan else None)
        print(json.dumps(result, sort_keys=True))
        return 0
    except Rejection as error:
        print(json.dumps({"valid": False, "code": error.code}))
        return 1
    except OSError:
        print('{"valid": false, "code": "CHAIN_DOCUMENT"}')
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
