"""Oracolo delle fixture: parser, Draft 2020-12 e invarianti semantiche distinti."""

import copy
import json
import unittest

from jsonschema import Draft202012Validator

from validate_chain import (ROOT, SCHEMAS, MAX_BYTES, Rejection, canonical, digest, parse,
                            schema_validator, validate)

FIXTURES = ROOT / "contracts/chains-v2/fixtures"


class ChainContractTests(unittest.TestCase):
    def test_schemas_are_draft_2020_12(self):
        for path in SCHEMAS.glob("*.schema.json"):
            with self.subTest(schema=path.name):
                Draft202012Validator.check_schema(json.loads(path.read_text(encoding="utf-8")))

    def test_all_fixtures(self):
        cases = json.loads((FIXTURES / "manifest.json").read_text(encoding="utf-8"))
        referenced = {"manifest.json"}
        for case in cases:
            with self.subTest(fixture=case["file"]):
                referenced.add(case["file"])
                raw = (FIXTURES / case["file"]).read_bytes()
                context = parse((FIXTURES / case["context"]).read_bytes())
                referenced.add(case["context"])
                plan = parse((FIXTURES / case["plan"]).read_bytes()) if case["plan"] else None
                if case["schema"] is not None:
                    value = parse(raw)
                    self.assertEqual(schema_validator(case["kind"]).is_valid(value), case["schema"])
                if case["valid"]:
                    first = validate(raw, case["kind"], context=context, plan=plan)
                    self.assertEqual(first, validate(raw, case["kind"], context=context, plan=plan))
                else:
                    with self.assertRaises(Rejection) as caught:
                        validate(raw, case["kind"], context=context, plan=plan)
                    self.assertEqual(caught.exception.code, case["code"])
                    self.assertNotIn("SENTINEL", str(caught.exception))
        self.assertEqual(referenced, {p.name for p in FIXTURES.glob("*.json")})

    def test_canonical_digest_is_stable_and_scope_sensitive(self):
        self.assertEqual(canonical({"b": 2, "a": 1}), b'{"a":1,"b":2}')
        self.assertEqual(digest({"a": 1}), "015abd7f5cc57a2dd94b7590f04ad8084273905ee33ec5cebeae62276a97f862")
        plan = parse((FIXTURES / "plan-valid.json").read_bytes())
        self.assertEqual(digest(plan), digest(dict(reversed(list(plan.items())))))
        other = copy.deepcopy(plan)
        other["steps"][2]["destination_ref"] = "ref:other"
        self.assertNotEqual(digest(plan), digest(other))

    def test_exact_document_depth_and_step_limits(self):
        self.assertEqual(parse(b"0" + b" " * (MAX_BYTES - 1)), 0)
        parse(b"[" * 63 + b"0" + b"]" * 63)
        with self.assertRaises(Rejection):
            parse(b"[" * 64 + b"0" + b"]" * 64)
        plan = parse((FIXTURES / "plan-valid.json").read_bytes())
        context = parse((FIXTURES / "context.json").read_bytes())
        step = plan["steps"][0]
        plan["steps"] = [dict(copy.deepcopy(step), id=f"s{i}") for i in range(1024)]
        plan["outputs"] = {}
        outcome = validate(canonical(plan), "plan", context=context)
        self.assertEqual(len(outcome["order"]), 1024)

    def test_result_rejects_invalid_plan_without_exposing_reference(self):
        plan = parse((FIXTURES / "plan-valid.json").read_bytes())
        context = parse((FIXTURES / "context.json").read_bytes())
        result = (FIXTURES / "result-valid.json").read_bytes()
        for source in ({"kind": "input", "input": "sentinel"},
                       {"kind": "step", "step": "sentinel", "port": "dataset"}):
            plan["outputs"]["dataset"]["source"] = source
            with self.assertRaises(Rejection) as caught:
                validate(result, "result", context=context, plan=plan)
            self.assertEqual(caught.exception.code, "CHAIN_REFERENCE")

    def test_all_new_patterns_have_exact_termination(self):
        common = json.loads((SCHEMAS / "common.schema.json").read_text())
        samples = {"id": "read", "operation": "io.read", "opaque": "ref:document",
                   "contract": "plenora-fixture-v1"}
        for name, sample in samples.items():
            validator = Draft202012Validator(common["$defs"][name])
            self.assertTrue(validator.is_valid(sample))
            self.assertFalse(validator.is_valid(sample + "\n"))

    def test_proposed_edges_remain_drafts(self):
        edges = json.loads((ROOT / "contracts/chains-v2/proposed-edges.json").read_text())
        self.assertEqual(edges["status"], "draft")
        context = parse((FIXTURES / "context.json").read_bytes())
        context["boundaries"] = edges["edges"]
        self.assertTrue(schema_validator("context").is_valid(context))
        self.assertEqual(len({edge["id"] for edge in edges["edges"]}), len(edges["edges"]))
        for edge in edges["edges"]:
            self.assertEqual(edge["status"], "draft")

    def test_primary_public_output_requires_producer_claim(self):
        plan = parse((FIXTURES / "plan-valid.json").read_bytes())
        context = parse((FIXTURES / "context.json").read_bytes())
        plan["steps"] = [plan["steps"][0]]
        for output in plan["outputs"].values():
            output["source"]["step"] = plan["steps"][0]["id"]
        validate(canonical(plan), "plan", context=context)
        plan["components"]["io"]["claims"] = []
        context["components"] = copy.deepcopy(plan["components"])
        with self.assertRaises(Rejection) as caught:
            validate(canonical(plan), "plan", context=context)
        self.assertEqual(caught.exception.code, "CHAIN_BOUNDARY")

    def test_error_report_unavailability_is_included_in_completeness(self):
        plan = parse((FIXTURES / "plan-valid.json").read_bytes())
        context = parse((FIXTURES / "context.json").read_bytes())
        original = parse((FIXTURES / "result-failed-valid.json").read_bytes())
        for where in range(4):
            result = copy.deepcopy(original)
            error = copy.deepcopy(result["error"])
            error["reports"] = [{"kind": "fidelity", "availability": "unavailable", "document": None}]
            failed = next(s for s in result["steps"] if s["error"] is not None)
            target = result if where < 2 else failed
            if where % 2:
                target["other_errors"].append(error)
            else:
                target["error"] = error
            result["reports_complete"] = True
            result["losses"] = "none"
            with self.assertRaises(Rejection) as caught:
                validate(canonical(result), "result", plan=plan, context=context)
            self.assertEqual(caught.exception.code, "CHAIN_RESULT")
            result["reports_complete"] = False
            result["losses"] = "unavailable"
            validate(canonical(result), "result", plan=plan, context=context)

    def test_closed_objects_reject_unknown_members(self):
        for kind in ("plan", "result", "event", "error"):
            value = parse((FIXTURES / f"{kind}-valid.json").read_bytes())
            paths = []

            def visit(node, path):
                if isinstance(node, dict):
                    paths.append(path)
                    for key, child in node.items():
                        visit(child, path + [key])
                elif isinstance(node, list):
                    for index, child in enumerate(node):
                        visit(child, path + [index])

            visit(value, [])
            for path in paths:
                broken = copy.deepcopy(value)
                node = broken
                for key in path:
                    node = node[key]
                # Mappe di porte/componenti accettano nuovi nomi ma mai valori scalari.
                node["unknown"] = "SENTINEL"
                with self.subTest(kind=kind, path=path):
                    self.assertFalse(schema_validator(kind).is_valid(broken))


if __name__ == "__main__":
    unittest.main()
