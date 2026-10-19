"""Regression checks for fail-closed, data-free adoption validation."""
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import shutil
import sys
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location(
    "adoption_gate", Path(__file__).with_name("check-contract-adoption.py"))
GATE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(GATE)


class AdoptionGateTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        shutil.copytree(GATE.ROOT / "contracts", self.root / "contracts")
        shutil.copytree(GATE.ROOT / "docs", self.root / "docs")

    def run_gate(self):
        output = io.StringIO()
        with patch.object(GATE, "ROOT", self.root), patch.object(sys, "argv", ["gate"]):
            with contextlib.redirect_stdout(output), contextlib.redirect_stderr(output):
                status = GATE.main()
        self.assertNotIn("SENTINEL_PRIVATE", output.getvalue())
        return status

    def test_valid_source_register(self):
        self.assertEqual(self.run_gate(), 0)

    def test_modified_fixture_and_uninventoried_file_fail(self):
        fixture = self.root / "contracts/runtime-v1/database-read-request.json"
        original = fixture.read_bytes()
        fixture.write_text("SENTINEL_PRIVATE", encoding="utf-8")
        self.assertEqual(self.run_gate(), 1)
        fixture.write_bytes(original)
        (fixture.parent / "SENTINEL_PRIVATE.json").write_text("{}", encoding="utf-8")
        self.assertEqual(self.run_gate(), 1)

    def test_duplicate_members_do_not_overwrite(self):
        pin = self.root / "contracts/source.json"
        pin.write_text('{"revision":"SENTINEL_PRIVATE","revision":null}', encoding="utf-8")
        self.assertEqual(self.run_gate(), 1)

    def test_false_release_claim_and_missing_hazard_fail(self):
        path = self.root / "contracts/adoption-source.json"
        manifest = json.loads(path.read_text(encoding="utf-8"))
        manifest["contracts"][0]["status"] = "conforming"
        path.write_text(json.dumps(manifest), encoding="utf-8")
        self.assertEqual(self.run_gate(), 1)
        manifest["contracts"][0]["status"] = "selected"
        deviation = next(item for item in manifest["requirements"] if item["status"] == "deviation")
        deviation.pop("hazard")
        path.write_text(json.dumps(manifest), encoding="utf-8")
        self.assertEqual(self.run_gate(), 1)

    def test_untrusted_arguments_are_not_printed(self):
        output = io.StringIO()
        with patch.object(sys, "argv", ["gate", "--SENTINEL_PRIVATE"]):
            with contextlib.redirect_stderr(output), self.assertRaises(SystemExit) as caught:
                GATE.main()
        self.assertEqual(caught.exception.code, 2)
        self.assertNotIn("SENTINEL_PRIVATE", output.getvalue())


if __name__ == "__main__":
    unittest.main()
