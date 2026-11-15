"""Run the installed-surface probe with a declared transport double on any OS."""
import contextlib
import io
import json
from pathlib import Path
import runpy
import subprocess
import sys
import types
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
PROBE = ROOT / "crates/plenora-runtime-chain-python/tests/interop_surface_probe.py"


class RealSurfacePolicy(unittest.TestCase):
    def invoke(self, state, error, exit_code, budget):
        native = types.ModuleType("plenora_runtime")
        native.Client = object
        native.AsyncClient = object
        native.ChainError = type("ChainError", (Exception,), {})
        response = {"error": error, "value": {"state": state}}
        request = {"config": {}, "plan": {}, "options": {}, "observation_ms": budget}
        process = subprocess.CompletedProcess([], exit_code, json.dumps(response), "")
        output = io.StringIO()
        with patch.dict(sys.modules, {"plenora_runtime": native}), \
             patch.object(sys, "argv", [str(PROBE), "cli"]), \
             patch.object(sys, "stdin", io.StringIO(json.dumps(request))), \
             patch.dict("os.environ", {"PLENORA_CHAIN_BIN": "declared-double"}), \
             patch("subprocess.run", return_value=process) as run, \
             contextlib.redirect_stdout(output):
            runpy.run_path(str(PROBE), run_name="__main__")
        self.assertEqual(run.call_args.kwargs["timeout"], budget / 1000)
        self.assertEqual(json.loads(output.getvalue()), response)

    def test_every_terminal_state_preserves_payload_exit_and_supplied_wait(self):
        cases = [("succeeded", None, 0), ("rejected", "invalid", 2),
                 ("failed", "failed", 3), ("partial", "failed", 3),
                 ("requires_recovery", "recovery", 4), ("cancelled", "cancelled", 5)]
        schema = json.loads((ROOT / "contracts/chains-v2/schemas/chain-result-v2.schema.json").read_text())
        self.assertEqual({state for state, _, _ in cases}, set(schema["properties"]["state"]["enum"]))
        for state, error, code in cases:
            for budget in (50_001, 231_000):
                with self.subTest(state=state, budget=budget):
                    self.invoke(state, error, code, budget)
                    with self.assertRaisesRegex(AssertionError, "exit/result mismatch"):
                        self.invoke(state, error, code + 1, budget)
